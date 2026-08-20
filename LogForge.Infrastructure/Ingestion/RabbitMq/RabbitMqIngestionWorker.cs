using LogForge.Domain.Ingestion;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;
using System.Threading.Channels;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqIngestionWorker : BackgroundService
{
    private readonly RabbitMqConnection _connection;
    private readonly NpgsqlLogBulkWriter _bulkWriter;
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqIngestionWorker> _logger;

    public RabbitMqIngestionWorker(
        RabbitMqConnection connection,
        NpgsqlLogBulkWriter bulkWriter,
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqIngestionWorker> logger)
    {
        _connection = connection;
        _bulkWriter = bulkWriter;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerCount = Math.Max(1, _options.ConsumerCount);

        _logger.LogInformation(
            "Starting {ConsumerCount} RabbitMQ push consumers",
            consumerCount);

        var consumers = Enumerable
            .Range(1, consumerCount)
            .Select(id => RunConsumerAsync(id, stoppingToken))
            .ToArray();

        await Task.WhenAll(consumers);
    }

    private async Task RunConsumerAsync(int consumerId, CancellationToken stoppingToken)
    {
        try
        {
            var connection = await _connection.GetConnectionAsync(stoppingToken);

            await using var channel = await connection.CreateChannelAsync(
                cancellationToken: stoppingToken);

            await channel.QueueDeclareAsync(
                queue: _options.QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                cancellationToken: stoppingToken);

            var batchSize = Math.Max(1, _options.ConsumerBatchSize);


            var deliveryChannel = Channel.CreateBounded<RabbitDelivery>(
                new BoundedChannelOptions(batchSize * 2)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    FullMode = BoundedChannelFullMode.Wait
                });

            await channel.BasicQosAsync(
                prefetchSize: 0,
                prefetchCount: (ushort)(_options.ConsumerPrefetchCount),
                global: false,
                cancellationToken: stoppingToken);

            var consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += async (_, args) =>
            {
                try
                {
                    var delivery = new RabbitDelivery(args.DeliveryTag, args.Body.ToArray());

                    await deliveryChannel.Writer.WriteAsync(delivery, stoppingToken);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    // Normal shutdown.
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Consumer {ConsumerId} failed enqueueing delivery",
                        consumerId);
                }
            };

            var consumerTag = await channel.BasicConsumeAsync(
                queue: _options.QueueName,
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken);

            _logger.LogInformation(
                "RabbitMQ push consumer {ConsumerId} started with tag {ConsumerTag}",
                consumerId,
                consumerTag);

            // Runs until deliveryChannel is completed and drained.
            await RunBatchLoopAsync(
                channel,
                deliveryChannel.Reader,
                consumerId,
                batchSize,
                stoppingToken);

            deliveryChannel.Writer.TryComplete();
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "RabbitMQ push consumer {ConsumerId} stopped",
                consumerId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RabbitMQ push consumer {ConsumerId} failed", consumerId);
            throw;
        }
    }

    private async Task RunBatchLoopAsync(
        IChannel channel,
        ChannelReader<RabbitDelivery> reader,
        int consumerId,
        int batchSize,
        CancellationToken stoppingToken)
    {
        var flushInterval = TimeSpan.FromMilliseconds(
            Math.Max(1, _options.ConsumerBatchFlushIntervalMs));

        var batch = new List<RabbitDelivery>(batchSize);
        using var flushTimer = new PeriodicTimer(flushInterval);

        var timerTask = WaitForTimerAsync(flushTimer, stoppingToken);
        var readTask = reader.ReadAsync(stoppingToken).AsTask();

        try
        {
            while (true)
            {
                var completed = await Task.WhenAny(readTask, timerTask);

                if (completed == readTask)
                {
                    RabbitDelivery item;

                    try
                    {
                        item = readTask.Result;
                    }
                    catch (ChannelClosedException)
                    {
                        break;
                    }

                    batch.Add(item);

                    if (batch.Count >= batchSize)
                    {
                        await FlushAsync(channel, batch, consumerId, stoppingToken);
                    }

                    readTask = reader.ReadAsync(stoppingToken).AsTask();
                }
                else
                {
                    if (batch.Count > 0)
                    {
                        await FlushAsync(channel, batch, consumerId, stoppingToken);
                    }

                    timerTask = WaitForTimerAsync(flushTimer, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown — fall through to final drain below.
        }

        // Drain whatever's left in the channel and flush on shutdown.
        while (reader.TryRead(out var leftover))
        {
            batch.Add(leftover);
        }

        if (batch.Count > 0)
        {
            await FlushAsync(channel, batch, consumerId, CancellationToken.None);
        }
    }

    private static async Task<bool> WaitForTimerAsync(
        PeriodicTimer timer,
        CancellationToken stoppingToken)
    {
        try
        {
            return await timer.WaitForNextTickAsync(stoppingToken);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private async Task FlushAsync(
        IChannel channel,
        List<RabbitDelivery> batch,
        int consumerId,
        CancellationToken stoppingToken)
    {
        if (batch.Count == 0)
            return;

        var currentBatch = batch.ToArray();
        batch.Clear();

        await ProcessBatchAsync(channel, currentBatch, consumerId, stoppingToken);
    }

    private async Task ProcessBatchAsync(
        IChannel channel,
        IReadOnlyList<RabbitDelivery> deliveries,
        int consumerId,
        CancellationToken stoppingToken)
    {
        var validDeliveries = new List<(RabbitDelivery Delivery, List<LogEntry> Logs)>();

        foreach (var delivery in deliveries)
        {
            try
            {
                var parsedLogs = JsonSerializer.Deserialize<List<LogEntry>>(delivery.Body)
                    ?? throw new JsonException("RabbitMQ message contained no logs");

                validDeliveries.Add((delivery, parsedLogs));
            }
            catch (JsonException ex)
            {
                _logger.LogError(
                    ex,
                    "Consumer {ConsumerId} discarding invalid RabbitMQ message",
                    consumerId);

                await channel.BasicNackAsync(
                    delivery.DeliveryTag,
                    multiple: false,
                    requeue: false,
                    cancellationToken: stoppingToken);
            }
        }

        if (validDeliveries.Count == 0)
            return;

        var combinedLogs = validDeliveries.SelectMany(x => x.Logs).ToList();

        try
        {
            await WriteWithRetryAsync(combinedLogs, stoppingToken);

            foreach (var item in validDeliveries)
            {
                await channel.BasicAckAsync(
                    item.Delivery.DeliveryTag,
                    multiple: false,
                    cancellationToken: stoppingToken);
            }

            _logger.LogDebug(
                "Consumer {ConsumerId} processed {MessageCount} messages / {LogCount} logs",
                consumerId,
                validDeliveries.Count,
                combinedLogs.Count);
        }
        catch (Exception ex)
            when (ex is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
        {
            _logger.LogError(
                ex,
                "Consumer {ConsumerId} failed to persist batch; requeueing",
                consumerId);

            foreach (var item in validDeliveries)
            {
                await channel.BasicNackAsync(
                    item.Delivery.DeliveryTag,
                    multiple: false,
                    requeue: true,
                    cancellationToken: stoppingToken);
            }
        }
    }

    private async Task WriteWithRetryAsync(
        IReadOnlyList<LogEntry> logs,
        CancellationToken stoppingToken)
    {
        const int maxAttempts = 3;
        Exception? last = null;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await _bulkWriter.WriteAsync(logs, stoppingToken);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (attempt < maxAttempts)
            {
                last = ex;
                await Task.Delay(TimeSpan.FromMilliseconds(25 * attempt), stoppingToken);
            }
            catch (Exception ex)
            {
                last = ex;
            }
        }

        throw last ?? new InvalidOperationException("Log batch write failed.");
    }

    private sealed record RabbitDelivery(ulong DeliveryTag, byte[] Body);
}