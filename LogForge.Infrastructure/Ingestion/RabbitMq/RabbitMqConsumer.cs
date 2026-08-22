using LogForge.Domain.Ingestion;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;
using System.Threading.Channels;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqIngestionConsumer : BackgroundService
{
    private readonly RabbitMqConnection _connection;
    private readonly NpgsqlLogBulkWriter _bulkWriter;
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqIngestionConsumer> _logger;

    public RabbitMqIngestionConsumer(
        RabbitMqConnection connection,
        NpgsqlLogBulkWriter bulkWriter,
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqIngestionConsumer> logger)
    {
        _connection = connection;
        _bulkWriter = bulkWriter;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerCount = Math.Max(1, _options.ConsumerCount);

        var workers = Enumerable.Range(0, consumerCount)
            .Select(workerId => RunWorkerLoopAsync(workerId, stoppingToken));

        await Task.WhenAll(workers);
    }

    private async Task RunWorkerLoopAsync(int workerId, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeAsync(workerId, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "RabbitMQ ingestion worker {WorkerId} failed; retrying", workerId);
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task ConsumeAsync(int workerId, CancellationToken stoppingToken)
    {
        var connection = await _connection.GetConnectionAsync(stoppingToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
        var batchSize = Math.Max(1, _options.ConsumerBatchSize);

        var deliveries = Channel.CreateBounded<RabbitDelivery>(new BoundedChannelOptions(
            Math.Max(batchSize, _options.PrefetchCount))
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });

        await channel.QueueDeclareAsync(
            queue: _options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken);
        await channel.BasicQosAsync(0, _options.PrefetchCount, false, stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, delivery) =>
        {
            await deliveries.Writer.WriteAsync(
                new RabbitDelivery(delivery.DeliveryTag, delivery.Body.ToArray()),
                stoppingToken);
        };

        await channel.BasicConsumeAsync(_options.QueueName, autoAck: false, consumer);

        while (!stoppingToken.IsCancellationRequested)
        {
            var batch = await ReadBatchAsync(deliveries.Reader, batchSize, stoppingToken);
            await ProcessBatchAsync(workerId, channel, batch, stoppingToken);
        }
    }

    private async Task<List<RabbitDelivery>> ReadBatchAsync(
        ChannelReader<RabbitDelivery> reader,
        int batchSize,
        CancellationToken stoppingToken)
    {
        var batch = new List<RabbitDelivery>(batchSize)
        {
            await reader.ReadAsync(stoppingToken)
        };

        if (batch.Count < batchSize)
        {
            await Task.Delay(
                TimeSpan.FromMilliseconds(Math.Max(1, _options.ConsumerBatchWaitMs)),
                stoppingToken);
        }

        while (batch.Count < batchSize && reader.TryRead(out var delivery))
            batch.Add(delivery);

        return batch;
    }

    private async Task ProcessBatchAsync(
        int workerId,
        IChannel channel,
        IReadOnlyList<RabbitDelivery> deliveries,
        CancellationToken stoppingToken)
    {
        var validDeliveries = new List<(RabbitDelivery Delivery, List<LogEntry> Logs)>();

        foreach (var delivery in deliveries)
        {
            List<LogEntry>? parsedLogs;
            try
            {
                parsedLogs = JsonSerializer.Deserialize<List<LogEntry>>(delivery.Body);
                if (parsedLogs is null)
                    throw new JsonException("RabbitMQ message contained no logs");
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "Worker {WorkerId}: discarding invalid RabbitMQ ingestion message", workerId);
                await channel.BasicNackAsync(delivery.DeliveryTag, false, false, stoppingToken);
                continue;
            }

            validDeliveries.Add((delivery, parsedLogs));
        }

        if (validDeliveries.Count == 0)
            return;

        var combinedLogs = validDeliveries.SelectMany(item => item.Logs).ToList();

        try
        {
            await WriteWithRetryAsync(combinedLogs, stoppingToken);
            await AckAllAsync(channel, validDeliveries, stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
        {
            _logger.LogError(ex, "Worker {WorkerId}: failed to persist RabbitMQ message batch; requeueing", workerId);
            await NackAllAsync(channel, validDeliveries, requeue: true, stoppingToken);
        }
    }

    private static async Task AckAllAsync(
        IChannel channel,
        List<(RabbitDelivery Delivery, List<LogEntry> Logs)> deliveries,
        CancellationToken stoppingToken)
    {
        foreach (var item in deliveries)
            await channel.BasicAckAsync(item.Delivery.DeliveryTag, false, stoppingToken);
    }

    private static async Task NackAllAsync(
        IChannel channel,
        List<(RabbitDelivery Delivery, List<LogEntry> Logs)> deliveries,
        bool requeue,
        CancellationToken stoppingToken)
    {
        foreach (var item in deliveries)
            await channel.BasicNackAsync(item.Delivery.DeliveryTag, false, requeue, stoppingToken);
    }

    private async Task WriteWithRetryAsync(IReadOnlyList<LogEntry> logs, CancellationToken stoppingToken)
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