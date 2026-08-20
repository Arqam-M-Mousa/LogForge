using LogForge.Domain.Ingestion;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Text.Json;

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

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var consumerCount = Math.Max(1, _options.ConsumerCount);

        _logger.LogInformation(
            "Starting {ConsumerCount} RabbitMQ polling consumers",
            consumerCount);

        var consumers = Enumerable
            .Range(1, consumerCount)
            .Select(id => RunConsumerAsync(id, stoppingToken))
            .ToArray();

        await Task.WhenAll(consumers);
    }

    private async Task RunConsumerAsync(
        int consumerId,
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PollAsync(
                    consumerId,
                    stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "RabbitMQ polling consumer {ConsumerId} failed; retrying",
                    consumerId);

                await Task.Delay(
                    TimeSpan.FromSeconds(5),
                    stoppingToken);
            }
        }
    }

    private async Task PollAsync(
        int consumerId,
        CancellationToken stoppingToken)
    {
        var connection =
            await _connection.GetConnectionAsync(stoppingToken);

        await using var channel =
            await connection.CreateChannelAsync(
                cancellationToken: stoppingToken);

        await channel.QueueDeclareAsync(
            queue: _options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken);

        var batchSize = Math.Max(
            1,
            _options.ConsumerBatchSize);

        _logger.LogInformation(
            "RabbitMQ polling consumer {ConsumerId} started",
            consumerId);

        while (!stoppingToken.IsCancellationRequested)
        {
            var batch = new List<RabbitDelivery>(
                batchSize);

            for (var i = 0; i < batchSize; i++)
            {
                var result = await channel.BasicGetAsync(
                    queue: _options.QueueName,
                    autoAck: false,
                    cancellationToken: stoppingToken);

                if (result is null)
                    break;

                batch.Add(
                    new RabbitDelivery(
                        result.DeliveryTag,
                        result.Body.ToArray()));
            }

            if (batch.Count == 0)
            {
                await Task.Delay(
                    TimeSpan.FromMilliseconds(
                        Math.Max(
                            1,
                            _options.ConsumerPollIntervalMs)),
                    stoppingToken);

                continue;
            }

            await ProcessBatchAsync(
                channel,
                batch,
                consumerId,
                stoppingToken);
        }
    }

    private async Task ProcessBatchAsync(
        IChannel channel,
        IReadOnlyList<RabbitDelivery> deliveries,
        int consumerId,
        CancellationToken stoppingToken)
    {
        var validDeliveries =
            new List<(RabbitDelivery Delivery, List<LogEntry> Logs)>();

        foreach (var delivery in deliveries)
        {
            try
            {
                var parsedLogs =
                    JsonSerializer.Deserialize<List<LogEntry>>(
                        delivery.Body)
                    ?? throw new JsonException(
                        "RabbitMQ message contained no logs");

                validDeliveries.Add(
                    (delivery, parsedLogs));
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

        var combinedLogs = validDeliveries
            .SelectMany(x => x.Logs)
            .ToList();

        try
        {
            await WriteWithRetryAsync(
                combinedLogs,
                stoppingToken);

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
            when (ex is not OperationCanceledException ||
                  !stoppingToken.IsCancellationRequested)
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
                await _bulkWriter.WriteAsync(
                    logs,
                    stoppingToken);

                return;
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (attempt < maxAttempts)
            {
                last = ex;

                await Task.Delay(
                    TimeSpan.FromMilliseconds(25 * attempt),
                    stoppingToken);
            }
            catch (Exception ex)
            {
                last = ex;
            }
        }

        throw last ??
              new InvalidOperationException(
                  "Log batch write failed.");
    }

    private sealed record RabbitDelivery(
        ulong DeliveryTag,
        byte[] Body);
}