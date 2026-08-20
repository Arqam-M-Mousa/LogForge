using System.Text.Json;
using LogForge.Domain.Ingestion;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
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
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "RabbitMQ ingestion worker failed; retrying");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task ConsumeAsync(CancellationToken stoppingToken)
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
            // Copy the body before the callback returns; RabbitMQ.Client reuses delivery memory.
            await deliveries.Writer.WriteAsync(
                new RabbitDelivery(delivery.DeliveryTag, delivery.Body.ToArray()),
                stoppingToken);
        };

        await channel.BasicConsumeAsync(_options.QueueName, autoAck: false, consumer);

        while (!stoppingToken.IsCancellationRequested)
        {
            var batch = new List<RabbitDelivery>(batchSize)
            {
                await deliveries.Reader.ReadAsync(stoppingToken)
            };

            if (batch.Count < batchSize)
            {
                await Task.Delay(
                    TimeSpan.FromMilliseconds(Math.Max(1, _options.ConsumerBatchWaitMs)),
                    stoppingToken);
            }

            while (batch.Count < batchSize && deliveries.Reader.TryRead(out var delivery))
                batch.Add(delivery);

            await ProcessBatchAsync(channel, batch, stoppingToken);
        }
    }

    private async Task ProcessBatchAsync(
        IChannel channel,
        IReadOnlyList<RabbitDelivery> deliveries,
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
                _logger.LogError(ex, "Discarding invalid RabbitMQ ingestion message");
                await channel.BasicNackAsync(delivery.DeliveryTag, false, false, stoppingToken);
            }
        }

        if (validDeliveries.Count == 0)
            return;

        var combinedLogs = validDeliveries.SelectMany(item => item.Logs).ToList();
        try
        {
            await WriteWithRetryAsync(combinedLogs, stoppingToken);

            foreach (var item in validDeliveries)
                await channel.BasicAckAsync(item.Delivery.DeliveryTag, false, stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
        {
            _logger.LogError(ex, "Failed to persist RabbitMQ message batch; requeueing");

            foreach (var item in validDeliveries)
                await channel.BasicNackAsync(item.Delivery.DeliveryTag, false, true, stoppingToken);
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
