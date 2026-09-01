using LogForge.Domain.Ingestion;
using LogForge.Domain.Ingestion.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Text.Json;
using System.Threading.Channels;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqPublisher : ILogIngestionService, IAsyncDisposable
{
    private const int MaxInFlightBatches = 250;

    private readonly RabbitMqConnection _connection;
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqPublisher> _logger;
    private readonly Channel<List<LogEntry>> _queue;
    private readonly Task _pump;
    private IChannel? _channel;

    public RabbitMqPublisher(
        RabbitMqConnection connection,
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqPublisher> logger)
    {
        _connection = connection;
        _options = options.Value;
        _logger = logger;
        _queue = Channel.CreateBounded<List<LogEntry>>(new BoundedChannelOptions(MaxInFlightBatches)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.DropWrite
        });
        _pump = PumpAsync();
    }

    public ValueTask PublishAsync(IReadOnlyList<LogEntry> logs, CancellationToken cancellationToken)
    {
        if (logs.Count == 0)
            return ValueTask.CompletedTask;

        var batch = logs as List<LogEntry> ?? [.. logs];

        if (!_queue.Writer.TryWrite(batch))
            _logger.LogWarning("Publish backlog full, dropping batch of {Count} logs to avoid unbounded memory growth", batch.Count);

        return ValueTask.CompletedTask;
    }

    private async Task PumpAsync()
    {
        await foreach (var batch in _queue.Reader.ReadAllAsync(CancellationToken.None))
        {
            try
            {
                await PublishCoreAsync(batch);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Fire-and-forget publish failed for batch of {Count} logs. Batch was NOT accepted by RabbitMQ.",
                    batch.Count);
            }
        }
    }

    private async Task PublishCoreAsync(List<LogEntry> logs)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(logs);

        if (_channel is not { IsOpen: true })
        {
            if (_channel is not null)
                await _channel.DisposeAsync();

            _channel = await CreateChannelAsync();
        }

        var properties = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Transient
        };

        await _channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: _options.QueueName,
            mandatory: true,
            basicProperties: properties,
            body: body,
            cancellationToken: CancellationToken.None);
    }

    private async Task<IChannel> CreateChannelAsync()
    {
        var connection = await _connection.GetConnectionAsync(CancellationToken.None);
        var channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(true, true, null, null),
            CancellationToken.None);

        await channel.QueueDeclareAsync(
            queue: _options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: CancellationToken.None);

        return channel;
    }

    public async ValueTask DisposeAsync()
    {
        _queue.Writer.TryComplete();
        await _pump;

        if (_channel is not null)
            await _channel.DisposeAsync();
    }
}
