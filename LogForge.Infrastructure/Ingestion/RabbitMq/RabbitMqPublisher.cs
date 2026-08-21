using LogForge.Domain.Ingestion;
using LogForge.Domain.Ingestion.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Text.Json;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqPublisher : ILogIngestionService, IAsyncDisposable
{
    private readonly RabbitMqConnection _connection;
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqPublisher> _logger;
    private readonly SemaphoreSlim _publishGate = new(1, 1);
    private IChannel? _channel;

    public RabbitMqPublisher(
        RabbitMqConnection connection,
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqPublisher> logger)
    {
        _connection = connection;
        _options = options.Value;
        _logger = logger;
    }

    public ValueTask PublishAsync(IReadOnlyList<LogEntry> logs, CancellationToken cancellationToken)
    {
        if (logs.Count == 0)
            return ValueTask.CompletedTask;

        var logsList = logs as List<LogEntry> ?? [.. logs];

        _ = PublishInBackgroundAsync(logsList);

        return ValueTask.CompletedTask;
    }

    private async Task PublishInBackgroundAsync(List<LogEntry> logs)
    {
        try
        {
            var body = JsonSerializer.SerializeToUtf8Bytes(logs);

            await _publishGate.WaitAsync(CancellationToken.None);
            try
            {
                if (_channel is not { IsOpen: true })
                {
                    if (_channel is not null)
                        await _channel.DisposeAsync();

                    _channel = await CreateChannelAsync(CancellationToken.None);
                }

                var properties = new BasicProperties
                {
                    ContentType = "application/json",
                    DeliveryMode = DeliveryModes.Persistent
                };

                await _channel.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: _options.QueueName,
                    mandatory: true,
                    basicProperties: properties,
                    body: body,
                    cancellationToken: CancellationToken.None);
            }
            finally
            {
                _publishGate.Release();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Fire-and-forget publish failed for batch of {Count} logs. Batch was NOT accepted by RabbitMQ.",
                logs.Count);
        }
    }

    private async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken)
    {
        var connection = await _connection.GetConnectionAsync(cancellationToken);
        var channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(true, true, null, null),
            cancellationToken);

        await channel.QueueDeclareAsync(
            queue: _options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken);

        return channel;
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
            await _channel.DisposeAsync();

        _publishGate.Dispose();
    }
}