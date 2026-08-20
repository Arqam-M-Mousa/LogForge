using LogForge.Domain.Ingestion;
using LogForge.Domain.Ingestion.Abstractions;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Text.Json;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqIngestionService : ILogIngestionService, IAsyncDisposable
{
    private readonly RabbitMqConnection _connection;
    private readonly RabbitMqOptions _options;
    private readonly SemaphoreSlim _publishGate = new(1, 1);
    private IChannel? _channel;

    public RabbitMqIngestionService(
        RabbitMqConnection connection,
        IOptions<RabbitMqOptions> options)
    {
        _connection = connection;
        _options = options.Value;
    }

    public async ValueTask PublishAsync(
        IReadOnlyList<LogEntry> logs,
        CancellationToken cancellationToken)
    {
        if (logs.Count == 0)
            return;

        var body = JsonSerializer.SerializeToUtf8Bytes(logs);

        await _publishGate.WaitAsync(cancellationToken);
        try
        {
            if (_channel is not { IsOpen: true })
            {
                if (_channel is not null)
                    await _channel.DisposeAsync();

                _channel = await CreateChannelAsync(cancellationToken);
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
                cancellationToken: cancellationToken);
        }
        finally
        {
            _publishGate.Release();
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
