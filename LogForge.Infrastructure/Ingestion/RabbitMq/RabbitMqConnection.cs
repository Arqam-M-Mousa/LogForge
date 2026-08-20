using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqConnection : IAsyncDisposable
{
    private readonly RabbitMqOptions _options;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IConnection? _connection;

    public RabbitMqConnection(IOptions<RabbitMqOptions> options)
    {
        _options = options.Value;
    }

    public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection is { IsOpen: true })
            return _connection;

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_connection is { IsOpen: true })
                return _connection;

            var factory = new ConnectionFactory
            {
                Uri = new Uri(_options.ConnectionString),
                ClientProvidedName = "logforge",
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
            };

            _connection = await factory.CreateConnectionAsync(cancellationToken);
            return _connection;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
            await _connection.DisposeAsync();

        _gate.Dispose();
    }
}
