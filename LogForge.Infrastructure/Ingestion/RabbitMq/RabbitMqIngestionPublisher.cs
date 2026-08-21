using LogForge.Domain.Ingestion;
using LogForge.Domain.Ingestion.Abstractions;
using MassTransit;
using Microsoft.Extensions.Options;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqIngestionPublisher : ILogIngestionService
{
    private readonly IBus _bus;
    private readonly Uri _queueAddress;
    private ISendEndpoint? _endpoint;

    public RabbitMqIngestionPublisher(IBus bus, IOptions<RabbitMqOptions> options)
    {
        _bus = bus;
        _queueAddress = new Uri($"queue:{options.Value.QueueName}");
    }

    public async ValueTask PublishAsync(
        IReadOnlyList<LogEntry> logs,
        CancellationToken cancellationToken)
    {
        if (logs.Count == 0)
            return;

        var endpoint = _endpoint ??= await _bus.GetSendEndpoint(_queueAddress);
        await endpoint.Send(
            new IngestLogsBatch { Logs = logs as List<LogEntry> ?? [.. logs] },
            cancellationToken);
    }
}