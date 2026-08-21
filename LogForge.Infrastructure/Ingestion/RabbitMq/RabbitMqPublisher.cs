using LogForge.Domain.Ingestion;
using LogForge.Domain.Ingestion.Abstractions;
using MassTransit;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqPublisher : ILogIngestionService
{
    private readonly IPublishEndpoint _publishEndpoint;

    public RabbitMqPublisher(IPublishEndpoint publishEndpoint)
    {
        _publishEndpoint = publishEndpoint;
    }

    public async ValueTask PublishAsync(
        IReadOnlyList<LogEntry> logs,
        CancellationToken cancellationToken)
    {
        if (logs.Count == 0)
            return;

        var logsList = logs as List<LogEntry> ?? [.. logs];

        await _publishEndpoint.Publish(
            new IngestLogsBatch { Logs = logsList },
            cancellationToken);
    }
}