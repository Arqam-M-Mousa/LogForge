using LogForge.Domain.Ingestion;
using LogForge.Domain.Ingestion.Abstractions;
using MassTransit;
using Microsoft.Extensions.Logging;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqPublisher : ILogIngestionService
{
    private readonly IBus _bus;
    private readonly ILogger<RabbitMqPublisher> _logger;

    public RabbitMqPublisher(
        IBus bus,
        ILogger<RabbitMqPublisher> logger)
    {
        _bus = bus;
        _logger = logger;
    }

    public ValueTask PublishAsync(IReadOnlyList<LogEntry> logs, CancellationToken cancellationToken)
    {
        if (logs.Count == 0)
            return ValueTask.CompletedTask;

        _ = PublishCoreAsync(logs as List<LogEntry> ?? [.. logs]);

        return ValueTask.CompletedTask;
    }

    private async Task PublishCoreAsync(List<LogEntry> logs)
    {
        try
        {
            await _bus.Publish(
                new IngestLogsBatch { Logs = logs },
                CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Fire-and-forget publish failed for batch of {Count} logs. Batch was NOT accepted by RabbitMQ.",
                logs.Count);
        }
    }
}
