using LogForge.Domain.Ingestion;
using LogForge.Domain.Ingestion.Abstractions;
using MassTransit;
using Microsoft.Extensions.Logging;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqPublisher : ILogIngestionService
{
    private readonly IPublishEndpoint _publishEndpoint;
    private readonly ILogger<RabbitMqPublisher> _logger;

    public RabbitMqPublisher(IPublishEndpoint publishEndpoint, ILogger<RabbitMqPublisher> logger)
    {
        _publishEndpoint = publishEndpoint;
        _logger = logger;
    }

    public ValueTask PublishAsync(
        IReadOnlyList<LogEntry> logs,
        CancellationToken cancellationToken)
    {
        if (logs.Count == 0)
            return ValueTask.CompletedTask;

        var logsList = logs as List<LogEntry> ?? [.. logs];

        _ = PublishInBackgroundAsync(logsList);

        return ValueTask.CompletedTask;
    }

    private async Task PublishInBackgroundAsync(List<LogEntry> logsList)
    {
        try
        {
            await _publishEndpoint.Publish(
                new IngestLogsBatch { Logs = logsList },
                CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Fire-and-forget publish failed for batch of {Count} logs. Batch was NOT accepted by RabbitMQ.",
                logsList.Count);
        }
    }
}