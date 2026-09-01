namespace LogForge.Domain.Ingestion.Abstractions;

public interface ILogIngestionService
{
    ValueTask PublishAsync(IReadOnlyList<LogEntry> logs, CancellationToken cancellationToken);
}
