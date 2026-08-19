namespace LogForge.Domain.Ingestion.Abstractions;

public interface ILogIngestionService
{
    Task PublishAsync(IReadOnlyList<LogEntry> logs, CancellationToken cancellationToken);
}
