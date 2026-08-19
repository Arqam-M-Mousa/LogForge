namespace LogForge.Domain.Ingestion.Abstractions;

public interface ILogIngestionService
{
    public ValueTask PublishAsync(IReadOnlyList<LogEntry> logs, CancellationToken cancellationToken);
}
