using LogForge.Domain.Ingestion.Abstractions;

namespace LogForge.Domain.Ingestion.Services;

public class LogIngestionService : ILogIngestionService
{
    public Task PublishAsync(IReadOnlyList<LogEntry> logs, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
