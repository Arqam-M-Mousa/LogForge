using LogForge.Domain.Ingestion;

namespace LogForge.Infrastructure.Ingestion.WriterChannel;

public sealed class IngestBatch
{
    public IngestBatch(IReadOnlyList<LogEntry> logs)
    {
        Logs = logs;
    }

    public IReadOnlyList<LogEntry> Logs { get; }
}