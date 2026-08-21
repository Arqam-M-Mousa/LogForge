using LogForge.Domain.Ingestion;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class IngestLogsBatch
{
    public List<LogEntry> Logs { get; set; } = [];
}