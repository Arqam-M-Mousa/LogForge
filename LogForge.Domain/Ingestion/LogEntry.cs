namespace LogForge.Domain.Ingestion;

public sealed record LogEntry(
    DateTimeOffset Timestamp,
    string Level,
    string Service,
    string Message,
    IReadOnlyDictionary<string, object>? Attributes);
