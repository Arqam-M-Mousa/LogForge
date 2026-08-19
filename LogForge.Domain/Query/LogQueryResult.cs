namespace LogForge.Domain.Query;

public sealed record LogQueryItem(
    long Id,
    DateTimeOffset Timestamp,
    string Level,
    string Service,
    string Message,
    IReadOnlyDictionary<string, object>? Attributes);

public sealed record LogQueryResult(
    IReadOnlyList<LogQueryItem> Logs,
    long HasMore);
