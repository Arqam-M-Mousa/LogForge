namespace LogForge.Domain.Query;

public sealed record LogQueryFilter(
    string? Service,
    string? Level,
    DateTimeOffset? Since,
    DateTimeOffset? Until,
    string? MessageContains,
    IReadOnlyDictionary<string, string> Attributes,
    int Limit,
    DateTimeOffset? CursorTimeStamp,
    long? CursorId);
