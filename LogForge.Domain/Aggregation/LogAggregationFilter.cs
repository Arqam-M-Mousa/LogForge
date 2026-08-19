namespace LogForge.Domain.Aggregation;

public sealed record LogAggregationFilter(
    string? Service,
    string? Level,
    DateTimeOffset Since,
    DateTimeOffset Until,
    string? MessageContains,
    IReadOnlyDictionary<string, string> AttributeFilters,
    string Bucket,
    string? GroupBy);