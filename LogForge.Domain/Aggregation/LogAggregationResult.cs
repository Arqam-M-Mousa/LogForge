namespace LogForge.Domain.Aggregation;

public sealed record LogAggregationBucket(
    DateTimeOffset Start,
    string? Group,
    long Count);

public sealed record LogAggregationResult(
    IReadOnlyList<LogAggregationBucket> Buckets);