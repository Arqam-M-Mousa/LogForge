namespace LogForge.Infrastructure.Aggregation.Cache;

public sealed class AggregationCacheOptions
{
    public const string SectionName = "AggregationCache";
    public int TtlSeconds { get; init; } = 5;
    public int MaxEntries { get; init; } = 256;
    public int QueryTimeoutSeconds { get; init; } = 8;
}