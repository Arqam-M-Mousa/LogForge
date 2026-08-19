using LogForge.Domain.Aggregation;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace LogForge.Infrastructure.Aggregation.Cache;

public sealed class AggregateResultCache
{
    private readonly IMemoryCache _memoryCache;
    private readonly TimeSpan _ttl;

    public AggregateResultCache(IMemoryCache memoryCache, IOptions<AggregationCacheOptions> options)
    {
        _memoryCache = memoryCache;
        _ttl = TimeSpan.FromSeconds(Math.Max(1, options.Value.TtlSeconds));
    }

    public async Task<LogAggregationResult> GetOrAddAsync(
        LogAggregationFilter filter,
        Func<Task<LogAggregationResult>> factory)
    {
        var key = BuildKey(filter);

        if (_memoryCache.TryGetValue(key, out LogAggregationResult? cached) && cached is not null)
            return cached;

        var result = await factory();

        _memoryCache.Set(key, result, new MemoryCacheEntryOptions
        {
            Size = 1,
            AbsoluteExpirationRelativeToNow = _ttl
        });

        return result;
    }

    private static string BuildKey(LogAggregationFilter filter)
    {
        var attributes = string.Join(',',
            filter.AttributeFilters
                .OrderBy(pair => pair.Key, StringComparer.Ordinal)
                .Select(pair => pair.Key + '=' + pair.Value));

        return $"aggregate|{filter.Service}|{filter.Level}|{filter.Since:O}|{filter.Until:O}|" +
               $"{filter.MessageContains}|{attributes}|{filter.Bucket}|{filter.GroupBy}";
    }
}