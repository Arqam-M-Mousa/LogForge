using LogForge.Api.Contracts.Common;
using LogForge.Domain.Aggregation;

namespace LogForge.Api.Contracts.Aggregation;

public static class AggregateLogsMapper
{
    private static readonly HashSet<string> AllowedBuckets = ["1m", "5m", "1h", "1d"];
    private static readonly HashSet<string> AllowedGroupBy = ["service", "level"];

    public static bool TryParse(
        AggregateLogsRequest request,
        IQueryCollection query,
        out LogAggregationFilter filter,
        out string? error)
    {
        filter = null!;
        error = null;

        if (!string.IsNullOrWhiteSpace(request.Level) && !LogFilterParsing.IsAllowedLevel(request.Level))
        {
            error = $"invalid level: '{request.Level}'";
            return false;
        }

        if (string.IsNullOrWhiteSpace(request.Since) || !LogFilterParsing.TryParseTimestamp(request.Since, out var since))
        {
            error = string.IsNullOrWhiteSpace(request.Since)
                ? "since is required"
                : $"invalid since: '{request.Since}'";
            return false;
        }

        if (string.IsNullOrWhiteSpace(request.Until) || !LogFilterParsing.TryParseTimestamp(request.Until, out var until))
        {
            error = string.IsNullOrWhiteSpace(request.Until)
                ? "until is required"
                : $"invalid until: '{request.Until}'";
            return false;
        }

        if (until < since)
        {
            error = "'until' must not be earlier than 'since'";
            return false;
        }

        if (string.IsNullOrWhiteSpace(request.Bucket) || !AllowedBuckets.Contains(request.Bucket))
        {
            error = string.IsNullOrWhiteSpace(request.Bucket)
                ? "bucket is required"
                : $"invalid bucket: '{request.Bucket}'";
            return false;
        }

        if (!string.IsNullOrWhiteSpace(request.GroupBy) && !AllowedGroupBy.Contains(request.GroupBy))
        {
            error = $"invalid group_by: '{request.GroupBy}'";
            return false;
        }

        filter = new LogAggregationFilter(
            request.Service,
            request.Level,
            since,
            until,
            request.Q,
            LogFilterParsing.ParseAttributeFilters(query),
            request.Bucket!,
            request.GroupBy);

        return true;
    }

    public static AggregateLogsResponse ToResponse(LogAggregationResult result) => new()
    {
        Buckets = result.Buckets
            .Select(bucket => new AggregateLogBucket
            {
                Start = bucket.Start,
                Group = bucket.Group,
                Count = bucket.Count
            })
            .ToList()
    };
}