using LogForge.Api.Contracts.Common;
using LogForge.Domain.Query;

namespace LogForge.Api.Contracts.Query;

public static class QueryLogsMapper
{
    public static bool TryParse(
        QueryLogsRequest request,
        IQueryCollection query,
        out LogQueryFilter filter,
        out string? error)
    {
        filter = null!;
        error = null;

        if (!string.IsNullOrWhiteSpace(request.Level) && !LogFilterParsing.IsAllowedLevel(request.Level))
        {
            error = $"invalid level: '{request.Level}'";
            return false;
        }

        DateTimeOffset? since = null;

        if (!string.IsNullOrWhiteSpace(request.Since))
        {
            if (!LogFilterParsing.TryParseTimestamp(request.Since, out var parsedSince))
            {
                error = $"invalid since: '{request.Since}'";
                return false;
            }

            since = parsedSince;
        }

        DateTimeOffset? until = null;

        if (!string.IsNullOrWhiteSpace(request.Until))
        {
            if (!LogFilterParsing.TryParseTimestamp(request.Until, out var parsedUntil))
            {
                error = $"invalid until: '{request.Until}'";
                return false;
            }

            until = parsedUntil;
        }

        if (since is { } start && until is { } end && end < start)
        {
            error = "'until' must not be earlier than 'since'";
            return false;
        }

        var limit = 100;

        if (!string.IsNullOrWhiteSpace(request.Limit))
        {
            if (!int.TryParse(request.Limit, out limit) || limit is < 1 or > 1000)
            {
                error = "limit must be a number between 1 and 1000";
                return false;
            }
        }

        LogCursor? cursor = null;

        if (!string.IsNullOrWhiteSpace(request.Cursor))
        {
            if (!LogCursor.TryDecode(request.Cursor, out cursor))
            {
                error = "invalid cursor";
                return false;
            }
        }

        filter = new LogQueryFilter(
            request.Service,
            request.Level,
            since,
            until,
            request.Q,
            LogFilterParsing.ParseAttributeFilters(query),
            limit,
            cursor?.Timestamp,
            cursor?.Id);

        return true;
    }

    public static QueryLogsResponse ToResponse(LogQueryResult result)
    {
        var logs = result.Logs
            .Select(log => new QueryLogItem
            {
                Id = log.Id.ToString(),
                Timestamp = log.Timestamp,
                Level = log.Level,
                Service = log.Service,
                Message = log.Message,
                Attributes = log.Attributes?.ToDictionary(attribute => attribute.Key, attribute => attribute.Value)
            })
            .ToList();

        var nextCursor = result.HasMore > 0 && result.Logs.Count > 0
            ? new LogCursor(result.Logs[^1].Timestamp, result.Logs[^1].Id).Encode()
            : null;

        return new QueryLogsResponse { Logs = logs, NextCursor = nextCursor };
    }
}