using LogForge.Domain.Aggregation;
using LogForge.Domain.Aggregation.Abstractions;
using LogForge.Infrastructure.Aggregation.Cache;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using System.Text;

namespace LogForge.Infrastructure.Aggregation;

public sealed class RollupLogAggregationService : ILogAggregationService
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly AggregateResultCache _cache;
    private readonly int _commandTimeoutSeconds;

    public RollupLogAggregationService(
        NpgsqlDataSource dataSource,
        AggregateResultCache cache,
        IOptions<AggregationCacheOptions> options)
    {
        _dataSource = dataSource;
        _cache = cache;
        _commandTimeoutSeconds = Math.Max(1, options.Value.QueryTimeoutSeconds);
    }

    public Task<LogAggregationResult> AggregateAsync(
        LogAggregationFilter filter,
        CancellationToken cancellationToken)
    {
        return _cache.GetOrAddAsync(
            filter,
            () => UsesRawLogs(filter)
                ? ExecuteRawLogsAsync(filter, cancellationToken)
                : ExecuteRollupAsync(filter, cancellationToken));
    }

    private static bool UsesRawLogs(LogAggregationFilter filter) =>
        !string.IsNullOrWhiteSpace(filter.MessageContains) || filter.AttributeFilters.Count > 0;

    private async Task<LogAggregationResult> ExecuteRollupAsync(
        LogAggregationFilter filter,
        CancellationToken cancellationToken)
    {
        var groupColumn = filter.GroupBy switch
        {
            "service" => "service",
            "level" => "level",
            _ => null
        };

        var sql = new StringBuilder(
            "SELECT date_bin(@bucket::interval, bucket_start, '2000-01-01T00:00:00Z'::timestamptz) AS start, ");
        sql.Append(groupColumn ?? "NULL::text").Append(" AS group, SUM(log_count) AS count ")
           .Append("FROM log_minute_rollup ")
           .Append("WHERE bucket_start >= date_bin('1 minute'::interval, @since, '2000-01-01T00:00:00Z'::timestamptz) ")
           .Append("AND bucket_start < date_bin('1 minute'::interval, @until, '2000-01-01T00:00:00Z'::timestamptz) + interval '1 minute'");

        var parameters = new List<NpgsqlParameter>
        {
            new("bucket", NpgsqlDbType.Varchar) { Value = filter.Bucket },
            new("since", NpgsqlDbType.TimestampTz) { Value = filter.Since },
            new("until", NpgsqlDbType.TimestampTz) { Value = filter.Until }
        };
        var paramIndex = 0;

        string NextParamName() => $"p{paramIndex++}";

        if (!string.IsNullOrWhiteSpace(filter.Service))
        {
            var name = NextParamName();
            sql.Append(" AND service = @").Append(name);
            parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Varchar) { Value = filter.Service });
        }

        if (!string.IsNullOrWhiteSpace(filter.Level))
        {
            var name = NextParamName();
            sql.Append(" AND level = @").Append(name);
            parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Varchar) { Value = filter.Level });
        }

        sql.Append(" GROUP BY 1");
        if (groupColumn is not null)
            sql.Append(", 2");

        return await ExecuteAsync(sql.ToString(), parameters, cancellationToken);
    }

    private async Task<LogAggregationResult> ExecuteRawLogsAsync(
        LogAggregationFilter filter,
        CancellationToken cancellationToken)
    {
        var groupColumn = filter.GroupBy switch
        {
            "service" => "\"Service\"",
            "level" => "\"Level\"",
            _ => null
        };

        var sql = new StringBuilder(
            "SELECT date_bin(@bucket::interval, \"Timestamp\", '2000-01-01T00:00:00Z'::timestamptz) AS start, ");
        sql.Append(groupColumn ?? "NULL::text").Append(" AS group, COUNT(*) AS count ")
           .Append("FROM log WHERE \"Timestamp\" >= @since AND \"Timestamp\" < @until");

        var parameters = new List<NpgsqlParameter>
        {
            new("bucket", NpgsqlDbType.Varchar) { Value = filter.Bucket },
            new("since", NpgsqlDbType.TimestampTz) { Value = filter.Since },
            new("until", NpgsqlDbType.TimestampTz) { Value = filter.Until }
        };
        var paramIndex = 0;

        string NextParamName() => $"p{paramIndex++}";

        if (!string.IsNullOrWhiteSpace(filter.Service))
        {
            var name = NextParamName();
            sql.Append(" AND \"Service\" = @").Append(name);
            parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Varchar) { Value = filter.Service });
        }

        if (!string.IsNullOrWhiteSpace(filter.Level))
        {
            var name = NextParamName();
            sql.Append(" AND \"Level\" = @").Append(name);
            parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Varchar) { Value = filter.Level });
        }

        if (!string.IsNullOrWhiteSpace(filter.MessageContains))
        {
            var name = NextParamName();
            sql.Append(" AND \"Message\" ILIKE @").Append(name);
            parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Varchar) { Value = $"%{filter.MessageContains}%" });
        }

        foreach (var (key, value) in filter.AttributeFilters)
        {
            var keyName = NextParamName();
            var valueName = NextParamName();
            sql.Append(" AND \"Attributes\" ->> @").Append(keyName).Append(" = @").Append(valueName);
            parameters.Add(new NpgsqlParameter(keyName, NpgsqlDbType.Varchar) { Value = key });
            parameters.Add(new NpgsqlParameter(valueName, NpgsqlDbType.Varchar) { Value = value });
        }

        sql.Append(" GROUP BY 1");
        if (groupColumn is not null)
            sql.Append(", 2");

        return await ExecuteAsync(sql.ToString(), parameters, cancellationToken);
    }

    private async Task<LogAggregationResult> ExecuteAsync(
        string sql,
        List<NpgsqlParameter> parameters,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection)
        {
            CommandTimeout = _commandTimeoutSeconds
        };
        command.Parameters.AddRange(parameters.ToArray());

        var buckets = new List<LogAggregationBucket>();

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            buckets.Add(new LogAggregationBucket(
                reader.GetFieldValue<DateTimeOffset>(0),
                await reader.IsDBNullAsync(1, cancellationToken) ? null : reader.GetString(1),
                reader.GetInt64(2)));
        }

        buckets.Sort(static (a, b) =>
        {
            var result = a.Start.CompareTo(b.Start);
            return result != 0 ? result : string.CompareOrdinal(a.Group, b.Group);
        });

        return new LogAggregationResult(buckets);
    }
}