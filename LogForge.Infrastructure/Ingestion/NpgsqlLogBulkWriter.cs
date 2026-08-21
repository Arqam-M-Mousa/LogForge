using LogForge.Domain.Ingestion;
using Npgsql;
using NpgsqlTypes;
using System.Text.Json;

namespace LogForge.Infrastructure.Ingestion;

public sealed class NpgsqlLogBulkWriter
{
    private const string RollupUpsertSql = """
        INSERT INTO log_minute_rollup ("BucketStart", "Service", "Level", "LogCount")
        SELECT t, s, l, c
        FROM unnest(@timestamps, @services, @levels, @counts) AS source(t, s, l, c)
        ON CONFLICT ("BucketStart", "Service", "Level")
        DO UPDATE SET "LogCount" = log_minute_rollup."LogCount" + EXCLUDED."LogCount";
        """;

    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };
    private readonly NpgsqlDataSource _dataSource;

    public NpgsqlLogBulkWriter(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource;
    }

    public async Task WriteAsync(IReadOnlyList<LogEntry> logs, CancellationToken cancellationToken)
    {
        if (logs.Count == 0)
            return;

        var rollups = new Dictionary<(DateTimeOffset Bucket, string Service, string Level), int>();

        foreach (var log in logs)
        {
            var utc = log.Timestamp.UtcDateTime;
            var bucket = new DateTimeOffset(
                utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, 0, TimeSpan.Zero);

            var key = (bucket, log.Service, log.Level);
            rollups[key] = rollups.GetValueOrDefault(key) + 1;
        }

        var rTimestamps = new DateTimeOffset[rollups.Count];
        var rServices = new string[rollups.Count];
        var rLevels = new string[rollups.Count];
        var rCounts = new int[rollups.Count];

        var idx = 0;
        foreach (var (key, count) in rollups)
        {
            rTimestamps[idx] = key.Bucket;
            rServices[idx] = key.Service;
            rLevels[idx] = key.Level;
            rCounts[idx] = count;
            idx++;
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var writer = await connection.BeginBinaryImportAsync(
            "COPY log (\"Timestamp\", \"Level\", \"Service\", \"Message\", \"Attributes\") FROM STDIN (FORMAT BINARY)",
            cancellationToken))
        {
            foreach (var log in logs)
            {
                await writer.StartRowAsync(cancellationToken);
                await writer.WriteAsync(log.Timestamp, NpgsqlDbType.TimestampTz, cancellationToken);
                await writer.WriteAsync(log.Level, NpgsqlDbType.Varchar, cancellationToken);
                await writer.WriteAsync(log.Service, NpgsqlDbType.Varchar, cancellationToken);
                await writer.WriteAsync(log.Message, NpgsqlDbType.Varchar, cancellationToken);

                var payload = JsonSerializer.SerializeToUtf8Bytes(log.Attributes, JsonOptions);
                await writer.WriteAsync(payload, NpgsqlDbType.Jsonb, cancellationToken);
            }

            await writer.CompleteAsync(cancellationToken);
        }

        await using (var rollupCommand = new NpgsqlCommand(RollupUpsertSql, connection, transaction))
        {
            rollupCommand.Parameters.Add(new NpgsqlParameter("timestamps", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz) { Value = rTimestamps });
            rollupCommand.Parameters.Add(new NpgsqlParameter("services", NpgsqlDbType.Array | NpgsqlDbType.Varchar) { Value = rServices });
            rollupCommand.Parameters.Add(new NpgsqlParameter("levels", NpgsqlDbType.Array | NpgsqlDbType.Varchar) { Value = rLevels });
            rollupCommand.Parameters.Add(new NpgsqlParameter("counts", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = rCounts });
            await rollupCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }
}