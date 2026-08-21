using LogForge.Domain.Ingestion;
using Npgsql;
using NpgsqlTypes;
using System.Text.Json;

namespace LogForge.Infrastructure.Ingestion;

public sealed class NpgsqlLogBulkWriter
{
    private const string RollupUpsertSql = """
        INSERT INTO log_minute_rollup ("BucketStart", "Service", "Level", "LogCount")
        SELECT date_bin('1 minute'::interval, t, '2000-01-01T00:00:00Z'::timestamptz), s, l, COUNT(*)
        FROM unnest(@timestamps, @services, @levels) AS source(t, s, l)
        GROUP BY 1, 2, 3
        ON CONFLICT ("BucketStart", "Service", "Level")
        DO UPDATE SET "LogCount" = log_minute_rollup."LogCount" + EXCLUDED."LogCount";
        """;

    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    private readonly NpgsqlDataSource _dataSource;

    public NpgsqlLogBulkWriter(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource;
    }

    public async Task WriteAsync(
        IReadOnlyList<LogEntry> logs,
        CancellationToken cancellationToken)
    {
        if (logs.Count == 0)
            return;

        var timestamps = new DateTimeOffset[logs.Count];
        var services = new string[logs.Count];
        var levels = new string[logs.Count];

        for (var i = 0; i < logs.Count; i++)
        {
            timestamps[i] = logs[i].Timestamp;
            services[i] = logs[i].Service;
            levels[i] = logs[i].Level;
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
            rollupCommand.Parameters.Add(new NpgsqlParameter("timestamps", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz) { Value = timestamps });
            rollupCommand.Parameters.Add(new NpgsqlParameter("services", NpgsqlDbType.Array | NpgsqlDbType.Varchar) { Value = services });
            rollupCommand.Parameters.Add(new NpgsqlParameter("levels", NpgsqlDbType.Array | NpgsqlDbType.Varchar) { Value = levels });
            await rollupCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }
}