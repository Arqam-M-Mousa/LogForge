using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using System.Globalization;

namespace LogForge.Infrastructure.Retention;

public sealed class LogRetentionService : BackgroundService
{
    private const string DeleteExpiredRollupsSql = """
        DELETE FROM log_minute_rollup
        WHERE "BucketStart" < date_bin('1 minute'::interval, @cutoff, '2000-01-01T00:00:00Z'::timestamptz)
        """;

    private const string DeleteExpiredLogsSql = """
        DELETE FROM log
        WHERE "Id" IN
        (
            SELECT "Id"
            FROM log
            WHERE "Timestamp" < @cutoff
            ORDER BY "Timestamp", "Id"
            LIMIT @batchSize
        )
        """;

    private readonly NpgsqlDataSource _dataSource;
    private readonly RetentionOptions _options;
    private readonly ILogger<LogRetentionService> _logger;

    public LogRetentionService(
        NpgsqlDataSource dataSource,
        IOptions<RetentionOptions> options,
        ILogger<LogRetentionService> logger)
    {
        _dataSource = dataSource;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(GetDelayUntilNextRun(), stoppingToken);
                await DeleteExpiredDataAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Log retention failed");
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
            }
        }
    }

    private TimeSpan GetDelayUntilNextRun()
    {
        var now = DateTimeOffset.UtcNow;
        var runAt = _options.RunAtUtc is { Ticks: >= 0 and < TimeSpan.TicksPerDay }
            ? _options.RunAtUtc
            : TimeSpan.FromHours(2);
        var nextRun = new DateTimeOffset(now.Date.Add(runAt), TimeSpan.Zero);

        if (nextRun <= now)
            nextRun = nextRun.AddDays(1);

        return nextRun - now;
    }

    private async Task DeleteExpiredDataAsync(CancellationToken cancellationToken)
    {
        var retentionDays = Math.Max(1, _options.RetentionDays);
        var batchSize = Math.Clamp(_options.DeleteBatchSize, 1, 100_000);
        var cutoff = DateTimeOffset.UtcNow.AddDays(-retentionDays);
        var deletedRollups = 0;
        var deletedLogs = 0;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var droppedPartitions = await DropExpiredPartitionsAsync(connection, transaction, cutoff, cancellationToken);

        await using (var rollupCommand = new NpgsqlCommand(DeleteExpiredRollupsSql, connection, transaction))
        {
            rollupCommand.Parameters.Add(new NpgsqlParameter("cutoff", NpgsqlDbType.TimestampTz) { Value = cutoff });
            deletedRollups = await rollupCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        while (true)
        {
            await using var command = new NpgsqlCommand(DeleteExpiredLogsSql, connection, transaction);
            command.Parameters.Add(new NpgsqlParameter("cutoff", NpgsqlDbType.TimestampTz) { Value = cutoff });
            command.Parameters.Add(new NpgsqlParameter("batchSize", NpgsqlDbType.Integer) { Value = batchSize });

            var deletedInBatch = await command.ExecuteNonQueryAsync(cancellationToken);
            deletedLogs += deletedInBatch;

            if (deletedInBatch < batchSize)
                break;
        }

        await transaction.CommitAsync(cancellationToken);

        _logger.LogInformation(
            "Retention: dropped {Partitions} partitions and deleted {RollupRows} rollup rows and {LogRows} logs older than {Cutoff:O}",
            droppedPartitions, deletedRollups, deletedLogs, cutoff);
    }

    private static async Task<int> DropExpiredPartitionsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DateTimeOffset cutoff,
        CancellationToken cancellationToken)
    {
        const string listSql = """
            SELECT child.relname
            FROM pg_inherits
            JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
            JOIN pg_class child ON child.oid = pg_inherits.inhrelid
            WHERE parent.relname = 'log' AND child.relname LIKE 'log________'
            """;

        var names = new List<string>();
        await using (var listCommand = new NpgsqlCommand(listSql, connection, transaction))
        await using (var reader = await listCommand.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var name = reader.GetString(0);
                if (DateTime.TryParseExact(name.AsSpan(4), "yyyyMMdd", CultureInfo.InvariantCulture,
                        DateTimeStyles.None, out var day) && day.AddDays(1) <= cutoff.UtcDateTime.Date)
                    names.Add(name);
            }
        }

        foreach (var name in names)
        {
            await using var dropCommand = new NpgsqlCommand($"DROP TABLE IF EXISTS \"{name}\"", connection, transaction);
            await dropCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        return names.Count;
    }
}
