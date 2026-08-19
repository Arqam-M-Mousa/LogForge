using LogForge.Domain.Ingestion;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LogForge.Infrastructure.Ingestion.WriterChannel;

public sealed class LogBatchWriterService : BackgroundService
{
    private readonly LogIngestionChannel _channel;
    private readonly NpgsqlLogBulkWriter _bulkWriter;
    private readonly IngestionOptions _options;
    private readonly ILogger<LogBatchWriterService> _logger;

    public LogBatchWriterService(
        LogIngestionChannel channel,
        NpgsqlLogBulkWriter bulkWriter,
        IOptions<IngestionOptions> options,
        ILogger<LogBatchWriterService> logger)
    {
        _channel = channel;
        _bulkWriter = bulkWriter;
        _options = options.Value;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return RunWriterLoopAsync(stoppingToken);
    }

    private async Task RunWriterLoopAsync(CancellationToken stoppingToken)
    {
        var reader = _channel.Reader;
        var maxBatchSize = Math.Max(1, _options.MaxBatchSize);
        var flushInterval = TimeSpan.FromMilliseconds(Math.Max(1, _options.FlushIntervalMs));
        var pending = new Queue<IngestBatch>();
        var logBuffer = new List<LogEntry>(maxBatchSize);
        var included = new List<IngestBatch>(16);

        while (!stoppingToken.IsCancellationRequested)
        {
            logBuffer.Clear();
            included.Clear();

            if (pending.Count == 0)
            {
                bool canRead;
                try
                {
                    canRead = await reader.WaitToReadAsync(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                if (!canRead)
                    return;

                while (reader.TryRead(out var batch))
                    pending.Enqueue(batch);
            }

            while (pending.Count > 0 && logBuffer.Count < maxBatchSize)
            {
                var batch = pending.Peek();
                if (logBuffer.Count > 0 && logBuffer.Count + batch.Logs.Count > maxBatchSize)
                    break;

                pending.Dequeue();
                included.Add(batch);
                logBuffer.AddRange(batch.Logs);
            }

            if (logBuffer.Count < maxBatchSize && pending.Count == 0)
            {
                var deadline = DateTime.UtcNow + flushInterval;

                while (logBuffer.Count < maxBatchSize)
                {
                    var remaining = deadline - DateTime.UtcNow;
                    if (remaining <= TimeSpan.Zero)
                        break;

                    var waitTask = reader.WaitToReadAsync(stoppingToken).AsTask();
                    var delayTask = Task.Delay(remaining, stoppingToken);
                    var completed = await Task.WhenAny(waitTask, delayTask);

                    if (completed != waitTask)
                        break;

                    bool moreAvailable;
                    try
                    {
                        moreAvailable = await waitTask;
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    if (!moreAvailable)
                        break;

                    while (reader.TryRead(out var batch))
                        pending.Enqueue(batch);

                    while (pending.Count > 0 && logBuffer.Count < maxBatchSize)
                    {
                        var batch = pending.Peek();
                        if (logBuffer.Count > 0 && logBuffer.Count + batch.Logs.Count > maxBatchSize)
                            break;

                        pending.Dequeue();
                        included.Add(batch);
                        logBuffer.AddRange(batch.Logs);
                    }
                }
            }

            if (included.Count == 0)
                continue;

            try
            {
                await WriteWithRetryAsync(logBuffer, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write batch of {Count} logs to PostgreSQL", logBuffer.Count);
            }
        }
    }

    private async Task WriteWithRetryAsync(IReadOnlyList<LogEntry> logs, CancellationToken stoppingToken)
    {
        const int maxAttempts = 3;
        Exception? last = null;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await _bulkWriter.WriteAsync(logs, stoppingToken);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (attempt < maxAttempts)
            {
                last = ex;
                await Task.Delay(TimeSpan.FromMilliseconds(25 * attempt), stoppingToken);
            }
            catch (Exception ex)
            {
                last = ex;
            }
        }

        throw last ?? new InvalidOperationException("Log batch write failed.");
    }
}