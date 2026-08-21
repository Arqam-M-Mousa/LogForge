using LogForge.Domain.Ingestion;
using MassTransit;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqIngestionConsumer : IConsumer<IngestLogsBatch>
{
    private readonly NpgsqlLogBulkWriter _bulkWriter;

    public RabbitMqIngestionConsumer(NpgsqlLogBulkWriter bulkWriter)
    {
        _bulkWriter = bulkWriter;
    }

    public async Task Consume(ConsumeContext<IngestLogsBatch> context)
    {
        var logs = context.Message.Logs;
        if (logs is not { Count: > 0 })
            return;

        await WriteWithRetryAsync(logs, context.CancellationToken);
    }

    private async Task WriteWithRetryAsync(
        IReadOnlyList<LogEntry> logs,
        CancellationToken stoppingToken)
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