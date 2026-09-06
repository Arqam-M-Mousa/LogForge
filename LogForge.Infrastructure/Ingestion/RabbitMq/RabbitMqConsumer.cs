using LogForge.Domain.Ingestion;
using MassTransit;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqConsumer : IConsumer<Batch<IngestLogsBatch>>
{
    private readonly NpgsqlLogBulkWriter _bulkWriter;

    public RabbitMqConsumer(NpgsqlLogBulkWriter bulkWriter)
    {
        _bulkWriter = bulkWriter;
    }

    public async Task Consume(ConsumeContext<Batch<IngestLogsBatch>> context)
    {
        var logs = context.Message
            .SelectMany(message => message.Message.Logs)
            .ToList();

        if (logs.Count == 0)
            return;

        await WriteWithRetryAsync(logs, context.CancellationToken);
    }

    private async Task WriteWithRetryAsync(IReadOnlyList<LogEntry> logs, CancellationToken cancellationToken)
    {
        const int maxAttempts = 3;

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await _bulkWriter.WriteAsync(logs, cancellationToken);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (attempt >= maxAttempts)
            {
                throw new InvalidOperationException("Log batch write failed.", ex);
            }
            catch
            {
                await Task.Delay(TimeSpan.FromMilliseconds(25 * attempt), cancellationToken);
            }
        }
    }
}
