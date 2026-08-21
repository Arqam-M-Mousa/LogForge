using LogForge.Domain.Ingestion;
using MassTransit;
using Microsoft.Extensions.Logging;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqConsumer : IConsumer<Batch<IngestLogsBatch>>
{
    private readonly NpgsqlLogBulkWriter _bulkWriter;
    private readonly ILogger<RabbitMqConsumer> _logger;

    public RabbitMqConsumer(
        NpgsqlLogBulkWriter bulkWriter,
        ILogger<RabbitMqConsumer> logger)
    {
        _bulkWriter = bulkWriter;
        _logger = logger;
    }

    public async Task Consume(ConsumeContext<Batch<IngestLogsBatch>> context)
    {
        if (context.Message.Length == 0)
            return;

        var logs = new List<LogEntry>();
        for (var i = 0; i < context.Message.Length; i++)
        {
            var batchMessage = context.Message[i].Message;
            if (batchMessage.Logs is { Count: > 0 })
            {
                logs.AddRange(batchMessage.Logs);
            }
        }

        if (logs.Count == 0)
            return;

        await _bulkWriter.WriteAsync(logs, context.CancellationToken);
        _logger.LogInformation("Flushed batch of {Count} logs.", logs.Count);
    }
}
