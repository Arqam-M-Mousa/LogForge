using LogForge.Domain.Ingestion;
using LogForge.Domain.Ingestion.Abstractions;

namespace LogForge.Infrastructure.Ingestion.WriterChannel;

public sealed class ChannelLogIngestionService : ILogIngestionService
{
    private readonly LogIngestionChannel _channel;

    public ChannelLogIngestionService(LogIngestionChannel channel)
    {
        _channel = channel;
    }

    public ValueTask PublishAsync(IReadOnlyList<LogEntry> logs, CancellationToken cancellationToken) =>
        _channel.PublishAsync(logs, cancellationToken);

}