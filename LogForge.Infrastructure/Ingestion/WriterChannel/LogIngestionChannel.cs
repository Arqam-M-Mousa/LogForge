using LogForge.Domain.Ingestion;
using Microsoft.Extensions.Options;
using System.Threading.Channels;

namespace LogForge.Infrastructure.Ingestion.WriterChannel;

public sealed class LogIngestionChannel
{
    private readonly Channel<IngestBatch> _channel;

    public LogIngestionChannel(IOptions<IngestionOptions> options)
    {
        var capacity = Math.Max(1, options.Value.ChannelCapacity);

        _channel = Channel.CreateBounded<IngestBatch>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });
    }

    public ChannelReader<IngestBatch> Reader => _channel.Reader;

    public async ValueTask PublishAsync(IReadOnlyList<LogEntry> logs, CancellationToken cancellationToken)
    {
        if (logs.Count == 0)
            return;

        await _channel.Writer.WriteAsync(new IngestBatch(logs), cancellationToken);
    }
}