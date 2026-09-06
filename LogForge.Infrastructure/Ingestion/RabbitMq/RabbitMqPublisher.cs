using LogForge.Domain.Ingestion;
using LogForge.Domain.Ingestion.Abstractions;
using MassTransit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Threading.Channels;

namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqPublisher : ILogIngestionService, IAsyncDisposable
{
    private const int MaxInFlightBatches = 250;

    private readonly IBus _bus;
    private readonly Uri _queueAddress;
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqPublisher> _logger;
    private readonly Channel<List<LogEntry>> _queue;
    private readonly Task _pump;
    private ISendEndpoint? _endpoint;

    public RabbitMqPublisher(
        IBus bus,
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqPublisher> logger)
    {
        _bus = bus;
        _options = options.Value;
        _logger = logger;
        _queueAddress = new Uri($"queue:{_options.QueueName}");
        _queue = Channel.CreateBounded<List<LogEntry>>(new BoundedChannelOptions(MaxInFlightBatches)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });
        _pump = PumpAsync();
    }

    public ValueTask PublishAsync(IReadOnlyList<LogEntry> logs, CancellationToken cancellationToken)
    {
        if (logs.Count == 0)
            return ValueTask.CompletedTask;

        var batch = logs as List<LogEntry> ?? [.. logs];

        if (!_queue.Writer.TryWrite(batch))
            _logger.LogWarning("Publish backlog full, dropping batch of {Count} logs to avoid unbounded memory growth", batch.Count);

        return ValueTask.CompletedTask;
    }

    private async Task PumpAsync()
    {
        await foreach (var batch in _queue.Reader.ReadAllAsync(CancellationToken.None))
        {
            try
            {
                await PublishCoreAsync(batch);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Fire-and-forget publish failed for batch of {Count} logs. Batch was NOT accepted by RabbitMQ.",
                    batch.Count);
            }
        }
    }

    private async Task PublishCoreAsync(List<LogEntry> logs)
    {
        var endpoint = _endpoint ??= await _bus.GetSendEndpoint(_queueAddress);
        await endpoint.Send(
            new IngestLogsBatch { Logs = logs },
            sendContext =>
            {
                sendContext.SetAwaitAck(false);

                if (sendContext is RabbitMqSendContext rabbitMqContext)
                    rabbitMqContext.BasicProperties.DeliveryMode = DeliveryModes.Transient;
            },
            CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        _queue.Writer.TryComplete();
        await _pump;
    }
}
