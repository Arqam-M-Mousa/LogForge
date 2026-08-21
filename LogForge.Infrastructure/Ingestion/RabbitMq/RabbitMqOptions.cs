namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqOptions
{
    public const string SectionName = "RabbitMq";

    public string ConnectionString { get; init; } = null!;

    public string QueueName { get; init; } = null!;

    public int PublisherCount { get; init; }

    public int ConsumerCount { get; init; }

    public int ConsumerBatchSize { get; init; }

    public int ConsumerBatchFlushIntervalMs { get; init; }

    public ushort ConsumerPrefetchCount { get; init; }
}