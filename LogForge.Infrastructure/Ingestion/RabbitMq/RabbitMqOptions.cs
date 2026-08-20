namespace LogForge.Infrastructure.Ingestion.RabbitMq;

public sealed class RabbitMqOptions
{
    public const string SectionName = "RabbitMq";
    public string ConnectionString { get; init; } = null!;
    public string QueueName { get; init; } = null!;
    public ushort PrefetchCount { get; init; }
    public int ConsumerCount { get; set; }
    public int ConsumerBatchSize { get; init; }
    public int ConsumerBatchWaitMs { get; init; }
}
