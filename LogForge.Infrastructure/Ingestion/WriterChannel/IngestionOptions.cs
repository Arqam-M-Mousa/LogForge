namespace LogForge.Infrastructure.Ingestion.WriterChannel;

public sealed class IngestionOptions
{
    public const string SectionName = "Ingestion";
    public int ChannelCapacity { get; init; } = 64;
    public int MaxBatchSize { get; init; } = 2_000;
    public int FlushIntervalMs { get; init; } = 10;
}