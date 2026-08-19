namespace LogForge.Infrastructure.Retention;

public sealed class RetentionOptions
{
    public const string SectionName = "Retention";
    public int RetentionDays { get; init; } = 30;
    public TimeSpan RunAtUtc { get; init; } = new(2, 0, 0);
    public int DeleteBatchSize { get; init; } = 10_000;
}