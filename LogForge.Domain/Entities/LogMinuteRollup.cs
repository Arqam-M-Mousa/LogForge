namespace LogForge.Domain.Entities;

public class LogMinuteRollup
{
    public DateTimeOffset BucketStart { get; set; }
    public string Service { get; set; } = null!;
    public string Level { get; set; } = null!;
    public long LogCount { get; set; }
}