namespace LogForge.Domain.Entities;

public class Log
{
    public long Id { get; set; }
    public DateTimeOffset Timestamp { get; set; }
    public string Level { get; set; } = null!;
    public string Service { get; set; } = null!;
    public string Message { get; set; } = null!;
    public Dictionary<string, object> Attributes { get; set; } = [];
}
