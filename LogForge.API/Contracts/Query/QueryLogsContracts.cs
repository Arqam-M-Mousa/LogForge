using System.Text.Json.Serialization;

namespace LogForge.Api.Contracts.Query;

public sealed class QueryLogsRequest
{
    public string? Service { get; init; }
    public string? Level { get; init; }
    public string? Since { get; init; }
    public string? Until { get; init; }
    public string? Q { get; init; }
    public string? Limit { get; init; }
    public string? Cursor { get; init; }
}

public sealed class QueryLogsResponse
{
    [JsonPropertyName("logs")]
    public required List<QueryLogItem> Logs { get; init; }

    [JsonPropertyName("next_cursor")]
    public string? NextCursor { get; init; }
}

public sealed class QueryLogItem
{
    [JsonPropertyName("id")]
    public required string Id { get; init; }

    [JsonPropertyName("timestamp")]
    public required DateTimeOffset Timestamp { get; init; }

    [JsonPropertyName("level")]
    public required string Level { get; init; }

    [JsonPropertyName("service")]
    public required string Service { get; init; }

    [JsonPropertyName("message")]
    public required string Message { get; init; }

    [JsonPropertyName("attributes")]
    public Dictionary<string, object>? Attributes { get; init; }
}