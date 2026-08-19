using Microsoft.AspNetCore.Mvc;
using System.Text.Json.Serialization;

namespace LogForge.Api.Contracts.Aggregation;

public sealed class AggregateLogsRequest
{
    public string? Service { get; init; }
    public string? Level { get; init; }
    public string? Since { get; init; }
    public string? Until { get; init; }
    public string? Q { get; init; }
    public string? Bucket { get; init; }

    [FromQuery(Name = "group_by")]
    public string? GroupBy { get; init; }
}

public sealed class AggregateLogsResponse
{
    [JsonPropertyName("buckets")]
    public required List<AggregateLogBucket> Buckets { get; init; }
}

public sealed class AggregateLogBucket
{
    [JsonPropertyName("start")]
    public required DateTimeOffset Start { get; init; }

    [JsonPropertyName("group")]
    public string? Group { get; init; }

    [JsonPropertyName("count")]
    public required long Count { get; init; }
}