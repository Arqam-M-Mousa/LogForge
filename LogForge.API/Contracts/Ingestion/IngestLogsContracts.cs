using System.Text.Json;
using System.Text.Json.Serialization;

namespace LogForge.Api.Contracts.Ingestion;

public sealed class IngestLogsRequest
{
    public List<LogInput>? Logs { get; init; } = [];
}

public sealed class LogInput
{
    [JsonConverter(typeof(LenientStringJsonConverter))]
    public string? Timestamp { get; init; }

    [JsonConverter(typeof(LenientStringJsonConverter))]
    public string? Level { get; init; }

    [JsonConverter(typeof(LenientStringJsonConverter))]
    public string? Service { get; init; }

    [JsonConverter(typeof(LenientStringJsonConverter))]
    public string? Message { get; init; }

    public JsonElement? Attributes { get; init; }
}

public sealed class IngestLogsResponse
{
    public int Accepted { get; init; }
    public List<RejectedLog> Rejected { get; init; } = [];
}

public sealed class RejectedLog
{
    public int Index { get; init; }
    public string Reason { get; init; } = string.Empty;
}