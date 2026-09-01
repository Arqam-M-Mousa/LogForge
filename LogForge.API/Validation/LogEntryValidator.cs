using LogForge.API.Contracts.Common;
using LogForge.API.Contracts.Ingestion;
using LogForge.Domain.Ingestion;
using System.Text.Json;

namespace LogForge.API.Validation;

public static class LogEntryValidator
{
    public static (List<LogEntry> Accepted, List<RejectedLog> Rejected) Validate(IReadOnlyList<LogInput> logs)
    {
        var accepted = new List<LogEntry>(logs.Count);
        var rejected = new List<RejectedLog>();
        var maximumAllowedTimestamp = DateTimeOffset.UtcNow.AddMinutes(5);

        for (var index = 0; index < logs.Count; index++)
        {
            if (TryParse(logs[index], maximumAllowedTimestamp, out var entry, out var reason))
                accepted.Add(entry!);
            else
                rejected.Add(new RejectedLog { Index = index, Reason = reason! });
        }

        return (accepted, rejected);
    }

    private static bool TryParse(
        LogInput? input,
        DateTimeOffset maximumAllowedTimestamp,
        out LogEntry? logEntry,
        out string? rejectionReason)
    {
        logEntry = null;
        rejectionReason = null;

        if (input is null)
            return Fail(out rejectionReason, "log entry is required");

        if (string.IsNullOrWhiteSpace(input.Timestamp))
            return Fail(out rejectionReason, "timestamp is required");

        if (!LogFilterParsing.TryParseTimestamp(input.Timestamp, out var timestamp))
            return Fail(out rejectionReason, "timestamp must be a valid ISO 8601 timestamp");

        if (timestamp > maximumAllowedTimestamp)
            return Fail(out rejectionReason, "timestamp must not be more than five minutes in the future");

        if (string.IsNullOrWhiteSpace(input.Level))
            return Fail(out rejectionReason, "level is required");

        if (!LogFilterParsing.IsAllowedLevel(input.Level))
            return Fail(out rejectionReason, $"invalid level: '{input.Level}'");

        if (string.IsNullOrWhiteSpace(input.Service))
            return Fail(out rejectionReason, "service is required");

        if (string.IsNullOrWhiteSpace(input.Message))
            return Fail(out rejectionReason, "message is required");

        if (!TryNormalizeAttributes(input.Attributes, out var attributes))
            return Fail(out rejectionReason, "attributes must be a flat object with string, number, or boolean values");

        logEntry = new LogEntry(timestamp, input.Level, input.Service, input.Message, attributes);
        return true;
    }

    private static bool TryNormalizeAttributes(JsonElement? attributes, out Dictionary<string, object>? result)
    {
        result = null;

        if (attributes is not { } element || element.ValueKind == JsonValueKind.Null)
            return true;

        if (element.ValueKind != JsonValueKind.Object)
            return false;

        result = new Dictionary<string, object>();

        foreach (var property in element.EnumerateObject())
        {
            if (!TryGetAttributeValue(property.Value, out var value))
                return false;

            result[property.Name] = value;
        }

        return true;
    }

    private static bool TryGetAttributeValue(JsonElement element, out string value)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.String:
                value = element.GetString()!;
                return true;
            case JsonValueKind.Number:
                value = element.GetRawText();
                return true;
            case JsonValueKind.True:
                value = "true";
                return true;
            case JsonValueKind.False:
                value = "false";
                return true;
            default:
                value = null!;
                return false;
        }
    }

    private static bool Fail(out string? reason, string message)
    {
        reason = message;
        return false;
    }
}
