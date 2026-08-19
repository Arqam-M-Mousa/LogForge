using System.Text.Json;
using LogForge.Api.Contracts.Common;
using LogForge.Domain.Ingestion;

namespace LogForge.Api.Contracts.Ingestion;

public static class IngestLogsMapper
{
    public static bool TryParse(
        LogInput? input,
        DateTimeOffset maximumAllowedTimestamp,
        out LogEntry? logEntry,
        out string? rejectionReason)
    {
        logEntry = null;
        rejectionReason = null;

        if (input is null)
        {
            rejectionReason = "log entry is required";
            return false;
        }

        if (string.IsNullOrWhiteSpace(input.Timestamp))
        {
            rejectionReason = "timestamp is required";
            return false;
        }

        if (!LogFilterParsing.TryParseTimestamp(input.Timestamp, out var timestamp))
        {
            rejectionReason = "timestamp must be a valid ISO 8601 timestamp";
            return false;
        }

        if (timestamp > maximumAllowedTimestamp)
        {
            rejectionReason = "timestamp must not be more than five minutes in the future";
            return false;
        }

        if (string.IsNullOrWhiteSpace(input.Level))
        {
            rejectionReason = "level is required";
            return false;
        }

        if (!LogFilterParsing.IsAllowedLevel(input.Level))
        {
            rejectionReason = $"invalid level: '{input.Level}'";
            return false;
        }

        if (string.IsNullOrWhiteSpace(input.Service))
        {
            rejectionReason = "service is required";
            return false;
        }

        if (string.IsNullOrWhiteSpace(input.Message))
        {
            rejectionReason = "message is required";
            return false;
        }

        Dictionary<string, object>? attributes = null;

        if (input.Attributes is { } attributesElement && attributesElement.ValueKind != JsonValueKind.Null)
        {
            if (attributesElement.ValueKind != JsonValueKind.Object)
            {
                rejectionReason = "attributes must be a flat object with string, number, or boolean values";
                return false;
            }

            attributes = new Dictionary<string, object>();

            foreach (var property in attributesElement.EnumerateObject())
            {
                if (!TryGetAttributeValue(property.Value, out var value))
                {
                    rejectionReason = "attributes must be a flat object with string, number, or boolean values";
                    return false;
                }

                attributes[property.Name] = value;
            }
        }

        logEntry = new LogEntry(timestamp, input.Level, input.Service, input.Message, attributes);
        return true;
    }

    private static bool TryGetAttributeValue(JsonElement element, out object value)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.String:
                value = element.GetString()!;
                return true;
            case JsonValueKind.Number:
                value = GetNumber(element);
                return true;
            case JsonValueKind.True:
                value = true;
                return true;
            case JsonValueKind.False:
                value = false;
                return true;
            default:
                value = null!;
                return false;
        }
    }

    private static object GetNumber(JsonElement value)
    {
        if (value.TryGetInt64(out var integer))
        {
            return integer;
        }

        if (value.TryGetDecimal(out var decimalValue))
        {
            return decimalValue;
        }

        return value.GetDouble();
    }
}