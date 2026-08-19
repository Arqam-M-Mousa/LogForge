namespace LogForge.Api.Contracts.Common;

public static class LogFilterParsing
{
    private const string AttributePrefix = "attr.";
    private static readonly HashSet<string> AllowedLevels = ["debug", "info", "warn", "error"];

    public static bool IsAllowedLevel(string? level) =>
        !string.IsNullOrWhiteSpace(level) && AllowedLevels.Contains(level);

    public static bool TryParseTimestamp(string? value, out DateTimeOffset timestamp)
    {
        timestamp = default;

        if (string.IsNullOrWhiteSpace(value) || !value.Contains('T', StringComparison.Ordinal))
        {
            return false;
        }

        if (!DateTimeOffset.TryParse(value, out timestamp))
        {
            return false;
        }

        timestamp = timestamp.ToUniversalTime();
        return true;
    }

    public static IReadOnlyDictionary<string, string> ParseAttributeFilters(IQueryCollection query)
    {
        var filters = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var parameter in query)
        {
            if (parameter.Key.StartsWith(AttributePrefix, StringComparison.Ordinal))
            {
                filters[parameter.Key[AttributePrefix.Length..]] = parameter.Value.ToString();
            }
        }

        return filters;
    }
}