using System.Text;
using System.Text.Json;

namespace LogForge.Api.Contracts.Query;

public sealed record LogCursor(DateTimeOffset Timestamp, long Id)
{
    public string Encode()
    {
        var json = JsonSerializer.Serialize(this);
        var bytes = Encoding.UTF8.GetBytes(json);

        return Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }

    public static bool TryDecode(string? value, out LogCursor? cursor)
    {
        cursor = null;

        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        try
        {
            var base64 = value
                .Replace('-', '+')
                .Replace('_', '/');

            base64 = base64.PadRight(base64.Length + ((4 - (base64.Length % 4)) % 4), '=');

            var bytes = Convert.FromBase64String(base64);
            var json = Encoding.UTF8.GetString(bytes);
            cursor = JsonSerializer.Deserialize<LogCursor>(json);

            return cursor is { Id: > 0 } && cursor.Timestamp != default;
        }
        catch
        {
            return false;
        }
    }
}