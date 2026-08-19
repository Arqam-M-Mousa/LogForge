namespace LogForge.Domain.Query.Abstractions;

public interface ILogQueryService
{
    Task<LogQueryResult> QueryAsync(LogQueryFilter filter, CancellationToken cancellationToken);
}
