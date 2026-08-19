using LogForge.Domain.Query.Abstractions;

namespace LogForge.Domain.Query.Services;

internal class LogQueryService : ILogQueryService
{
    public Task<LogQueryResult> QueryAsync(LogQueryFilter filter, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
