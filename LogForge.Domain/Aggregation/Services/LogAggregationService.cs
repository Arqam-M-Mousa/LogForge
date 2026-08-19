using LogForge.Domain.Aggregation.Abstractions;

namespace LogForge.Domain.Aggregation.Services;

internal class LogAggregationService : ILogAggregationService
{
    public Task<LogAggregationResult> AggregateAsync(LogAggregationFilter filter, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
