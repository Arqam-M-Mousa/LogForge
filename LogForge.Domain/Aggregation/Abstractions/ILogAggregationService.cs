namespace LogForge.Domain.Aggregation.Abstractions;

public interface ILogAggregationService
{
    Task<LogAggregationResult> AggregateAsync(LogAggregationFilter filter, CancellationToken cancellationToken);
}