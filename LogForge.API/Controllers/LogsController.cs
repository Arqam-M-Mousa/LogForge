using LogForge.API.Contracts.Aggregation;
using LogForge.API.Contracts.Common;
using LogForge.API.Contracts.Ingestion;
using LogForge.API.Contracts.Query;
using LogForge.API.Validation;
using LogForge.Domain.Aggregation.Abstractions;
using LogForge.Domain.Ingestion.Abstractions;
using LogForge.Domain.Query.Abstractions;
using Microsoft.AspNetCore.Mvc;

namespace LogForge.API.Controllers;

[ApiController]
[Route("logs")]
public sealed class LogsController : ControllerBase
{
    private readonly ILogIngestionService _logIngestionService;
    private readonly ILogQueryService _logQueryService;
    private readonly ILogAggregationService _logAggregationService;

    public LogsController(
           ILogIngestionService logIngestionService,
           ILogQueryService logQueryService,
           ILogAggregationService logAggregationService)
    {
        _logIngestionService = logIngestionService;
        _logQueryService = logQueryService;
        _logAggregationService = logAggregationService;
    }

    [HttpPost]
    public async Task<IActionResult> Ingest(IngestLogsRequest request, CancellationToken cancellationToken)
    {
        if (request.Logs is not { Count: > 0 })
        {
            return BadRequest(new ApiError("logs must contain at least one entry"));
        }

        var (accepted, rejected) = LogEntryValidator.Validate(request.Logs);

        if (accepted.Count > 0)
        {
            try
            {
                await _logIngestionService.PublishAsync(accepted, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch
            {
                return StatusCode(StatusCodes.Status503ServiceUnavailable,
                    new ApiError("ingestion temporarily unavailable"));
            }
        }

        var response = new IngestLogsResponse { Accepted = accepted.Count, Rejected = rejected };
        return accepted.Count > 0 ? Ok(response) : BadRequest(response);
    }

    [HttpGet]
    public async Task<IActionResult> Query([FromQuery] QueryLogsRequest request, CancellationToken cancellationToken)
    {
        if (!QueryLogsMapper.TryParse(request, HttpContext.Request.Query, out var filter, out var error))
        {
            return BadRequest(new ApiError(error!));
        }

        var result = await _logQueryService.QueryAsync(filter, cancellationToken);
        return Ok(QueryLogsMapper.ToResponse(result));
    }

    [HttpGet("aggregate")]
    public async Task<IActionResult> Aggregate([FromQuery] AggregateLogsRequest request, CancellationToken cancellationToken)
    {
        if (!AggregateLogsMapper.TryParse(request, HttpContext.Request.Query, out var filter, out var error))
        {
            return BadRequest(new ApiError(error!));
        }

        try
        {
            var result = await _logAggregationService.AggregateAsync(filter, cancellationToken);
            return Ok(AggregateLogsMapper.ToResponse(result));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            return StatusCode(StatusCodes.Status503ServiceUnavailable,
                new ApiError("aggregation temporarily unavailable"));
        }
    }
}
