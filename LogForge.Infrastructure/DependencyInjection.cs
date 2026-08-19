using LogForge.Domain.Aggregation.Abstractions;
using LogForge.Domain.Ingestion.Abstractions;
using LogForge.Domain.Query.Abstractions;
using LogForge.Infrastructure.Aggregation;
using LogForge.Infrastructure.Aggregation.Cache;
using LogForge.Infrastructure.Ingestion;
using LogForge.Infrastructure.Ingestion.WriterChannel;
using LogForge.Infrastructure.Persistence;
using LogForge.Infrastructure.Query;
using LogForge.Infrastructure.Retention;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace LogForge.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddInfrastructure(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.AddPersistence(configuration);
        services.AddLogQuery();
        services.AddLogIngestion(configuration);
        services.AddLogAggregation(configuration);
        services.AddLogRetention(configuration);
        services.AddHealthChecks().AddDbContextCheck<LogForgeDbContext>();

        return services;
    }

    public static async Task ApplyDatabaseMigrationsAsync(this IServiceProvider serviceProvider)
    {
        using var scope = serviceProvider.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<LogForgeDbContext>();
        await dbContext.Database.MigrateAsync();
    }

    private static IServiceCollection AddPersistence(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("Connection string 'DefaultConnection' is missing.");

        services.AddDbContext<LogForgeDbContext>(options => options.UseNpgsql(connectionString));

        var dataSourceBuilder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            MaxPoolSize = 50,
            MinPoolSize = 2
        };

        services.AddSingleton(new NpgsqlDataSourceBuilder(dataSourceBuilder.ConnectionString).Build());

        return services;
    }

    private static IServiceCollection AddLogQuery(this IServiceCollection services)
    {
        services.AddScoped<ILogQueryService, NpgsqlLogQueryService>();
        return services;
    }

    private static IServiceCollection AddLogIngestion(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<IngestionOptions>(options =>
            configuration.GetSection(IngestionOptions.SectionName).Bind(options));

        services.AddSingleton<LogIngestionChannel>();
        services.AddSingleton<NpgsqlLogBulkWriter>();
        services.AddSingleton<ILogIngestionService, ChannelLogIngestionService>();
        services.AddHostedService<LogBatchWriterService>();

        return services;
    }

    private static IServiceCollection AddLogAggregation(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<AggregationCacheOptions>(options =>
            configuration.GetSection(AggregationCacheOptions.SectionName).Bind(options));

        var cacheOptions = configuration
            .GetSection(AggregationCacheOptions.SectionName)
            .Get<AggregationCacheOptions>() ?? new AggregationCacheOptions();

        services.AddMemoryCache(options =>
            options.SizeLimit = Math.Max(1, cacheOptions.MaxEntries));

        services.AddSingleton<AggregateResultCache>();
        services.AddScoped<ILogAggregationService, RollupLogAggregationService>();

        return services;
    }

    private static IServiceCollection AddLogRetention(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<RetentionOptions>(options =>
            configuration.GetSection(RetentionOptions.SectionName).Bind(options));

        services.AddHostedService<LogRetentionService>();

        return services;
    }
}