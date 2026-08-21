using LogForge.Domain.Aggregation.Abstractions;
using LogForge.Domain.Ingestion;
using LogForge.Domain.Ingestion.Abstractions;
using LogForge.Domain.Query.Abstractions;
using LogForge.Infrastructure.Aggregation;
using LogForge.Infrastructure.Aggregation.Cache;
using LogForge.Infrastructure.Ingestion;
using LogForge.Infrastructure.Ingestion.RabbitMq;
using LogForge.Infrastructure.Persistence;
using LogForge.Infrastructure.Query;
using LogForge.Infrastructure.Retention;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
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
            MaxPoolSize = 20,
            MinPoolSize = 4
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
        services.Configure<RabbitMqOptions>(options =>
            configuration.GetSection(RabbitMqOptions.SectionName).Bind(options));

        var ingestionConnectionString = configuration.GetConnectionString("IngestionConnection")
            ?? configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("Ingestion connection string is missing.");

        var ingestionPoolBuilder = new NpgsqlConnectionStringBuilder(ingestionConnectionString)
        {
            MaxPoolSize = 20,
            MinPoolSize = 5,
            Timeout = 15,
            CommandTimeout = 30
        };

        var ingestionDataSource = new NpgsqlDataSourceBuilder(ingestionPoolBuilder.ConnectionString).Build();

        services.AddSingleton<NpgsqlLogBulkWriter>(_ => new NpgsqlLogBulkWriter(ingestionDataSource));
        services.AddSingleton<ILogIngestionService, RabbitMqPublisher>();

        services.AddMassTransit(x =>
        {
            x.AddConsumer<RabbitMqConsumer>();

            x.UsingRabbitMq((context, cfg) =>
            {
                var options = context.GetRequiredService<IOptions<RabbitMqOptions>>().Value;

                cfg.Host(options.ConnectionString);

                cfg.ReceiveEndpoint(options.QueueName, e =>
                {
                    e.PrefetchCount = options.ConsumerPrefetchCount;
                    e.ConcurrentMessageLimit = options.ConsumerCount;

                    e.Batch<IngestLogsBatch>(b =>
                    {
                        b.MessageLimit = options.ConsumerBatchSize;
                        b.TimeLimit = TimeSpan.FromMilliseconds(options.ConsumerBatchFlushIntervalMs);
                        b.Consumer<RabbitMqConsumer, IngestLogsBatch>(context);
                    });
                });
            });
        });

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