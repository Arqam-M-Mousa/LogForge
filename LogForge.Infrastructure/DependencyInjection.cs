using LogForge.Domain.Aggregation.Abstractions;
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
            MaxPoolSize = 12,
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

        var rabbitMqOptions = configuration
            .GetSection(RabbitMqOptions.SectionName)
            .Get<RabbitMqOptions>()
            ?? throw new InvalidOperationException("RabbitMq configuration is missing.");

        services.AddSingleton<NpgsqlLogBulkWriter>();
        services.AddSingleton<RabbitMqIngestionConsumer>();
        services.AddSingleton<ILogIngestionService, RabbitMqIngestionPublisher>();

        var consumerCount = Math.Max(1, rabbitMqOptions.ConsumerCount);
        var prefetchCount = Math.Max(consumerCount, (int)rabbitMqOptions.ConsumerPrefetchCount);

        services.AddMassTransit(x =>
        {
            x.AddConsumer<RabbitMqIngestionConsumer>(cfg =>
            {
                cfg.ConcurrentMessageLimit = consumerCount;
            });

            x.UsingRabbitMq((context, cfg) =>
            {
                cfg.Host(new Uri(rabbitMqOptions.ConnectionString), h =>
                {
                    h.PublisherConfirmation = true;
                    h.RequestedChannelMax(32);
                });

                cfg.UseRawJsonSerializer();

                cfg.ReceiveEndpoint(rabbitMqOptions.QueueName, e =>
                {
                    e.PrefetchCount = prefetchCount;
                    e.ConcurrentMessageLimit = consumerCount;
                    e.ConfigureConsumer<RabbitMqIngestionConsumer>(context);
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