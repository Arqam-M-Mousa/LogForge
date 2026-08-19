using LogForge.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace LogForge.Infrastructure.Persistence;

public class LogForgeDbContext : DbContext
{
    public LogForgeDbContext(DbContextOptions<LogForgeDbContext> options) : base(options)
    {
    }

    public DbSet<Log> Logs { get; set; }
    public DbSet<LogMinuteRollup> LogMinuteRollups { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfigurationsFromAssembly(typeof(LogForgeDbContext).Assembly);
        base.OnModelCreating(modelBuilder);
    }
}