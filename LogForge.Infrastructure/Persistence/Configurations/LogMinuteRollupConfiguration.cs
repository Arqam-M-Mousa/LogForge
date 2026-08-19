using LogForge.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace LogForge.Infrastructure.Persistence.Configurations;

public class LogMinuteRollupConfiguration : IEntityTypeConfiguration<LogMinuteRollup>
{
    public void Configure(EntityTypeBuilder<LogMinuteRollup> builder)
    {
        builder.ToTable("log_minute_rollup");

        builder.HasKey(x => new { x.BucketStart, x.Service, x.Level });

        builder.Property(x => x.BucketStart)
            .IsRequired();

        builder.Property(x => x.Service)
            .IsRequired();

        builder.Property(x => x.Level)
            .HasMaxLength(5)
            .IsRequired();

        builder.Property(x => x.LogCount)
            .IsRequired()
            .HasDefaultValue(0L);

        builder.HasIndex(x => x.BucketStart)
            .HasDatabaseName("ix_log_minute_rollup_bucket");
    }
}