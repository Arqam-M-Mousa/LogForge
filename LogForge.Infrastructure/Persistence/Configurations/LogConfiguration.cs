using LogForge.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace LogForge.Infrastructure.Persistence.Configurations;

public class LogConfiguration : IEntityTypeConfiguration<Log>
{
    public void Configure(EntityTypeBuilder<Log> builder)
    {
        builder.ToTable("log");

        builder.HasKey(x => new { x.Timestamp, x.Id });

        builder.Property(x => x.Id)
            .ValueGeneratedOnAdd();

        builder.Property(x => x.Timestamp)
            .IsRequired();

        builder.Property(x => x.Level)
            .HasMaxLength(5)
            .IsRequired();

        builder.Property(x => x.Service)
            .IsRequired();

        builder.Property(x => x.Message)
            .IsRequired();

        builder.Property(x => x.Attributes)
            .HasColumnType("jsonb")
            .HasDefaultValueSql("'{}'::jsonb")
            .IsRequired();

        builder.HasIndex(x => new { x.Service, x.Timestamp, x.Id })
            .IsDescending(false, true, true);

        builder.HasIndex(x => x.Timestamp)
            .HasMethod("brin");
    }
}
