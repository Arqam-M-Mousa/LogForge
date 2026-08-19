using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace LogForge.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class InitialSchema : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "log",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    Timestamp = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    Level = table.Column<string>(type: "character varying(5)", maxLength: 5, nullable: false),
                    Service = table.Column<string>(type: "text", nullable: false),
                    Message = table.Column<string>(type: "text", nullable: false),
                    Attributes = table.Column<Dictionary<string, object>>(type: "jsonb", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_log", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "log_minute_rollup",
                columns: table => new
                {
                    BucketStart = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    Service = table.Column<string>(type: "text", nullable: false),
                    Level = table.Column<string>(type: "character varying(5)", maxLength: 5, nullable: false),
                    LogCount = table.Column<long>(type: "bigint", nullable: false, defaultValue: 0L)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_log_minute_rollup", x => new { x.BucketStart, x.Service, x.Level });
                });

            migrationBuilder.CreateIndex(
                name: "IX_log_Service_Timestamp_Id",
                table: "log",
                columns: new[] { "Service", "Timestamp", "Id" },
                descending: new[] { false, true, true });

            migrationBuilder.CreateIndex(
                name: "IX_log_Timestamp_Id",
                table: "log",
                columns: new[] { "Timestamp", "Id" },
                descending: new bool[0]);

            migrationBuilder.CreateIndex(
                name: "ix_log_minute_rollup_bucket",
                table: "log_minute_rollup",
                column: "BucketStart");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "log");

            migrationBuilder.DropTable(
                name: "log_minute_rollup");
        }
    }
}
