using LogForge.Infrastructure;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddInfrastructure(builder.Configuration);

var app = builder.Build();

await app.Services.ApplyDatabaseMigrationsAsync();


app.MapControllers();
app.MapHealthChecks("/health");

app.Run();