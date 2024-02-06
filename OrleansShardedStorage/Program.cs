using OrleansShardedStorage;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseOrleans(static siloBuilder =>
{
    siloBuilder.UseLocalhostClustering();
    siloBuilder.AddMemoryGrainStorage("shard-0");
    siloBuilder.AddMemoryGrainStorage("shard-1");
    siloBuilder.AddMemoryGrainStorage("shard-2");
    siloBuilder.AddMemoryGrainStorage("shard-3");
    siloBuilder.AddMemoryGrainStorage("shard-4");
    siloBuilder.AddMemoryGrainStorage("shard-5");
    siloBuilder.AddMemoryGrainStorage("shard-6");
    siloBuilder.AddMemoryGrainStorage("shard-7");
    siloBuilder.AddMemoryGrainStorage("shard-8");
    siloBuilder.AddMemoryGrainStorage("shard-9");

    ReadOnlySpan<string> shards = [ "shard-0", "shard-1", "shard-2", "shard-3", "shard-4", "shard-5", "shard-6", "shard-7", "shard-8", "shard-9"];

    siloBuilder.AddShardedGrainStorage("sharded-grain-storage", shards);
});

using var app = builder.Build();

app.MapGet("/", static () => "Welcome to the URL shortener, powered by Orleans!");

app.MapGet("/shorten",
    static async (IGrainFactory grains, HttpRequest request, string url) =>
    {
        var host = $"{request.Scheme}://{request.Host.Value}";

        // Validate the URL query string.
        if (string.IsNullOrWhiteSpace(url) &&
            Uri.IsWellFormedUriString(url, UriKind.Absolute) is false)
        {
            return Results.BadRequest($"""
                The URL query string is required and needs to be well formed.
                Consider, ${host}/shorten?url=https://www.microsoft.com.
                """);
        }

        // Create a unique, short ID
        var shortenedRouteSegment = Guid.NewGuid().GetHashCode().ToString("X");

        // Create and persist a grain with the shortened ID and full URL
        var shortenerGrain =
            grains.GetGrain<IUrlShortenerGrain>(shortenedRouteSegment);

        await shortenerGrain.SetUrl(url);

        // Return the shortened URL for later use
        var resultBuilder = new UriBuilder(host)
        {
            Path = $"/go/{shortenedRouteSegment}"
        };

        return Results.Ok(resultBuilder.Uri);
    });

app.MapGet("/go/{shortenedRouteSegment:required}",
    static async (IGrainFactory grains, string shortenedRouteSegment) =>
    {
        // Retrieve the grain using the shortened ID and url to the original URL
        var shortenerGrain =
            grains.GetGrain<IUrlShortenerGrain>(shortenedRouteSegment);

        var url = await shortenerGrain.GetUrl();

        // Handles missing schemes, defaults to "http://".
        var redirectBuilder = new UriBuilder(url);

        return Results.Redirect(redirectBuilder.Uri.ToString());
    });

app.Run();