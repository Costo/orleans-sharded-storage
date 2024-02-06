using Orleans.Runtime;

namespace OrleansShardedStorage;

public sealed class UrlShortenerGrain(
    [PersistentState(
        stateName: "url",
        storageName: "sharded-grain-storage")]
        IPersistentState<UrlDetails> state)
    : Grain, IUrlShortenerGrain
{
    public async Task SetUrl(string fullUrl)
    {
        state.State = new()
        {
            ShortenedRouteSegment = this.GetPrimaryKeyString(),
            FullUrl = fullUrl
        };

        await state.WriteStateAsync();
    }

    public Task<string> GetUrl() =>
        Task.FromResult(state.State.FullUrl);
}

[GenerateSerializer, Alias(nameof(UrlDetails))]
public sealed record class UrlDetails
{
    [Id(0)]
    public string FullUrl { get; set; } = "";

    [Id(1)]
    public string ShortenedRouteSegment { get; set; } = "";
}

public interface IUrlShortenerGrain : IGrainWithStringKey
{
    Task SetUrl(string fullUrl);

    Task<string> GetUrl();
}