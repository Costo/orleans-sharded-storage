using Orleans.Runtime;
using Orleans.Storage;

namespace OrleansShardedStorage;

public class GrainStorageShard : IGrainStorageShard
{
    private readonly IGrainStorage grainStorage;

    public GrainStorageShard(int index, IGrainStorage grainStorage)
    {
        Index = index;
        this.grainStorage = grainStorage;
    }

    public int Index { get; }

    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        => grainStorage.ClearStateAsync(stateName, grainId, grainState);

    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        => grainStorage.ReadStateAsync(stateName, grainId, grainState);

    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        => grainStorage.WriteStateAsync(stateName, grainId, grainState);
}
