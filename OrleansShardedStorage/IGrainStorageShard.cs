using Orleans.Storage;

namespace OrleansShardedStorage;

public interface IGrainStorageShard : IGrainStorage
{
    public int Index { get; }
}
