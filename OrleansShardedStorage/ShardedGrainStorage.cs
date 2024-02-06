using Orleans.Runtime;
using Orleans.Storage;

namespace OrleansShardedStorage;

public class ShardedGrainStorage : IGrainStorage
{
    private readonly IGrainStorageShard[] shards;
    private readonly ILogger<ShardedGrainStorage> logger;

    public ShardedGrainStorage(
        IEnumerable<IGrainStorageShard> shards,
        ILogger<ShardedGrainStorage> logger)
    {
        this.shards = shards.ToArray();
        Array.Sort(this.shards, (x, y) => x.Index.CompareTo(y.Index));
        for (int i = 0; i < this.shards.Length; i += 1)
        {
            if (this.shards[i].Index != i)
            {
                var msg = string.Format("Shard {0} has index {1}, but it should have index {2}.", this.shards[i], this.shards[i].Index, i);
                throw new OrleansException(msg);
            }
        }

        this.logger = logger;
    }


    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        int num = FindStorageShard(grainId);
        var shard = shards[num];
        logger.LogDebug("Clearing grain {0} from shard {1}.", grainId, num);
        return shard.ClearStateAsync(stateName, grainId, grainState);
    }

    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        int num = FindStorageShard(grainId);
        var shard = shards[num];
        logger.LogDebug("Reading grain {0} from shard {1}.", grainId, num);
        return shard.ReadStateAsync(stateName, grainId, grainState);
    }

    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        int num = FindStorageShard(grainId);
        var shard = shards[num];
        logger.LogDebug("Writing grain {0} to shard {1}.", grainId, num);
        return shard.WriteStateAsync(stateName, grainId, grainState);
    }

    private int FindStorageShard(GrainId grainId)
    {
        int num = HashFunction(grainId, shards.Length);

        if (num < 0 || num >= shards.Length)
        {
            var msg = string.Format("Hash function returned out of bounds value {0}. This is an error.", num);
            throw new OrleansException(msg);
        }
        return num;
    }

    public static int HashFunction(GrainId grainId, int hashRange)
    {
        int hash = unchecked((int)grainId.GetUniformHashCode());
        int positiveHash = ((hash % hashRange) + hashRange) % hashRange;
        return positiveHash;
    }
}

public static class ShardedGrainStorageFactory
{
    public static ShardedGrainStorage Create(IServiceProvider services, object name)
    {
        var shards = services.GetKeyedServices<IGrainStorageShard>(name);
        return ActivatorUtilities.CreateInstance<ShardedGrainStorage>(services, shards);
    }
}

