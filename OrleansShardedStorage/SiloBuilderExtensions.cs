using Orleans.Storage;

namespace OrleansShardedStorage;

public static class SiloBuilderExtensions
{
    public static ISiloBuilder AddShardedGrainStorage(this ISiloBuilder builder, string name, ReadOnlySpan<string> shardNames)
    {
        for (int i = 0; i < shardNames.Length; i += 1)
        {
            var shardIndex = i;
            var shardName = shardNames[i];
            builder.Services.AddKeyedSingleton<IGrainStorageShard>(name, (sp, _) => new GrainStorageShard(shardIndex, sp.GetRequiredKeyedService<IGrainStorage>(shardName)));
        }
        builder.Services.AddKeyedSingleton<IGrainStorage>(name, ShardedGrainStorageFactory.Create);

        return builder;
    }
}

