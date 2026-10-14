Add support for **per-namespace KV sharding** so a namespace can start small and scale out later as load grows.

Today Celld effectively uses `SHARDS = 1`, meaning an entire KV namespace lives in a single cell with one owner/writer. The code already includes shard-aware cell names and a stable `shard_of(key)` function, so the next step is to replace the global shard count with namespace metadata that tracks the active shard count and routing generation.

Implement online resharding, for example allowing a namespace to grow from 1 shard to 16 shards. Existing keys should be redistributed into the new shard cells according to the existing shard hash. During migration, use temporary dual-write and read-fallback/dual-read behavior so the namespace remains available and no keys become inaccessible while data is moving.

Once all keys have migrated, atomically switch the namespace to the new routing generation and retire the old shard layout.

Each shard should remain a normal Celld cell with its own ownership, lease/fencing, SQLite database and LTX replication. This preserves the current durability and failover model while allowing a single KV namespace to scale horizontally across multiple cells and nodes.

`get`, `put`, `delete`, bulk operations and `list()` will need to become shard-aware. In particular, `list()` will need to query multiple shards and merge the ordered results.

Benchmark this before considering a SQLite replacement. The aim is to determine whether namespace-level sharding removes the primary KV scalability bottleneck while keeping the existing Celld storage architecture intact.