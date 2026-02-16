# Shardovod

**Shardovod** is a Minecraft mod that visualizes OpenSearch shard states in-game using sheep.  
Each sheep represents a shard, and its color indicates the shard’s current status:

| Shard Status   | Sheep Color |
|----------------|-------------|
| STARTED        | 🟢 Green    |
| INITIALIZING   | 🟠 Orange   |
| RELOCATING     | 🟡 Yellow   |
| UNASSIGNED     | ⚫ Black    |
| Other / Unknown| 🔴 Red     |

---

## 📦 Installation

1. Build the mod from source or download the pre-built JAR.  
2. Place the JAR into your `mods/` folder of your Forge-enabled Minecraft installation.  
3. Launch the game locally — the mod works with the integrated server.

---

## ⚙️ Commands

- `/shardovod sync` — synchronizes sheep with the current OpenSearch shards within a 50-block radius:  
  - Updates existing sheep colors and names to match shard status.  
  - Spawns new sheep for missing shards.  
  - Kills sheep representing shards that no longer exist.

---

## 📝 Example Usage

```text
/shardovod sync
Expected chat output:
Found 5 shards in OpenSearch
Shard: index1:0p state=STARTED
Existing sheep for shardId: index1:0p
Created new sheep for shard: index1:1r color=YELLOW
Sheep died (obsolete) with shardId: index2:0p
Sync complete: updated=3, created=2, removed=1
```