# 🐑 Shardovod

> Visualize your OpenSearch cluster as a Minecraft sheep farm.

Shardovod connects to an OpenSearch cluster and reflects its state in a
Minecraft world in real time — each **data node** becomes a fenced pen,
each **shard** becomes a sheep inside that pen.

---

## How it works

| OpenSearch concept | Minecraft object |
|---|---|
| Data node | Fenced pen with a name sign |
| Shard | Sheep inside the pen |
| Shard state / primary | Sheep wool color |
| Node goes offline | Pen walls turn red stained glass |
| Shard relocates | Sheep teleports to the new pen |
| Unassigned shard | Sheep roams outside all pens |

### Sheep colors

| State        | Primary | Color  | Wool ID |
| ------------ | ------- | ------ | ------- |
| STARTED      | ✅       | White  | 0       |
| STARTED      | ❌       | Lime   | 5       |
| INITIALIZING | any     | Orange | 1       |
| RELOCATING   | any     | Yellow | 4       |
| UNASSIGNED   | any     | Black  | 15      |
| unknown      | any     | Red    | 14      |

---

## Architecture

```
OpenSearch CAT API
       │  poll every N seconds
       ▼
    Syncer
    ├── reconcileNodes → BuildPen / SetPenOffline
    └── reconcileShards → SpawnSheep / MoveSheep / UpdateSheepColor
       │
       ▼
  Commander (RCON queue, 40 cmd/s)
       │
       ▼
  Minecraft 1.16.5 server
```

- **Syncer** — diffing engine; compares previous vs current cluster state and emits events
- **Commander** — rate-limited RCON command queue with auto-reconnect
- **Mapping** — bidirectional index: `nodeID ↔ penID`, `shardID ↔ sheepID`

---

## Configuration

| Env variable | Description |
|---|---|
| `OPENSEARCH_ADDR` | OpenSearch HTTP address |
| `OPENSEARCH_USER` | Username |
| `OPENSEARCH_PASS` | Password |
| `MC_RCON_ADDR` | Minecraft RCON address |
| `MC_RCON_PASS` | RCON password |
| `ORIGIN_X/Y/Z` | World coordinates of the first pen center |
| `POLL_INTERVAL` | How often to poll OpenSearch (e.g. `5s`) |

---

## Getting started

```bash
git clone https://github.com/ch4t5ky/shardovod
cd shardovod
cp .env.example .env   # fill in your values
go run ./cmd/shardovod
```

Requires:
- Go 1.21+
- Minecraft Java Edition 1.16.5 server with RCON enabled
- OpenSearch cluster

---

## Pen layout

Pens are placed along the **Z axis** from the origin point:

```
origin (center of pen 0)
│
├── [pen-opensearch-hot]    Z = origin.Z
├── [pen-opensearch-warm-2] Z = origin.Z + 8
└── ...                     step = PenDepth(7) + Gap(1) = 8
```

Each pen is `10 × 7` blocks. A name sign is placed 1 block outside
the left wall, facing outward.

---

## License

MIT
