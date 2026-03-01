package org.inviewclub.shardovod.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.inviewclub.shardovod.opensearch.OpenSearch;

import com.mojang.brigadier.CommandDispatcher;
import com.mojang.brigadier.exceptions.CommandSyntaxException;

import net.minecraft.block.Block;
import net.minecraft.block.Blocks;
import net.minecraft.command.CommandSource;
import net.minecraft.command.Commands;
import net.minecraft.entity.EntityType;
import net.minecraft.entity.item.ArmorStandEntity;
import net.minecraft.entity.passive.SheepEntity;
import net.minecraft.entity.player.ServerPlayerEntity;
import net.minecraft.item.DyeColor;
import net.minecraft.nbt.CompoundNBT;
import net.minecraft.tileentity.SignTileEntity;
import net.minecraft.util.DamageSource;
import net.minecraft.util.math.AxisAlignedBB;
import net.minecraft.util.math.BlockPos;
import net.minecraft.util.text.StringTextComponent;
import net.minecraft.world.server.ServerWorld;

public class ShardovodCommands {

    // -------------------------------------------------------------------------
    // Layout constants
    // -------------------------------------------------------------------------

    /**
     * Walkable interior size of each node sub-zone (N×N cells).
     * The sheep grid uses (N-2)×(N-2) interior slots — one block away from
     * every edge — so ZONE_INNER=9 gives a 7×7 = 49-slot grid per zone.
     */
    private static final int ZONE_INNER    = 9;

    /**
     * Grid side length inside a zone: ZONE_INNER minus the 1-block edge gap
     * on each side.  Slots are laid out on a GRID_SIDE × GRID_SIDE grid.
     * With ZONE_INNER=9 this is 7, giving 49 slots per zone.
     */
    private static final int GRID_SIDE     = ZONE_INNER - 2; // 7

    /** Gap between adjacent sub-zones; first block is a fence divider wall. */
    private static final int ZONE_GAP      = 2;

    /** Total X-space one sub-zone occupies. */
    private static final int ZONE_SPACING  = ZONE_INNER + ZONE_GAP; // 11

    /** Empty border between outermost sub-zone edges and the outer fence. */
    private static final int GLOBAL_MARGIN = 3;

    /** How far in front of the player (+Z) the pen's SW corner is placed on first build. */
    private static final int PEN_OFFSET_Z  = 6;

    /** Search radius for managed sheep and the pen anchor. */
    private static final double SYNC_RADIUS = 300.0;

    /**
     * Maximum wool column height (blocks).  Log10 scale maps
     * 1 doc → 1 block … 100 M docs → 8 blocks.
     */
    private static final int MAX_WOOL_HEIGHT = 8;

    /** NBT boolean that marks the pen anchor ArmorStand. */
    private static final String ANCHOR_TAG       = "shardovod_pen_anchor";

    /** NBT string on the anchor: pipe-separated zone list from the last build. */
    private static final String ANCHOR_NODES_TAG = "shardovod_pen_nodes";

    // -------------------------------------------------------------------------
    // Block tables
    // -------------------------------------------------------------------------

    // Wool blocks indexed by DyeColor.ordinal():
    // WHITE ORANGE MAGENTA LIGHT_BLUE YELLOW LIME PINK GRAY
    // LIGHT_GRAY CYAN PURPLE BLUE BROWN GREEN RED BLACK
    private static final Block[] WOOL_BY_DYE = {
        Blocks.WHITE_WOOL,      Blocks.ORANGE_WOOL,     Blocks.MAGENTA_WOOL,
        Blocks.LIGHT_BLUE_WOOL, Blocks.YELLOW_WOOL,     Blocks.LIME_WOOL,
        Blocks.PINK_WOOL,       Blocks.GRAY_WOOL,       Blocks.LIGHT_GRAY_WOOL,
        Blocks.CYAN_WOOL,       Blocks.PURPLE_WOOL,     Blocks.BLUE_WOOL,
        Blocks.BROWN_WOOL,      Blocks.GREEN_WOOL,       Blocks.RED_WOOL,
        Blocks.BLACK_WOOL
    };

    private static final Block[] ZONE_FLOOR_BLOCKS = {
        Blocks.GRAY_CONCRETE,    // 0 → always UNASSIGNED
        Blocks.LIME_CONCRETE,    Blocks.CYAN_CONCRETE,   Blocks.BLUE_CONCRETE,
        Blocks.PURPLE_CONCRETE,  Blocks.MAGENTA_CONCRETE, Blocks.YELLOW_CONCRETE,
        Blocks.ORANGE_CONCRETE,  Blocks.PINK_CONCRETE,   Blocks.BROWN_CONCRETE,
        Blocks.RED_CONCRETE,     Blocks.GREEN_CONCRETE,
    };

    // -------------------------------------------------------------------------
    // Registration
    // -------------------------------------------------------------------------

    public ShardovodCommands(CommandDispatcher<CommandSource> dispatcher) {
        dispatcher.register(
            Commands.literal("shardovod")
                .then(Commands.literal("sync")
                    .executes(ctx -> syncShards(ctx.getSource()))
                )
        );
    }

    // -------------------------------------------------------------------------
    // Main sync
    // -------------------------------------------------------------------------

    private int syncShards(CommandSource source) throws CommandSyntaxException {
        ServerPlayerEntity player = source.getPlayerOrException();
        ServerWorld world = source.getLevel();

        OpenSearch os = new OpenSearch();
        if (!os.init("localhost", 9200, "http", "admin", "admin")) {
            source.sendFailure(new StringTextComponent("OpenSearch init failed"));
            return 0;
        }

        try {
            List<Map<String, Object>> rawShards = os.getAllShards();
            source.sendSuccess(new StringTextComponent(
                    "Found " + rawShards.size() + " shards in OpenSearch"), true);

            // Filter out internal indices (names starting with '.')
            Map<String, Map<String, Object>> shardMap = buildShardMap(rawShards, source);
            // FIX: collectNodeNames now skips dot-index shards so empty zones
            //      are not built for nodes that only host internal indices.
            List<String> nodeNames    = collectNodeNames(rawShards);
            List<String> currentZones = buildZoneList(nodeNames);

            // ------------------------------------------------------------------
            // 1. Find existing pen anchor, or build the pen fresh
            // ------------------------------------------------------------------
            AxisAlignedBB searchBox = player.getBoundingBox().inflate(SYNC_RADIUS);
            ArmorStandEntity anchor  = findPenAnchor(world, searchBox);

            Map<String, BlockPos> zoneSWMap;   // zone name → SW corner of zone floor
            Map<String, BlockPos> zoneCenters; // zone name → centre of zone floor

            if (anchor == null) {
                BlockPos penOrigin = player.blockPosition().offset(0, 0, PEN_OFFSET_Z);
                zoneSWMap   = buildGlobalPen(currentZones, penOrigin, world, source);
                zoneCenters = swToCenter(zoneSWMap);
                spawnPenAnchor(penOrigin, currentZones, world);
                source.sendSuccess(new StringTextComponent(
                        "Built new pen at " + penOrigin.toShortString()), true);
            } else {
                BlockPos penOrigin  = anchor.blockPosition();
                List<String> stored = readStoredZones(anchor);

                if (stored.equals(currentZones)) {
                    source.sendSuccess(new StringTextComponent(
                            "Pen up to date at " + penOrigin.toShortString()), true);
                    zoneSWMap   = computeZoneSWMap(currentZones, penOrigin);
                    zoneCenters = swToCenter(zoneSWMap);
                } else {
                    source.sendSuccess(new StringTextComponent(
                            "Node list changed — rebuilding pen"), true);
                    clearPenArea(penOrigin, stored.size(), world);
                    zoneSWMap   = buildGlobalPen(currentZones, penOrigin, world, source);
                    zoneCenters = swToCenter(zoneSWMap);
                    updateAnchorNodes(anchor, currentZones);
                }
            }

            // ------------------------------------------------------------------
            // 2. Collect existing managed sheep
            // ------------------------------------------------------------------
            Map<String, SheepEntity> sheepMap = collectExistingSheep(player, searchBox);
            source.sendSuccess(new StringTextComponent(
                    "Found " + sheepMap.size() + " managed sheep"), true);

            // Build per-zone sets of taken slot indices so new sheep can claim
            // the lowest free slot without overlapping existing ones.
            Map<String, Set<Integer>> takenSlots = new HashMap<>();
            for (String zone : currentZones) takenSlots.put(zone, new HashSet<>());

            for (SheepEntity sheep : sheepMap.values()) {
                String zone = sheep.getPersistentData().getString("shardNode");
                int    slot = sheep.getPersistentData().getInt("slotIdx");
                if (takenSlots.containsKey(zone)) takenSlots.get(zone).add(slot);
            }

            // ------------------------------------------------------------------
            // 3. Sync shards → sheep
            // ------------------------------------------------------------------
            int updated = 0, created = 0, moved = 0, removed = 0;

            for (Map.Entry<String, Map<String, Object>> entry : shardMap.entrySet()) {
                String shardKey = entry.getKey();
                Map<String, Object> shard = entry.getValue();

                String   status    = (String) shard.get("state");
                String   node      = (String) shard.get("node");
                DyeColor color     = colorByStatus(status);
                // FIX: fall back to UNASSIGNED if node name is not in zoneSWMap
                //      (race between node discovery and shard listing).
                String   zoneName  = (node != null && zoneSWMap.containsKey(node))
                                         ? node : "UNASSIGNED";
                BlockPos zoneSW    = zoneSWMap.get(zoneName);
                long     docs      = parseDocs(shard.get("docs"));
                int      newHeight = woolColumnHeight(docs);
                // FIX: primary shards → adult sheep, replicas → baby sheep.
                boolean  isPrimary = isPrimary(shardKey);

                SheepEntity sheep = sheepMap.get(shardKey);

                if (sheep != null) {
                    String prevZone   = sheep.getPersistentData().getString("shardNode");
                    int    prevHeight = sheep.getPersistentData().getInt("woolHeight");
                    boolean zoneChanged   = !zoneName.equals(prevZone);
                    boolean heightChanged = newHeight != prevHeight;

                    if (zoneChanged) {
                        // Release old slot, claim new one in new zone
                        int oldSlot = sheep.getPersistentData().getInt("slotIdx");
                        if (takenSlots.containsKey(prevZone))
                            takenSlots.get(prevZone).remove(oldSlot);

                        clearStoredColumn(sheep, world);

                        // FIX: computeIfAbsent guards against a null slot set
                        //      when zoneName was not pre-populated in takenSlots.
                        Set<Integer> slots = takenSlots.computeIfAbsent(zoneName, k -> new HashSet<>());
                        int      newSlot = nextFreeSlot(slots);
                        BlockPos spot    = slotPos(zoneSW, newSlot);
                        slots.add(newSlot);

                        placeWoolColumn(world, spot, newHeight, color);
                        storeColumnNbt(sheep, spot, newHeight);
                        sheep.getPersistentData().putInt("slotIdx", newSlot);
                        sheep.getPersistentData().putString("shardNode", zoneName);
                        sheep.teleportTo(
                                spot.getX() + 0.5,
                                spot.getY() + 1 + newHeight,
                                spot.getZ() + 0.5);
                        moved++;
                        source.sendSuccess(new StringTextComponent(
                                "Moved '" + shardKey + "'  " + prevZone + " → " + zoneName), true);

                    } else if (heightChanged) {
                        // Same zone, rebuild column in place
                        clearStoredColumn(sheep, world);
                        BlockPos spot = storedSpot(sheep);
                        placeWoolColumn(world, spot, newHeight, color);
                        storeColumnNbt(sheep, spot, newHeight);
                        sheep.teleportTo(
                                spot.getX() + 0.5,
                                spot.getY() + 1 + newHeight,
                                spot.getZ() + 0.5);
                    }

                    applySheepMeta(sheep, shardKey, status, color, docs, isPrimary);
                    updated++;

                } else {
                    // Spawn new sheep at the next free grid slot in its zone
                    SheepEntity newSheep = EntityType.SHEEP.create(world);
                    if (newSheep == null) {
                        source.sendFailure(new StringTextComponent(
                                "Failed to create sheep for: " + shardKey));
                        continue;
                    }

                    // FIX: computeIfAbsent guards against a null slot set.
                    Set<Integer> slots = takenSlots.computeIfAbsent(zoneName, k -> new HashSet<>());
                    int      slot = nextFreeSlot(slots);
                    BlockPos spot = slotPos(zoneSW, slot);
                    slots.add(slot);

                    placeWoolColumn(world, spot, newHeight, color);
                    storeColumnNbt(newSheep, spot, newHeight);
                    newSheep.getPersistentData().putInt("slotIdx",    slot);
                    newSheep.getPersistentData().putString("shardId",   shardKey);
                    newSheep.getPersistentData().putString("shardNode", zoneName);

                    newSheep.moveTo(
                            spot.getX() + 0.5,
                            spot.getY() + 1 + newHeight,
                            spot.getZ() + 0.5,
                            0f, 0f);

                    applySheepMeta(newSheep, shardKey, status, color, docs, isPrimary);
                    world.addFreshEntity(newSheep);
                    created++;

                    source.sendSuccess(new StringTextComponent(
                            "Spawned: " + shardKey + " → '" + zoneName
                            + "'  slot=" + slot + "  docs=" + docs
                            + "  col=" + newHeight
                            + (isPrimary ? "  [PRIMARY]" : "  [replica]")), true);
                }
            }

            // Remove sheep (and their columns) whose shard disappeared
            for (Map.Entry<String, SheepEntity> entry : sheepMap.entrySet()) {
                if (!shardMap.containsKey(entry.getKey())) {
                    SheepEntity sheep = entry.getValue();
                    // FIX 1: clearStoredColumn now sweeps the full MAX_WOOL_HEIGHT
                    //         range, so orphaned blocks from NBT drift are also cleared.
                    clearStoredColumn(sheep, world);
                    // FIX 2: shear before killing so the sheep drops no wool item.
                    sheep.setSheared(true);
                    sheep.hurt(DamageSource.MAGIC, Float.MAX_VALUE);
                    removed++;
                    source.sendSuccess(new StringTextComponent(
                            "Removed: " + entry.getKey()), true);
                }
            }

            source.sendSuccess(new StringTextComponent(
                    "Sync complete — updated=" + updated + ", created=" + created
                    + ", moved=" + moved + ", removed=" + removed), true);

        } finally {
            os.close();
        }
        return 1;
    }

    // -------------------------------------------------------------------------
    // Grid slot helpers
    // -------------------------------------------------------------------------

    /**
     * Converts a slot index (0-based, row-major) to a BlockPos inside the zone.
     * <p>
     * Layout inside a zone of ZONE_INNER=9:
     * <pre>
     *   fence  .  .  .  .  .  .  .  fence
     *   .      0  1  2  3  4  5  6  .        ← row 0
     *   .      7  8  9 10 11 12 13  .        ← row 1
     *   .     14 15 16 17 18 19 20  .        ← row 2
     *   …
     *   fence  .  .  .  .  .  .  .  fence
     * </pre>
     * Each slot is 1 block apart.  The 1-block margin from fence posts is
     * already baked into the offset (column = slot % GRID_SIDE + 1,
     * row = slot / GRID_SIDE + 1 inside the zone SW corner).
     */
    private static BlockPos slotPos(BlockPos zoneSW, int slot) {
        int col = slot % GRID_SIDE; // 0 … GRID_SIDE-1
        int row = slot / GRID_SIDE; // 0 … GRID_SIDE-1
        // +1 to skip the fence-post column/row at the zone edge
        return zoneSW.offset(col + 1, 0, row + 1);
    }

    /**
     * Returns the lowest non-negative integer not present in {@code taken}.
     * Guarantees every sheep in a zone occupies a unique, evenly-spaced slot.
     */
    private static int nextFreeSlot(Set<Integer> taken) {
        int slot = 0;
        while (taken != null && taken.contains(slot)) slot++;
        return slot;
    }

    // -------------------------------------------------------------------------
    // Wool column
    // -------------------------------------------------------------------------

    /**
     * Log10 scale: 1–9 docs → 1 block, 10–99 → 2, 1 000+ → 4, 100 M+ → 8.
     * 0 docs (UNASSIGNED) → 0, sheep sits directly on zone floor.
     */
    private static int woolColumnHeight(long docs) {
        if (docs <= 0) return 0;
        return Math.min(MAX_WOOL_HEIGHT, (int) Math.ceil(Math.log10(docs + 1)));
    }

    private static void placeWoolColumn(
            ServerWorld world, BlockPos base, int height, DyeColor color) {
        if (height <= 0) return;
        Block wool = WOOL_BY_DYE[color.ordinal()];
        for (int dy = 1; dy <= height; dy++) {
            world.setBlock(base.offset(0, dy, 0), wool.defaultBlockState(), 3);
        }
    }

    /**
     * FIX: Sweeps the full MAX_WOOL_HEIGHT range above the stored base position,
     * rather than only up to the stored woolHeight value.
     *
     * Previously, if a sync crashed after placing blocks but before updating
     * woolHeight in NBT, the stored height was lower than the actual column,
     * leaving orphaned wool blocks behind when the shard/node was later removed.
     * Now we always clear the maximum possible range, which is safe because
     * the zone floor occupies Y+0 and slots above MAX_WOOL_HEIGHT are always air.
     */
    private static void clearStoredColumn(SheepEntity sheep, ServerWorld world) {
        CompoundNBT data = sheep.getPersistentData();
        if (!data.contains("woolX")) return;
        BlockPos base = new BlockPos(
                data.getInt("woolX"), data.getInt("woolY"), data.getInt("woolZ"));
        for (int dy = 1; dy <= MAX_WOOL_HEIGHT; dy++) {
            world.setBlock(base.offset(0, dy, 0), Blocks.AIR.defaultBlockState(), 3);
        }
    }

    private static void storeColumnNbt(SheepEntity sheep, BlockPos base, int height) {
        CompoundNBT data = sheep.getPersistentData();
        data.putInt("woolX",      base.getX());
        data.putInt("woolY",      base.getY());
        data.putInt("woolZ",      base.getZ());
        data.putInt("woolHeight", height);
    }

    private static BlockPos storedSpot(SheepEntity sheep) {
        CompoundNBT data = sheep.getPersistentData();
        return new BlockPos(data.getInt("woolX"), data.getInt("woolY"), data.getInt("woolZ"));
    }

    // -------------------------------------------------------------------------
    // Pen anchor
    // -------------------------------------------------------------------------

    private ArmorStandEntity findPenAnchor(ServerWorld world, AxisAlignedBB box) {
        List<ArmorStandEntity> stands = world.getEntities(
                EntityType.ARMOR_STAND, box,
                e -> e.getPersistentData().getBoolean(ANCHOR_TAG));
        return stands.isEmpty() ? null : stands.get(0);
    }

    private void spawnPenAnchor(BlockPos origin, List<String> zones, ServerWorld world) {
        ArmorStandEntity anchor = EntityType.ARMOR_STAND.create(world);
        if (anchor == null) return;

        anchor.moveTo(origin.getX() + 0.5, origin.getY(), origin.getZ() + 0.5, 0f, 0f);
        anchor.setInvisible(true);
        anchor.setInvulnerable(true);
        anchor.setNoGravity(true);

        // setMarker() is package-private in Forge 1.16.5 — write via NBT
        CompoundNBT nbt = new CompoundNBT();
        anchor.saveWithoutId(nbt);
        nbt.putBoolean("Marker", true);
        anchor.load(nbt);

        anchor.setCustomNameVisible(false);
        anchor.getPersistentData().putBoolean(ANCHOR_TAG,      true);
        anchor.getPersistentData().putString(ANCHOR_NODES_TAG, String.join("|", zones));
        world.addFreshEntity(anchor);
    }

    private List<String> readStoredZones(ArmorStandEntity anchor) {
        String raw = anchor.getPersistentData().getString(ANCHOR_NODES_TAG);
        if (raw == null || raw.isEmpty()) return new ArrayList<>();
        return new ArrayList<>(Arrays.asList(raw.split("\\|")));
    }

    private void updateAnchorNodes(ArmorStandEntity anchor, List<String> zones) {
        anchor.getPersistentData().putString(ANCHOR_NODES_TAG, String.join("|", zones));
    }

    // -------------------------------------------------------------------------
    // Pen builder
    // -------------------------------------------------------------------------

    private void clearPenArea(BlockPos origin, int oldZoneCount, ServerWorld world) {
        int penInnerW = penWidth(oldZoneCount);
        int penInnerD = 2 * GLOBAL_MARGIN + ZONE_INNER;
        for (int dx = -1; dx <= penInnerW; dx++) {
            for (int dz = -1; dz <= penInnerD; dz++) {
                for (int dy = 0; dy <= MAX_WOOL_HEIGHT + 1; dy++) {
                    world.setBlock(origin.offset(dx, dy, dz),
                            Blocks.AIR.defaultBlockState(), 3);
                }
            }
        }
    }

    /**
     * Builds the global pen and returns a map of zone-name → zone SW corner.
     * SW corner is used instead of centre so grid slot arithmetic stays simple.
     */
    private Map<String, BlockPos> buildGlobalPen(
            List<String> zones, BlockPos origin,
            ServerWorld world, CommandSource source) {

        int numZones  = zones.size();
        int penInnerW = penWidth(numZones);
        int penInnerD = 2 * GLOBAL_MARGIN + ZONE_INNER;

        source.sendSuccess(new StringTextComponent(
                "Building pen  " + penInnerW + "×" + penInnerD
                + "  (" + numZones + " zones, "
                + GRID_SIDE + "×" + GRID_SIDE + " grid each)"), true);

        // Grass floor
        for (int dx = 0; dx < penInnerW; dx++) {
            for (int dz = 0; dz < penInnerD; dz++) {
                world.setBlock(origin.offset(dx, 0, dz),
                        Blocks.GRASS_BLOCK.defaultBlockState(), 3);
            }
        }

        // Outer fence ring
        for (int dx = -1; dx <= penInnerW; dx++) {
            for (int dz = -1; dz <= penInnerD; dz++) {
                if (dx == -1 || dx == penInnerW || dz == -1 || dz == penInnerD) {
                    world.setBlock(origin.offset(dx, 1, dz),
                            Blocks.OAK_FENCE.defaultBlockState(), 3);
                }
            }
        }

        // Gate centred on south wall
        world.setBlock(origin.offset(penInnerW / 2, 1, penInnerD),
                Blocks.OAK_FENCE_GATE.defaultBlockState(), 3);

        // Sub-zones
        Map<String, BlockPos> swMap = new LinkedHashMap<>();

        for (int i = 0; i < numZones; i++) {
            String   zoneName = zones.get(i);
            int      zoneX    = GLOBAL_MARGIN + i * ZONE_SPACING;
            BlockPos zoneSW   = origin.offset(zoneX, 0, GLOBAL_MARGIN);

            swMap.put(zoneName, zoneSW);

            // Coloured concrete floor
            Block floor = ZONE_FLOOR_BLOCKS[i % ZONE_FLOOR_BLOCKS.length];
            for (int dx = 0; dx < ZONE_INNER; dx++) {
                for (int dz = 0; dz < ZONE_INNER; dz++) {
                    world.setBlock(zoneSW.offset(dx, 0, dz),
                            floor.defaultBlockState(), 3);
                }
            }

            // Fence divider on east edge (full-depth column, separates zones)
            if (i < numZones - 1) {
                int dividerX = zoneX + ZONE_INNER;
                for (int dz = 0; dz < penInnerD; dz++) {
                    world.setBlock(origin.offset(dividerX, 1, dz),
                            Blocks.OAK_FENCE.defaultBlockState(), 3);
                }
            }

            // Sign on north inner edge
            int half = ZONE_INNER / 2;
            BlockPos signPos = zoneSW.offset(half, 1, 0);
            world.setBlock(signPos, Blocks.OAK_SIGN.defaultBlockState(), 3);
            SignTileEntity sign = (SignTileEntity) world.getBlockEntity(signPos);
            if (sign != null) {
                String label = zoneName.length() > 15 ? zoneName.substring(0, 15) : zoneName;
                sign.setMessage(0, new StringTextComponent(label));
                sign.setChanged();
            }

            source.sendSuccess(new StringTextComponent(
                    "  Zone [" + i + "] '" + zoneName
                    + "'  SW=" + zoneSW.toShortString()), true);
        }

        return swMap;
    }

    // -------------------------------------------------------------------------
    // Zone geometry (pure math)
    // -------------------------------------------------------------------------

    private static int penWidth(int numZones) {
        return 2 * GLOBAL_MARGIN
             + numZones * ZONE_INNER
             + (numZones - 1) * ZONE_GAP;
    }

    private Map<String, BlockPos> computeZoneSWMap(List<String> zones, BlockPos origin) {
        Map<String, BlockPos> swMap = new LinkedHashMap<>();
        for (int i = 0; i < zones.size(); i++) {
            int zoneX = GLOBAL_MARGIN + i * ZONE_SPACING;
            swMap.put(zones.get(i), origin.offset(zoneX, 0, GLOBAL_MARGIN));
        }
        return swMap;
    }

    /** Converts SW-corner map to centre map (for display / compat). */
    private Map<String, BlockPos> swToCenter(Map<String, BlockPos> swMap) {
        Map<String, BlockPos> centers = new LinkedHashMap<>();
        int half = ZONE_INNER / 2;
        for (Map.Entry<String, BlockPos> e : swMap.entrySet()) {
            centers.put(e.getKey(), e.getValue().offset(half, 0, half));
        }
        return centers;
    }

    private List<String> buildZoneList(List<String> nodeNames) {
        List<String> zones = new ArrayList<>();
        zones.add("UNASSIGNED");
        for (String n : nodeNames) {
            if (!zones.contains(n)) zones.add(n);
        }
        return zones;
    }

    // -------------------------------------------------------------------------
    // Sheep helpers
    // -------------------------------------------------------------------------

    private Map<String, SheepEntity> collectExistingSheep(
            ServerPlayerEntity player, AxisAlignedBB box) {

        List<SheepEntity> found = player.level.getEntities(
                EntityType.SHEEP, box,
                s -> s.getPersistentData().contains("shardId"));

        Map<String, SheepEntity> map = new HashMap<>();
        for (SheepEntity s : found) {
            map.put(s.getPersistentData().getString("shardId"), s);
        }
        return map;
    }

    /**
     * Applies metadata to a sheep entity to represent a shard visually.
     *
     * FIX (sheep size): Primary shards are represented as adult (full-size) sheep;
     * replicas are represented as baby (half-size) sheep.  This gives players an
     * immediate visual cue without any mixins or entity subclasses — baby sheep
     * are a vanilla feature.  Baby age is forced to -24000 ticks (20 minutes of
     * game time) on every sync to prevent them from growing up between syncs.
     * The "®" suffix on the name tag also marks replicas in text.
     */
    private static void applySheepMeta(
            SheepEntity sheep, String shardKey, String status,
            DyeColor color, long docs, boolean isPrimary) {
        sheep.setNoAi(true);
        sheep.setColor(color);
        sheep.setCustomNameVisible(true);
        sheep.setCustomName(new StringTextComponent(
                shardKey + "  " + status + "  [" + formatDocs(docs) + "]"
                + (isPrimary ? "" : " ®")));

        if (isPrimary) {
            // Ensure a sheep that switched from replica to primary is grown up.
            if (sheep.isBaby()) sheep.setAge(0);
        } else {
            // Keep replica as baby indefinitely; large negative age = ticks to adulthood.
            sheep.setBaby(true);
            sheep.setAge(-24000);
        }
    }

    // -------------------------------------------------------------------------
    // Data helpers
    // -------------------------------------------------------------------------

    /**
     * Builds the shard map, skipping any index whose name starts with '.'
     * (OpenSearch internal indices such as .kibana, .security-7, etc.).
     */
    private Map<String, Map<String, Object>> buildShardMap(
            List<Map<String, Object>> rawShards, CommandSource source) {

        Map<String, Map<String, Object>> shardMap = new LinkedHashMap<>();
        Map<String, Integer> counter = new HashMap<>();
        int skipped = 0;

        for (Map<String, Object> shard : rawShards) {
            String index = String.valueOf(shard.get("index"));

            // Skip internal OpenSearch indices
            if (index.startsWith(".")) {
                skipped++;
                continue;
            }

            String base = index + ":" + shard.get("shard") + ":" + shard.get("prirep");
            int n = counter.getOrDefault(base, 0);
            counter.put(base, n + 1);

            String key = base + ":" + n;
            shardMap.put(key, shard);
            source.sendSuccess(new StringTextComponent(
                    "Key: " + key + "  state=" + shard.get("state")
                    + "  docs=" + shard.get("docs")), true);
        }

        if (skipped > 0) {
            source.sendSuccess(new StringTextComponent(
                    "Skipped " + skipped + " internal index shards (dot-prefixed)"), true);
        }

        return shardMap;
    }

    /**
     * FIX: Skips dot-prefixed index shards when collecting node names.
     * Previously, a node that served only internal indices (e.g. .kibana)
     * would still get a zone floor built for it, leaving an empty, confusing
     * pen section.  Now only nodes that host at least one user-visible index
     * are included — consistent with what buildShardMap retains.
     */
    private List<String> collectNodeNames(List<Map<String, Object>> rawShards) {
        List<String> names = new ArrayList<>();
        for (Map<String, Object> shard : rawShards) {
            String index = String.valueOf(shard.get("index"));
            if (index.startsWith(".")) continue; // skip internal indices
            String node = (String) shard.get("node");
            if (node != null && !names.contains(node)) names.add(node);
        }
        return names;
    }

    /**
     * Determines whether a shard key represents a primary shard.
     * Key format built in buildShardMap: "index:shardNum:prirep:n"
     * e.g. "my-index:0:p:0" (primary) or "my-index:0:r:0" (replica).
     */
    private static boolean isPrimary(String shardKey) {
        String[] parts = shardKey.split(":");
        return parts.length >= 3 && "p".equals(parts[2]);
    }

    private static long parseDocs(Object raw) {
        if (raw == null) return 0;
        try { return Long.parseLong(raw.toString().trim()); }
        catch (NumberFormatException e) { return 0; }
    }

    private static String formatDocs(long docs) {
        if (docs >= 1_000_000) return String.format("%.1fM", docs / 1_000_000.0);
        if (docs >= 1_000)     return String.format("%.1fk", docs / 1_000.0);
        return String.valueOf(docs);
    }

    private static DyeColor colorByStatus(String status) {
        if (status == null) return DyeColor.RED;
        switch (status) {
            case "STARTED":      return DyeColor.GREEN;
            case "INITIALIZING": return DyeColor.ORANGE;
            case "RELOCATING":   return DyeColor.YELLOW;
            case "UNASSIGNED":   return DyeColor.BLACK;
            default:             return DyeColor.RED;
        }
    }
}