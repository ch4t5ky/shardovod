package org.inviewclub.shardovod.commands;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.inviewclub.shardovod.opensearch.OpenSearch;

import com.mojang.brigadier.CommandDispatcher;
import com.mojang.brigadier.exceptions.CommandSyntaxException;

import net.minecraft.command.CommandSource;
import net.minecraft.command.Commands;
import net.minecraft.entity.Entity;
import net.minecraft.entity.passive.SheepEntity;
import net.minecraft.entity.player.ServerPlayerEntity;
import net.minecraft.item.DyeColor;
import net.minecraft.nbt.CompoundNBT;
import net.minecraft.util.math.AxisAlignedBB;
import net.minecraft.util.text.StringTextComponent;
import net.minecraft.entity.EntityType;

public class ShardovodCommands {

    public ShardovodCommands(CommandDispatcher<CommandSource> dispatcher) {
        dispatcher.register(
            Commands.literal("shardovod")
                .then(Commands.literal("sync")
                    .executes(command -> syncShards(command.getSource()))
                )
        );
    }

    private int syncShards(CommandSource source) throws CommandSyntaxException {
        ServerPlayerEntity player = source.getPlayerOrException();
        OpenSearch os = new OpenSearch();

        // Инициализация OpenSearch
        boolean initialized = os.init("localhost", 9200, "http", "admin", "admin");
        if (!initialized) {
            source.sendFailure(new StringTextComponent("OpenSearch init failed"));
            return 0;
        }

        // Получаем все шарды
        List<Map<String, Object>> shards = os.getAllShards();
        source.sendSuccess(new StringTextComponent("Found " + shards.size() + " shards in OpenSearch"), true);

        // Создаем map shardId -> данные
        Map<String, Map<String, Object>> shardMap = new HashMap<>();
        for (Map<String, Object> s : shards) {
            String key = s.get("index") + ":" + s.get("shard") + s.get("prirep");
            shardMap.put(key, s);
            source.sendSuccess(new StringTextComponent("Shard: " + key + " state=" + s.get("state")), true);
        }

        // Ищем все овцы во всём мире
        double radius = 50.0;
        AxisAlignedBB searchBox = player.getBoundingBox().inflate(radius);
        List<SheepEntity> existingSheep = player.level.getEntitiesOfClass(SheepEntity.class, searchBox,
                sheep -> sheep.getPersistentData().contains("shardId"));
        source.sendSuccess(new StringTextComponent("Found " + existingSheep.size() + " sheep in radius " + radius), true);

        Map<String, SheepEntity> sheepMap = new HashMap<>();
        for (SheepEntity sheep : existingSheep) {
            String shardKey = sheep.getPersistentData().getString("shardId");
            sheepMap.put(shardKey, sheep);
            source.sendSuccess(new StringTextComponent("Existing sheep for shardId: " + shardKey), true);
        }


        int updated = 0;
        int created = 0;
        int removed = 0;

        // Проходим по всем шардам
        for (Map.Entry<String, Map<String, Object>> entry : shardMap.entrySet()) {
            String shardKey = entry.getKey();
            Map<String, Object> shard = entry.getValue();

            String status = (String) shard.get("state");
            boolean primary = "p".equals(shard.get("prirep"));
            DyeColor color = colorByStatus(status);

            SheepEntity sheep = sheepMap.get(shardKey);
            if (sheep != null) {
                // Овца уже есть → обновляем
                sheep.setColor(color);
                sheep.setCustomNameVisible(true);
                sheep.setCustomName(new StringTextComponent(shardKey + " " + status));
                updated++;
                source.sendSuccess(new StringTextComponent("Updated sheep for shard: " + shardKey + " color=" + color), true);
            } else {
                // Овцы нет → создаем новую
                CompoundNBT compoundTag = new CompoundNBT();
                compoundTag.putString("id", "minecraft:sheep");

                Entity entity = EntityType.loadEntityRecursive(compoundTag, source.getLevel(), e -> {
                    e.moveTo(player.getX(), player.getY(), player.getZ() + 1);
                    return e;
                });

                if (entity != null && entity instanceof SheepEntity) {
                    SheepEntity newSheep = (SheepEntity) entity;
                    newSheep.setColor(color);
                    newSheep.setCustomNameVisible(true);
                    newSheep.setCustomName(new StringTextComponent(shardKey + " " + status));
                    newSheep.getPersistentData().putString("shardId", shardKey);
                    source.getLevel().addFreshEntity(newSheep);
                    created++;
                    source.sendSuccess(new StringTextComponent("Created new sheep for shard: " + shardKey + " color=" + color), true);
                } else {
                    source.sendFailure(new StringTextComponent("Failed to create sheep for shard: " + shardKey));
                }
            }
        }

        // Удаляем овец, которых больше нет в OpenSearch
        for (Map.Entry<String, SheepEntity> entry : sheepMap.entrySet()) {
            if (!shardMap.containsKey(entry.getKey())) {
                SheepEntity sheep = entry.getValue();
                sheep.die(null);
                removed++;
                source.sendSuccess(new StringTextComponent("Removed obsolete sheep with shardId: " + entry.getKey()), true);
            }
        }

        source.sendSuccess(new StringTextComponent(
                "Sync complete: updated=" + updated + ", created=" + created + ", removed=" + removed
        ), true);

        os.close();
        return 1;
    }

    private static DyeColor colorByStatus(String status) {
        if (status == null) return DyeColor.RED;
        switch (status) {
            case "STARTED":       return DyeColor.GREEN;
            case "INITIALIZING":  return DyeColor.ORANGE;
            case "RELOCATING":    return DyeColor.YELLOW;
            case "UNASSIGNED":    return DyeColor.BLACK;
            default:              return DyeColor.RED;
        }
    }
}
