package org.inviewclub.events;

import org.inviewclub.shardovod.Shardovod;
import org.inviewclub.shardovod.commands.ShardovodCommands;

import net.minecraftforge.event.RegisterCommandsEvent;
import net.minecraftforge.eventbus.api.SubscribeEvent;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.server.command.ConfigCommand;

@Mod.EventBusSubscriber(modid = Shardovod.MOD_ID)
public class ModEvents {

    @SubscribeEvent
    public static void onCommandsRegister(RegisterCommandsEvent event) {
        new ShardovodCommands(event.getDispatcher());

        ConfigCommand.register(event.getDispatcher());
    }
}
