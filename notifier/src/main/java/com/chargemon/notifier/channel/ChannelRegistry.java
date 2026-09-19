package com.chargemon.notifier.channel;

import com.chargemon.alert.ChannelRef;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

@Component
public class ChannelRegistry {

    private final Map<String, NotificationChannel> byType;

    public ChannelRegistry(List<NotificationChannel> channels) {
        this.byType = channels.stream().collect(Collectors.toUnmodifiableMap(
                c -> c.type().toLowerCase(Locale.ROOT), Function.identity()));
    }

    public Optional<NotificationChannel> resolve(ChannelRef ref) {
        return Optional.ofNullable(byType.get(ref.type()));
    }

    public Map<String, NotificationChannel> all() {
        return byType;
    }
}
