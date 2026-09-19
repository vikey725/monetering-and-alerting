package com.chargemon.alert;

import java.util.Locale;
import java.util.Objects;

/** Notification target such as {@code slack:#ops}, {@code pagerduty:svc-charging}, {@code webhook:https://...}. */
public record ChannelRef(String type, String target) {

    public ChannelRef {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(target, "target");
        type = type.toLowerCase(Locale.ROOT);
    }

    public static ChannelRef parse(String s) {
        if (s == null) {
            throw new IllegalArgumentException("channel ref is null");
        }
        int i = s.indexOf(':');
        if (i <= 0 || i == s.length() - 1) {
            throw new IllegalArgumentException("channel ref must be <type>:<target>, got '" + s + "'");
        }
        return new ChannelRef(s.substring(0, i).trim(), s.substring(i + 1).trim());
    }

    @Override
    public String toString() {
        return type + ":" + target;
    }
}
