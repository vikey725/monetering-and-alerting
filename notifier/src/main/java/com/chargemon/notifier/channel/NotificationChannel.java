package com.chargemon.notifier.channel;

/**
 * One delivery mechanism (slack, pagerduty, email, webhook, ...). Implementations
 * are Spring beans; the registry picks them by {@link #type()}.
 */
public interface NotificationChannel {

    /** Matches {@code ChannelRef.type()}, lower case. */
    String type();

    /** Delivers to a target (channel name, service, address, URL). Throws {@link DeliveryException} on failure. */
    void send(Notification notification, String target);
}
