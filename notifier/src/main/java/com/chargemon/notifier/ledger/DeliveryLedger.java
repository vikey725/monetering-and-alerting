package com.chargemon.notifier.ledger;

import com.chargemon.alert.ChannelRef;

/** Idempotency + bookkeeping for deliveries (one row per alert event x channel). */
public interface DeliveryLedger {

    /** Claims the delivery; false when it was already delivered (duplicate Kafka record). */
    boolean claim(String alertEventId, ChannelRef channel);

    void markDelivered(String alertEventId, ChannelRef channel);

    void markFailed(String alertEventId, ChannelRef channel, String error, boolean willRetry);
}
