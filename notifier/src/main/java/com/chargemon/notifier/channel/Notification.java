package com.chargemon.notifier.channel;

import com.chargemon.alert.AlertEvent;

/** What gets delivered: the event plus pre-rendered text. */
public record Notification(AlertEvent event, String title, String body) {
}
