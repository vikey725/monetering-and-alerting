package com.chargemon.ocpp.model;

public record BootNotification(
        EventMeta meta,
        String vendor,
        String model,
        String serialNumber,
        String firmwareVersion,
        String reason) implements StationMessage {
}
