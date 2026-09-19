package com.chargemon.ocpp.model;

public record Authorize(EventMeta meta, String idToken, String idTokenType) implements StationMessage {
}
