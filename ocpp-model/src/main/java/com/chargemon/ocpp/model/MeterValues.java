package com.chargemon.ocpp.model;

import java.util.List;

/** 1.6 MeterValues (2.0.1 sends periodic readings inside TransactionEvent or standalone MeterValues per EVSE). */
public record MeterValues(EventMeta meta, int evseOrConnectorId, Integer transactionId, List<MeterValue> meterValues)
        implements StationMessage {
}
