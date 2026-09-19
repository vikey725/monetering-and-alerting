package com.chargemon.ocpp.model;

/** A single OCPP CALL that maps 1:1 to a canonical event without needing the response. */
public sealed interface StationMessage extends OcppEvent
        permits BootNotification, Heartbeat, StatusNotification, NotifyEvent, SecurityEventNotification,
                StartTransaction, StopTransaction, TransactionEvent, MeterValues, Authorize {
}
