package com.chargemon.ocpp.model;

/** Events synthesized by joining a CALL with its CALLRESULT / CALLERROR, or by a timeout. */
public sealed interface CorrelatedEvent extends OcppEvent
        permits BootCompleted, SessionStarted, AuthorizationResult, CallFailed, CallTimedOut {
}
