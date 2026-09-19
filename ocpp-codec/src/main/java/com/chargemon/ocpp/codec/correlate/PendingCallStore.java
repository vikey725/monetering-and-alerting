package com.chargemon.ocpp.codec.correlate;

import java.util.Optional;

/** Storage port for in-flight CALLs; the Flink operator adapts this over MapState. */
public interface PendingCallStore {

    Optional<PendingCall> get(String key);

    void put(String key, PendingCall call);

    void remove(String key);

    Optional<PendingResponse> getResponse(String callKey);

    void putResponse(String callKey, PendingResponse response);

    void removeResponse(String callKey);
}
