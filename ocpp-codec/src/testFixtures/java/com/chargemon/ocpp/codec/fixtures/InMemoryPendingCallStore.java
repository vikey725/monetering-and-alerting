package com.chargemon.ocpp.codec.fixtures;

import com.chargemon.ocpp.codec.correlate.PendingCall;
import com.chargemon.ocpp.codec.correlate.PendingCallStore;
import com.chargemon.ocpp.codec.correlate.PendingResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class InMemoryPendingCallStore implements PendingCallStore {

    private final Map<String, PendingCall> map = new HashMap<>();
    private final Map<String, PendingResponse> responses = new HashMap<>();

    @Override
    public Optional<PendingCall> get(String key) {
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public void put(String key, PendingCall call) {
        map.put(key, call);
    }

    @Override
    public void remove(String key) {
        map.remove(key);
    }

    @Override
    public Optional<PendingResponse> getResponse(String callKey) {
        return Optional.ofNullable(responses.get(callKey));
    }

    @Override
    public void putResponse(String callKey, PendingResponse response) {
        responses.put(callKey, response);
    }

    @Override
    public void removeResponse(String callKey) {
        responses.remove(callKey);
    }

    public int size() {
        return map.size();
    }

    public int parked() {
        return responses.size();
    }
}
