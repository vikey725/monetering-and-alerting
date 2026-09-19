package com.chargemon.ocpp.codec.mapper.v201;

import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.model.OcppVersion;

final class V201 {
    private V201() {
    }

    static MapperKey key(String action) {
        return new MapperKey(OcppVersion.V201, action);
    }
}
