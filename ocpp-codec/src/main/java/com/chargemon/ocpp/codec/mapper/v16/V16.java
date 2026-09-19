package com.chargemon.ocpp.codec.mapper.v16;

import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.model.OcppVersion;

final class V16 {
    private V16() {
    }

    static MapperKey key(String action) {
        return new MapperKey(OcppVersion.V16, action);
    }
}
