package com.chargemon.ocpp.codec.mapper;

import com.chargemon.ocpp.model.OcppVersion;

public record MapperKey(OcppVersion version, String action) {
}
