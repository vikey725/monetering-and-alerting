package com.chargemon.flink.model;

import com.chargemon.flink.serde.JsonTypeInfoFactory;
import com.chargemon.ocpp.codec.envelope.RawEnvelope;
import com.chargemon.ocpp.codec.frame.RawFrame;
import org.apache.flink.api.common.typeinfo.TypeInfo;

@TypeInfo(JsonTypeInfoFactory.class)
public record DecodedFrame(RawEnvelope envelope, RawFrame frame) {
}
