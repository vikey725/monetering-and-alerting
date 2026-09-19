package com.chargemon.ocpp.codec.envelope;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.common.result.Result;
import com.chargemon.ocpp.codec.fixtures.Frames;
import com.chargemon.ocpp.codec.frame.FrameParser;
import com.chargemon.ocpp.codec.frame.RawFrame;
import com.chargemon.ocpp.model.Direction;
import com.chargemon.ocpp.model.OcppVersion;
import org.junit.jupiter.api.Test;

class EnvelopeAndFrameParserTest {

    private final EnvelopeParser envelopes = new EnvelopeParser();
    private final FrameParser frames = new FrameParser();

    @Test
    void parsesCanonicalEnvelope() {
        String json = Frames.envelopeJson(Frames.fromStation("ST-1", OcppVersion.V201, Frames.T0,
                Frames.call("42", "Heartbeat", Frames.heartbeat())));
        Result<RawEnvelope, String> r = envelopes.parse(json, "t-0-1");
        assertThat(r.isOk()).isTrue();
        RawEnvelope e = ((Result.Ok<RawEnvelope, String>) r).value();
        assertThat(e.stationId()).isEqualTo("ST-1");
        assertThat(e.ocppVersion()).isEqualTo(OcppVersion.V201);
        assertThat(e.direction()).isEqualTo(Direction.STATION_TO_CSMS);
        assertThat(e.receivedAt()).isEqualTo(Frames.T0);
        RawFrame f = ((Result.Ok<RawFrame, String>) frames.parse(e.frame())).value();
        assertThat(f).isInstanceOf(RawFrame.Call.class);
        assertThat(((RawFrame.Call) f).action()).isEqualTo("Heartbeat");
    }

    @Test
    void acceptsAliasFieldNamesAndStringFrame() {
        String json = "{\"chargePointId\":\"CP-9\",\"protocol\":\"ocpp1.6\",\"dir\":\"inbound\",\"timestamp\":1735689600000,"
                + "\"frame\":\"[3,\\\"7\\\",{\\\"status\\\":\\\"Accepted\\\"}]\"}";
        Result<RawEnvelope, String> r = envelopes.parse(json, "x");
        assertThat(r.isOk()).as(r.toString()).isTrue();
        RawEnvelope e = ((Result.Ok<RawEnvelope, String>) r).value();
        assertThat(e.ocppVersion()).isEqualTo(OcppVersion.V16);
        assertThat(frames.parse(e.frame()).isOk()).isTrue();
    }

    @Test
    void rejectsMalformed() {
        assertThat(envelopes.parse("not json", "x").isOk()).isFalse();
        assertThat(envelopes.parse("{\"stationId\":\"a\"}", "x").isOk()).isFalse();
        assertThat(frames.parse(Frames.JSON.createArrayNode().add(9).add("1").add("x")).isOk()).isFalse();
        assertThat(frames.parse(Frames.JSON.createArrayNode().add(2).add("1")).isOk()).isFalse();
    }

    @Test
    void parsesCallError() {
        RawFrame f = ((Result.Ok<RawFrame, String>) frames.parse(Frames.callError("1", "NotImplemented", "nope"))).value();
        assertThat(f).isInstanceOf(RawFrame.CallError.class);
        assertThat(((RawFrame.CallError) f).errorCode()).isEqualTo("NotImplemented");
    }
}
