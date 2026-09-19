package com.chargemon.ocpp.codec.fact;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.ocpp.codec.fixtures.Frames;
import com.chargemon.ocpp.codec.mapper.MapperRegistry;
import com.chargemon.ocpp.model.Direction;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.OcppEvent;
import com.chargemon.ocpp.model.OcppVersion;
import com.chargemon.ocpp.model.station.StationContext;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class OcppEventFactTest {

    private final MapperRegistry registry = MapperRegistry.fromServiceLoader();

    private static EventMeta meta(OcppVersion v, String action) {
        return new EventMeta("ST-1", v, Direction.STATION_TO_CSMS, "u1", action, Frames.T0, null, "src");
    }

    @Test
    void resolvesTypedFieldsMetaStationAndAggregates() {
        OcppEvent e = registry.mapCall(meta(OcppVersion.V16, "StatusNotification"),
                Frames.status16(2, "Faulted", "GroundFailure", Frames.T0));
        StationContext ctx = new StationContext("ST-1", "ACME", "X", "1", Map.of("tier", "gold"), Set.of("site:1"),
                Set.of("site:1", "region:eu"), true);
        OcppEventFact fact = new OcppEventFact(e, ctx, Map.of("zeroEnergy.rolling7d", 3), Frames.T0);

        assertThat(fact.get("event.type")).contains("StatusNotification");
        assertThat(fact.get("event.status")).contains("FAULTED");
        assertThat(fact.get("event.errorCode")).contains("GroundFailure");
        assertThat(fact.get("event.connectorId")).contains(2L);
        assertThat(fact.get("event.stationId")).contains("ST-1");
        assertThat(fact.get("event.version")).contains("V16");
        assertThat(fact.get("station.vendor")).contains("ACME");
        assertThat(fact.get("station.attributes.tier")).contains("gold");
        assertThat(fact.get("station.allGroupIds")).map(Object::toString).get().asString().contains("region:eu");
        assertThat(fact.get("agg.zeroEnergy.rolling7d")).contains(3);
        assertThat(fact.get("now")).contains(Frames.T0);
        assertThat(fact.get("event.nope")).isEmpty();
    }

    @Test
    void genericEventExposesRawPayload() {
        OcppEvent e = registry.mapCall(meta(OcppVersion.V201, "NotifyChargingLimit"),
                Frames.obj("evseId", 2, "chargingLimit", Frames.obj("chargingLimitSource", "EMS", "isGridCritical", true)));
        OcppEventFact fact = new OcppEventFact(e, null, Map.of(), Frames.T0);
        assertThat(fact.get("event.payload.chargingLimit.chargingLimitSource")).contains("EMS");
        assertThat(fact.get("event.payload.chargingLimit.isGridCritical")).contains(true);
        assertThat(fact.get("station.known")).contains(false);
    }

    @Test
    void resolvesArrayIndexes() {
        OcppEvent e = registry.mapCall(meta(OcppVersion.V201, "TransactionEvent"),
                Frames.txEvent201("Ended", "tx", 1, Frames.T0, "StopAuthorized", 42L, 1));
        OcppEventFact fact = new OcppEventFact(e, null, Map.of(), Frames.T0);
        assertThat(fact.get("event.meterValues[0].sampledValues[0].value")).contains(new BigDecimal("42"));
        assertThat(fact.get("event.transactionInfo.transactionId")).contains("tx");
    }
}
