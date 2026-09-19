package com.chargemon.ocpp.codec.mapper;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.ocpp.codec.fixtures.Frames;
import com.chargemon.ocpp.model.ConnectorStatus;
import com.chargemon.ocpp.model.Direction;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.GenericOcppEvent;
import com.chargemon.ocpp.model.MeterValue;
import com.chargemon.ocpp.model.OcppEvent;
import com.chargemon.ocpp.model.OcppVersion;
import com.chargemon.ocpp.model.SecurityEventNotification;
import com.chargemon.ocpp.model.StatusNotification;
import com.chargemon.ocpp.model.StopTransaction;
import com.chargemon.ocpp.model.TransactionEvent;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class MapperRegistryTest {

    private final MapperRegistry registry = MapperRegistry.fromServiceLoader();

    private static EventMeta meta(OcppVersion v, String action) {
        return new EventMeta("ST-1", v, Direction.STATION_TO_CSMS, "u1", action, Frames.T0, null, "src");
    }

    @Test
    void serviceLoaderFindsAllMvpMappers() {
        assertThat(registry.callKeys()).hasSize(15);
        assertThat(registry.correlatedKeys()).hasSize(5);
    }

    @Test
    void mapsStatusNotificationBothVersions() {
        Instant ts = Frames.T0.plusSeconds(5);
        StatusNotification s16 = (StatusNotification) registry.mapCall(meta(OcppVersion.V16, "StatusNotification"),
                Frames.status16(1, "Faulted", "GroundFailure", ts));
        assertThat(s16.status()).isEqualTo(ConnectorStatus.FAULTED);
        assertThat(s16.errorCode()).isEqualTo("GroundFailure");
        assertThat(s16.meta().eventTime()).isEqualTo(ts);

        StatusNotification s201 = (StatusNotification) registry.mapCall(meta(OcppVersion.V201, "StatusNotification"),
                Frames.status201(1, 1, "Occupied", ts));
        assertThat(s201.status()).isEqualTo(ConnectorStatus.OCCUPIED);
        assertThat(s201.rawStatus()).isEqualTo("Occupied");
    }

    @Test
    void mapsTransactionEventWithEnergy() {
        TransactionEvent te = (TransactionEvent) registry.mapCall(meta(OcppVersion.V201, "TransactionEvent"),
                Frames.txEvent201("Ended", "tx-9", 3, Frames.T0, "EVCommunicationLost", 1500L, 1));
        assertThat(te.eventType()).isEqualTo(TransactionEvent.TxEventType.ENDED);
        assertThat(te.sessionId().canonical()).isEqualTo("2.0.1:ST-1:tx-9");
        assertThat(te.meterValues().get(0).find(MeterValue.ENERGY_ACTIVE_IMPORT_REGISTER))
                .map(sv -> sv.value().longValue()).contains(1500L);
    }

    @Test
    void mapsStopTransactionAndSecurityEvent() {
        StopTransaction st = (StopTransaction) registry.mapCall(meta(OcppVersion.V16, "StopTransaction"),
                Frames.stopTx16(77, 1200, "Local", Frames.T0));
        assertThat(st.sessionId().canonical()).isEqualTo("1.6:ST-1:77");

        SecurityEventNotification se = (SecurityEventNotification) registry.mapCall(
                meta(OcppVersion.V201, "SecurityEventNotification"), Frames.securityEvent201("FirmwareMismatch", Frames.T0));
        assertThat(se.type()).isEqualTo("FirmwareMismatch");
    }

    @Test
    void unknownActionFallsBackToGeneric() {
        OcppEvent e = registry.mapCall(meta(OcppVersion.V201, "NotifyChargingLimit"), Frames.obj("evseId", 2));
        assertThat(e).isInstanceOf(GenericOcppEvent.class);
        assertThat(e.action()).isEqualTo("NotifyChargingLimit");
        assertThat(((GenericOcppEvent) e).rawPayloadJson()).contains("\"evseId\":2");
    }
}
