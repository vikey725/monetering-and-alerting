package com.chargemon.ocpp.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EnumParsingTest {

    @Test
    void parsesVersionsLeniently() {
        assertThat(OcppVersion.parse("ocpp1.6")).isEqualTo(OcppVersion.V16);
        assertThat(OcppVersion.parse("2.0.1")).isEqualTo(OcppVersion.V201);
        assertThat(OcppVersion.parse("OCPP2.0.1")).isEqualTo(OcppVersion.V201);
    }

    @Test
    void parsesDirectionsAndStatuses() {
        assertThat(Direction.parse("inbound")).isEqualTo(Direction.STATION_TO_CSMS);
        assertThat(Direction.parse("csms-to-station")).isEqualTo(Direction.CSMS_TO_STATION);
        assertThat(ConnectorStatus.parse("SuspendedEV")).isEqualTo(ConnectorStatus.SUSPENDED_EV);
        assertThat(ConnectorStatus.parse("weird")).isEqualTo(ConnectorStatus.UNKNOWN);
        assertThat(AuthStatus.parse("ConcurrentTx")).isEqualTo(AuthStatus.CONCURRENT_TX);
    }
}
