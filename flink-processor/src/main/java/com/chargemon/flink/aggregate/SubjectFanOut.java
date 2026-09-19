package com.chargemon.flink.aggregate;

import com.chargemon.flink.energy.SessionEnergy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/** One zero-energy session becomes one record for the station plus one per (transitive) group. */
public final class SubjectFanOut implements FlatMapFunction<SessionEnergy, SubjectSession> {

    @Override
    public void flatMap(SessionEnergy s, Collector<SubjectSession> out) {
        if (!s.isZeroEnergy()) {
            return;
        }
        out.collect(new SubjectSession(SubjectKey.station(s.stationId()), s.sessionId(), s.endedAt(), s.groupIds()));
        for (String g : s.groupIds()) {
            out.collect(new SubjectSession(SubjectKey.group(g), s.sessionId(), s.endedAt(), s.groupIds()));
        }
    }
}
