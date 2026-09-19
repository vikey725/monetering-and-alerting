package com.chargemon.flink.enrich;

import com.chargemon.flink.model.EnrichedEvent;
import com.chargemon.flink.model.GroupMemberDelta;
import com.chargemon.flink.model.StationStreamElement;
import com.chargemon.flink.serde.JsonTypes;
import com.chargemon.ocpp.model.station.GroupRecord;
import com.chargemon.ocpp.model.station.StationContext;
import com.chargemon.ocpp.model.station.StationRecord;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keyed by station: joins events with the station mirror (keyed state) and the
 * group hierarchy (broadcast state) into {@link EnrichedEvent}. Membership
 * changes are emitted as {@link GroupMemberDelta} for group-level consumers.
 */
public final class StationEnrichmentOperator
        extends KeyedBroadcastProcessFunction<String, StationStreamElement, GroupRecord, EnrichedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(StationEnrichmentOperator.class);

    public static final MapStateDescriptor<String, GroupRecord> GROUPS =
            new MapStateDescriptor<>("groups", Types.STRING, JsonTypes.of(GroupRecord.class));
    public static final OutputTag<GroupMemberDelta> MEMBER_DELTAS = new OutputTag<>("member-deltas") {
    };

    private transient ValueState<StationRecord> station;
    /** Group closure last announced via {@link #MEMBER_DELTAS}; re-diffed on every event so late group records self-heal. */
    private transient ValueState<List<String>> announced;
    private transient GroupHierarchy hierarchy;
    private transient Counter unknownStations;

    @Override
    public void open(OpenContext ctx) {
        station = getRuntimeContext().getState(new ValueStateDescriptor<>("station", JsonTypes.of(StationRecord.class)));
        announced = getRuntimeContext().getState(new ValueStateDescriptor<>("announcedGroups", Types.LIST(Types.STRING)));
        hierarchy = new GroupHierarchy();
        unknownStations = getRuntimeContext().getMetricGroup().counter("unknownStationEvents");
    }

    @Override
    public void processElement(StationStreamElement el, ReadOnlyContext ctx, Collector<EnrichedEvent> out) throws Exception {
        ReadOnlyBroadcastState<String, GroupRecord> groups = ctx.getBroadcastState(GROUPS);
        if (el.station() != null) {
            onStationRecord(el.station(), groups, ctx);
            return;
        }
        StationRecord rec = station.value();
        StationContext context;
        if (rec == null || rec.deleted()) {
            unknownStations.inc();
            context = StationContext.unknown(ctx.getCurrentKey());
        } else {
            Set<String> closure = hierarchy.closure(rec.groupIds(), lookup(groups));
            announce(rec.stationId(), closure, ctx);
            context = StationContext.from(rec, closure);
        }
        LOG.debug("enrich station={} action={} known={} groups={}", ctx.getCurrentKey(), el.event().action(), context.known(), context.allGroupIds());
        out.collect(new EnrichedEvent(el.event(), context));
    }

    private void onStationRecord(StationRecord next, ReadOnlyBroadcastState<String, GroupRecord> groups,
                                 ReadOnlyContext ctx) throws Exception {
        Set<String> after = next.deleted() ? Set.of() : hierarchy.closure(next.groupIds(), lookup(groups));
        announce(next.stationId(), after, ctx);
        if (next.deleted()) {
            station.clear();
        } else {
            station.update(next);
        }
    }

    /** Emits ADD/REMOVE deltas for the difference between the last announced closure and the current one. */
    private void announce(String stationId, Set<String> closure, ReadOnlyContext ctx) throws Exception {
        List<String> prev = announced.value();
        Set<String> before = prev == null ? Set.of() : new HashSet<>(prev);
        if (before.equals(closure)) {
            return;
        }
        for (String g : closure) {
            if (!before.contains(g)) {
                ctx.output(MEMBER_DELTAS, new GroupMemberDelta(g, stationId, true));
            }
        }
        for (String g : before) {
            if (!closure.contains(g)) {
                ctx.output(MEMBER_DELTAS, new GroupMemberDelta(g, stationId, false));
            }
        }
        if (closure.isEmpty()) {
            announced.clear();
        } else {
            announced.update(List.copyOf(closure));
        }
    }

    @Override
    public void processBroadcastElement(GroupRecord g, Context ctx, Collector<EnrichedEvent> out) throws Exception {
        BroadcastState<String, GroupRecord> groups = ctx.getBroadcastState(GROUPS);
        if (g.deleted()) {
            groups.remove(g.groupId());
        } else {
            groups.put(g.groupId(), g);
        }
        hierarchy.invalidate();
        // Memberships are re-announced lazily on each station's next event (see announce), so late or
        // re-parented group records converge without replaying the stations topic.
    }

    private static java.util.function.Function<String, GroupRecord> lookup(ReadOnlyBroadcastState<String, GroupRecord> s) {
        return id -> {
            try {
                return s.get(id);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        };
    }

    static Set<String> union(Set<String> a, Map<String, ?> b) {
        Set<String> s = new HashSet<>(a);
        s.addAll(b.keySet());
        return s;
    }
}
