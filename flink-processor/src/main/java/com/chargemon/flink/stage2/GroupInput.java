package com.chargemon.flink.stage2;

import com.chargemon.alert.AlertEvent;
import com.chargemon.flink.model.AggregateSnapshot;
import com.chargemon.flink.model.GroupMemberDelta;
import com.chargemon.flink.serde.JsonTypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInfo;

/** Union input of the group evaluator, keyed by group id. Exactly one payload is set. */
@TypeInfo(JsonTypeInfoFactory.class)
public record GroupInput(String groupId, AlertEvent alert, GroupMemberDelta delta, AggregateSnapshot snapshot) {

    public static GroupInput alert(String groupId, AlertEvent a) {
        return new GroupInput(groupId, a, null, null);
    }

    public static GroupInput delta(GroupMemberDelta d) {
        return new GroupInput(d.groupId(), null, d, null);
    }

    public static GroupInput snapshot(AggregateSnapshot s) {
        return new GroupInput(s.subjectId(), null, null, s);
    }
}
