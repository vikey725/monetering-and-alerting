package com.chargemon.flink.stage2;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import java.time.Duration;

/** State descriptors shared by the stage-2 operators. */
final class Stage2Support {

    private Stage2Support() {
    }

    static MapStateDescriptor<String, byte[]> blobs(Duration ttl) {
        MapStateDescriptor<String, byte[]> d = new MapStateDescriptor<>("ruleState", Types.STRING,
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        d.enableTimeToLive(ttl(ttl));
        return d;
    }

    static MapStateDescriptor<String, Long> timers(Duration ttl) {
        MapStateDescriptor<String, Long> d = new MapStateDescriptor<>("ruleTimers", Types.STRING, Types.LONG);
        d.enableTimeToLive(ttl(ttl));
        return d;
    }

    private static StateTtlConfig ttl(Duration ttl) {
        return StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupInRocksdbCompactFilter(1000)
                .build();
    }
}
