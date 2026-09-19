package com.chargemon.flink.source;

import com.chargemon.flink.model.KafkaRecord;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Never fails: hands raw bytes downstream where decoding errors become dead letters. */
public final class KafkaRecordDeserializer implements KafkaRecordDeserializationSchema<KafkaRecord> {

    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<KafkaRecord> out) {
        String key = r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8);
        out.collect(new KafkaRecord(key, r.value(), r.timestamp(), r.topic() + "-" + r.partition() + "-" + r.offset()));
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return Types.POJO(KafkaRecord.class);
    }
}
