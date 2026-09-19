package com.chargemon.flink.control;

import com.chargemon.flink.model.RuleChange;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads the compacted rules topic to its end once; last write per key wins, tombstones delete. */
public final class KafkaRuleLoader implements RuleLoader {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaRuleLoader.class);

    private final String bootstrap;
    private final String topic;

    public KafkaRuleLoader(String bootstrap, String topic) {
        this.bootstrap = bootstrap;
        this.topic = topic;
    }

    @Override
    public List<RuleChange> load() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "chargemon-rule-loader-" + UUID.randomUUID());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        RuleChangeDeserializer schema = new RuleChangeDeserializer();
        Map<String, RuleChange> latest = new LinkedHashMap<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic);
            if (infos == null || infos.isEmpty()) {
                LOG.warn("Rules topic {} has no partitions yet; starting with no rules", topic);
                return List.of();
            }
            List<TopicPartition> parts = infos.stream().map(i -> new TopicPartition(topic, i.partition())).toList();
            consumer.assign(parts);
            consumer.seekToBeginning(parts);
            Map<TopicPartition, Long> end = consumer.endOffsets(parts);
            while (parts.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<byte[], byte[]> r : records) {
                    schema.deserialize(r, new Collector<>() {
                        @Override
                        public void collect(RuleChange c) {
                            latest.put(c.ruleId(), c);
                        }

                        @Override
                        public void close() {
                        }
                    });
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("cannot preload rules from " + topic, e);
        }
        List<RuleChange> out = new ArrayList<>();
        latest.values().stream().filter(c -> !c.isDelete()).forEach(out::add);
        LOG.info("Preloaded {} rule(s) from {}", out.size(), topic);
        return out;
    }
}
