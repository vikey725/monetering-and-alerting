package com.chargemon.flink.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.flink.JobMain;
import com.chargemon.flink.config.JobConfig;
import com.chargemon.flink.sink.ProductionSinks;
import com.chargemon.flink.source.KafkaSources;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

/**
 * Builds the real production graph (Kafka sources, Kafka + JDBC sinks) without executing it.
 * Flink's closure cleaner serializes every user function at this point, so any
 * non-serializable capture fails here instead of at deploy time.
 */
class ProductionTopologyTest {

    @Test
    void productionGraphBuildsAndSerializes() {
        JobConfig cfg = JobConfig.defaults();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(JobMain.baseConfig());
        JobMain.configure(env, cfg);
        TopologyBuilder.build(env, cfg, new KafkaSources(cfg), new ProductionSinks(cfg));
        JobGraph graph = env.getStreamGraph().getJobGraph();
        assertThat(graph.getVertices()).isNotEmpty();
        assertThat(graph.getName()).isNotNull();
    }
}
