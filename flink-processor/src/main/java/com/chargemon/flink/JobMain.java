package com.chargemon.flink;

import com.chargemon.common.config.Env;
import com.chargemon.flink.config.JobConfig;
import com.chargemon.flink.sink.ProductionSinks;
import com.chargemon.flink.source.KafkaSources;
import com.chargemon.flink.topology.TopologyBuilder;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class JobMain {

    private JobMain() {
    }

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.fromEnv(Env.system());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(baseConfig());
        configure(env, cfg);
        TopologyBuilder.build(env, cfg, new KafkaSources(cfg), new ProductionSinks(cfg));
        env.execute("chargemon-event-processor");
    }

    /** Settings every environment (prod, tests) must share. */
    public static Configuration baseConfig() {
        Configuration c = new Configuration();
        c.set(PipelineOptions.GENERIC_TYPES, false);      // fail fast on Kryo fallback
        c.set(PipelineOptions.OBJECT_REUSE, true);        // everything we carry is immutable
        return c;
    }

    public static void configure(StreamExecutionEnvironment env, JobConfig cfg) {
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(cfg.checkpointInterval().toMillis(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(cfg.checkpointInterval().toMillis() / 2);
        env.getCheckpointConfig().setCheckpointTimeout(java.time.Duration.ofMinutes(10).toMillis());
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        if (cfg.parallelism() > 0) {
            env.setParallelism(cfg.parallelism());
        }
    }
}
