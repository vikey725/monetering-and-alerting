package com.chargemon.flink.decode;

import com.chargemon.common.result.Result;
import com.chargemon.flink.model.DecodedFrame;
import com.chargemon.flink.model.KafkaRecord;
import com.chargemon.ocpp.codec.envelope.EnvelopeParser;
import com.chargemon.ocpp.codec.envelope.RawEnvelope;
import com.chargemon.ocpp.codec.frame.FrameParser;
import com.chargemon.ocpp.codec.frame.RawFrame;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Kafka bytes -> envelope + frame. The station id is the record key; the body carries none.
 * Anything unparseable (including a keyless record) goes to the dead-letter side output.
 */
public final class FrameDecodeFunction extends ProcessFunction<KafkaRecord, DecodedFrame> {

    public static final OutputTag<DeadLetter> DEAD_LETTER = new OutputTag<>("dead-letter") {
    };

    private transient EnvelopeParser envelopes;
    private transient FrameParser frames;
    private transient Counter decoded;
    private transient Counter rejected;

    @Override
    public void open(OpenContext ctx) {
        envelopes = new EnvelopeParser();
        frames = new FrameParser();
        decoded = getRuntimeContext().getMetricGroup().counter("framesDecoded");
        rejected = getRuntimeContext().getMetricGroup().counter("framesRejected");
    }

    @Override
    public void processElement(KafkaRecord rec, Context ctx, Collector<DecodedFrame> out) {
        Result<RawEnvelope, String> env = envelopes.parse(rec.key(), rec.value(), rec.sourceRef());
        if (env instanceof Result.Err<RawEnvelope, String> err) {
            reject(ctx, rec, err.error());
            return;
        }
        RawEnvelope envelope = ((Result.Ok<RawEnvelope, String>) env).value();
        Result<RawFrame, String> frame = frames.parse(envelope.frame());
        if (frame instanceof Result.Err<RawFrame, String> err) {
            reject(ctx, rec, err.error());
            return;
        }
        decoded.inc();
        out.collect(new DecodedFrame(envelope, ((Result.Ok<RawFrame, String>) frame).value()));
    }

    private void reject(Context ctx, KafkaRecord rec, String reason) {
        rejected.inc();
        String payload = rec.value() == null ? "" : new String(rec.value(), StandardCharsets.UTF_8);
        ctx.output(DEAD_LETTER, new DeadLetter(rec.sourceRef(), reason, payload));
    }
}
