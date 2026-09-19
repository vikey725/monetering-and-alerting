package com.chargemon.rules.eval;

import com.chargemon.alert.ConditionSignal;
import com.chargemon.alert.SubjectRef;
import java.time.Instant;
import java.util.Set;

/** Everything an evaluator may touch. Implemented by the Flink operator (and by test fakes). */
public interface RuleContext {

    SubjectRef subject();

    /** Subject's transitive group ids (last known), used for timer-driven signals. */
    Set<String> groupIds();

    RuleStateStore state();

    RuleTimers timers();

    void emit(ConditionSignal signal);

    /** Processing time. */
    Instant now();
}
