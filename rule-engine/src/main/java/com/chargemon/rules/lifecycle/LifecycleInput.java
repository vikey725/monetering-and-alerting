package com.chargemon.rules.lifecycle;

import com.chargemon.alert.ConditionSignal;
import com.chargemon.alert.ResolveReason;
import java.time.Instant;

public sealed interface LifecycleInput permits LifecycleInput.Signal, LifecycleInput.TimerFired, LifecycleInput.RuleGone {

    record Signal(ConditionSignal signal) implements LifecycleInput {
    }

    record TimerFired(TimerKind kind, Instant at) implements LifecycleInput {
    }

    record RuleGone(ResolveReason reason) implements LifecycleInput {
    }
}
