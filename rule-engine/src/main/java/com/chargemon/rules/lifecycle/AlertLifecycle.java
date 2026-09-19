package com.chargemon.rules.lifecycle;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.AlertEventType;
import com.chargemon.alert.ConditionSignal;
import com.chargemon.alert.ResolveReason;
import com.chargemon.alert.SubjectRef;
import com.chargemon.common.id.Ids;
import com.chargemon.rules.definition.RuleDefinition;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Pure state machine implementing the grace / suppression semantics:
 *
 * <ol>
 *   <li>TRIGGERED starts the grace window. CLEARED before it ends: nothing happens.</li>
 *   <li>Grace expires with the condition still true: OPENED. Suppression window starts now.</li>
 *   <li>While suppressed, re-triggers never open a new alert (they are absorbed, even after a RESOLVED).</li>
 *   <li>Suppression expires with the condition still true: a fresh grace window starts.</li>
 * </ol>
 */
public final class AlertLifecycle {

    public record Decision(LifecycleState next, List<AlertEvent> emits, List<TimerRequest> timers) {
    }

    private final Supplier<String> ids;

    public AlertLifecycle() {
        this(Ids::uuidV7String);
    }

    public AlertLifecycle(Supplier<String> ids) {
        this.ids = ids;
    }

    public Decision on(LifecycleState cur, LifecycleInput in, RuleDefinition rule, SubjectRef subject, Instant now) {
        return switch (in) {
            case LifecycleInput.Signal s -> onSignal(cur, s.signal(), rule, subject, now);
            case LifecycleInput.TimerFired t -> onTimer(cur, t, rule, subject, now);
            case LifecycleInput.RuleGone g -> onRuleGone(cur, g.reason(), rule, subject, now);
        };
    }

    // ---------------------------------------------------------------- signals

    private Decision onSignal(LifecycleState cur, ConditionSignal sig, RuleDefinition rule, SubjectRef subject, Instant now) {
        return switch (sig.kind()) {
            case TRIGGERED -> onTriggered(cur, sig, rule, subject, now);
            case CLEARED -> onCleared(cur, rule, subject, now);
        };
    }

    private Decision onTriggered(LifecycleState cur, ConditionSignal sig, RuleDefinition rule, SubjectRef subject,
                                 Instant now) {
        return switch (cur.phase()) {
            case IDLE -> startGrace(cur, sig, rule, subject, now);
            case PENDING, OPEN, SUPPRESSED -> noop(withContext(cur, sig));
            case RESOLVED -> {
                if (cur.suppressedUntil() != null && now.isBefore(cur.suppressedUntil())) {
                    yield noop(withPhase(withContext(cur, sig), Phase.SUPPRESSED));
                }
                yield startGrace(cur, sig, rule, subject, now);
            }
        };
    }

    private Decision onCleared(LifecycleState cur, RuleDefinition rule, SubjectRef subject, Instant now) {
        return switch (cur.phase()) {
            case IDLE, RESOLVED -> noop(cur);
            case PENDING -> new Decision(afterSuppression(cur, now), List.of(), List.of(TimerRequest.cancel(TimerKind.GRACE)));
            case OPEN -> resolve(cur, ResolveReason.CONDITION_CLEARED, rule, subject, now);
            case SUPPRESSED -> noop(withPhase(cur, Phase.RESOLVED));
        };
    }

    // ---------------------------------------------------------------- timers

    private Decision onTimer(LifecycleState cur, LifecycleInput.TimerFired t, RuleDefinition rule, SubjectRef subject,
                             Instant now) {
        return switch (t.kind()) {
            case GRACE -> {
                if (cur.phase() != Phase.PENDING || !t.at().equals(cur.graceDeadline())) {
                    yield noop(cur);
                }
                yield open(cur, rule, subject, now);
            }
            case SUPPRESSION -> {
                if (!t.at().equals(cur.suppressedUntil())) {
                    yield noop(cur);
                }
                yield switch (cur.phase()) {
                    case RESOLVED -> noop(LifecycleState.idle().withSeq(cur.seq()));
                    case SUPPRESSED -> startGrace(cur.withSuppressedUntil(null), null, rule, subject, now);
                    case OPEN -> noop(cur.withSuppressedUntil(null));
                    default -> noop(cur);
                };
            }
            case AUTO_RESOLVE -> {
                if (cur.phase() != Phase.OPEN || !t.at().equals(cur.autoResolveAt())) {
                    yield noop(cur);
                }
                yield resolve(cur, ResolveReason.AUTO_RESOLVE_TIMEOUT, rule, subject, now);
            }
        };
    }

    private Decision onRuleGone(LifecycleState cur, ResolveReason reason, RuleDefinition rule, SubjectRef subject,
                                Instant now) {
        List<TimerRequest> cancels = List.of(TimerRequest.cancel(TimerKind.GRACE),
                TimerRequest.cancel(TimerKind.SUPPRESSION), TimerRequest.cancel(TimerKind.AUTO_RESOLVE));
        if (cur.phase() == Phase.OPEN) {
            Decision d = resolve(cur, reason, rule, subject, now);
            return new Decision(LifecycleState.idle().withSeq(d.next().seq()), d.emits(), cancels);
        }
        return new Decision(LifecycleState.idle().withSeq(cur.seq()), List.of(), cancels);
    }

    // ---------------------------------------------------------------- transitions

    private Decision startGrace(LifecycleState cur, ConditionSignal sig, RuleDefinition rule, SubjectRef subject,
                                Instant now) {
        Duration grace = rule.timing().graceWindow();
        LifecycleState base = sig == null ? cur : withContext(cur, sig);
        LifecycleState pending = new LifecycleState(Phase.PENDING, null, base.seq(), now, null, deadline(now, grace),
                base.suppressedUntil(), null, base.context(), base.groupIds());
        if (grace.isZero()) {
            return open(pending, rule, subject, now);
        }
        return new Decision(pending, List.of(), List.of(TimerRequest.schedule(TimerKind.GRACE, pending.graceDeadline())));
    }

    private Decision open(LifecycleState cur, RuleDefinition rule, SubjectRef subject, Instant now) {
        Duration suppression = rule.timing().suppressionWindow();
        Duration autoResolve = rule.timing().autoResolveAfter();
        Instant suppressedUntil = suppression.isZero() ? null : deadline(now, suppression);
        Instant autoResolveAt = autoResolve == null ? null : deadline(now, autoResolve);
        LifecycleState next = new LifecycleState(Phase.OPEN, ids.get(), cur.seq() + 1, cur.triggeredAt(), now, null,
                suppressedUntil, autoResolveAt, cur.context(), cur.groupIds());
        List<TimerRequest> timers = new ArrayList<>(2);
        if (suppressedUntil != null) {
            timers.add(TimerRequest.schedule(TimerKind.SUPPRESSION, suppressedUntil));
        }
        if (autoResolveAt != null) {
            timers.add(TimerRequest.schedule(TimerKind.AUTO_RESOLVE, autoResolveAt));
        }
        return new Decision(next, List.of(event(next, AlertEventType.OPENED, rule, subject, null, now)), timers);
    }

    private Decision resolve(LifecycleState cur, ResolveReason reason, RuleDefinition rule, SubjectRef subject, Instant now) {
        LifecycleState resolved = new LifecycleState(Phase.RESOLVED, cur.alertId(), cur.seq() + 1, cur.triggeredAt(),
                cur.openedAt(), null, cur.suppressedUntil(), null, cur.context(), cur.groupIds());
        AlertEvent ev = event(resolved, AlertEventType.RESOLVED, rule, subject, reason, now);
        LifecycleState next = afterSuppression(resolved, now);
        return new Decision(next, List.of(ev), List.of(TimerRequest.cancel(TimerKind.AUTO_RESOLVE)));
    }

    /** RESOLVED/PENDING collapse to IDLE once no suppression window is pending. */
    private static LifecycleState afterSuppression(LifecycleState s, Instant now) {
        if (s.suppressedUntil() == null || !now.isBefore(s.suppressedUntil())) {
            return LifecycleState.idle().withSeq(s.seq());
        }
        return new LifecycleState(Phase.RESOLVED, s.alertId(), s.seq(), s.triggeredAt(), s.openedAt(), null,
                s.suppressedUntil(), null, s.context(), s.groupIds());
    }

    private AlertEvent event(LifecycleState s, AlertEventType type, RuleDefinition rule, SubjectRef subject,
                             ResolveReason reason, Instant now) {
        return new AlertEvent(
                ids.get(),
                s.alertId(),
                rule.id() + "|" + (subject == null ? "" : subject.key()),
                s.seq(),
                type,
                rule.id(),
                rule.version(),
                rule.name(),
                rule.kind().name(),
                rule.severity(),
                rule.channels(),
                subject == null ? rule.subjectType() : subject.type(),
                subject == null ? "" : subject.id(),
                s.groupIds(),
                s.triggeredAt(),
                s.openedAt(),
                type == AlertEventType.RESOLVED ? now : null,
                reason,
                s.context(),
                now,
                AlertEvent.CURRENT_SCHEMA_VERSION);
    }

    /** Timer services work at millisecond precision; deadlines must round-trip exactly. */
    private static Instant deadline(Instant now, Duration d) {
        return now.plus(d).truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
    }

    private static Decision noop(LifecycleState s) {
        return new Decision(s, List.of(), List.of());
    }

    private static LifecycleState withContext(LifecycleState s, ConditionSignal sig) {
        Map<String, String> ctx = sig.context().isEmpty() ? s.context() : sig.context();
        Set<String> groups = sig.groupIds().isEmpty() ? s.groupIds() : sig.groupIds();
        return new LifecycleState(s.phase(), s.alertId(), s.seq(), s.triggeredAt(), s.openedAt(), s.graceDeadline(),
                s.suppressedUntil(), s.autoResolveAt(), ctx, groups);
    }

    private static LifecycleState withPhase(LifecycleState s, Phase p) {
        return new LifecycleState(p, s.alertId(), s.seq(), s.triggeredAt(), s.openedAt(), s.graceDeadline(),
                s.suppressedUntil(), s.autoResolveAt(), s.context(), s.groupIds());
    }
}
