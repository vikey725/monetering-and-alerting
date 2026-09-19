package com.chargemon.flink.control;

import com.chargemon.flink.model.RuleChange;
import java.io.Serializable;
import java.util.List;

/**
 * Initial rule snapshot for an operator instance, loaded in {@code open()} before
 * any event is processed. Removes the start-up race between the event stream and
 * the rules broadcast: the broadcast then only delivers changes on top.
 */
@FunctionalInterface
public interface RuleLoader extends Serializable {

    List<RuleChange> load();

    static RuleLoader none() {
        return List::of;
    }

    static RuleLoader of(List<RuleChange> rules) {
        List<RuleChange> copy = List.copyOf(rules);
        return () -> copy;
    }
}
