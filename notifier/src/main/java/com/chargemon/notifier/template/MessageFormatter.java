package com.chargemon.notifier.template;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.AlertEventType;
import com.chargemon.notifier.channel.Notification;
import java.time.Duration;
import java.util.Map;
import org.springframework.stereotype.Component;

/** Renders the human-readable title/body shared by all channels. */
@Component
public class MessageFormatter {

    public Notification format(AlertEvent e) {
        String state = e.type() == AlertEventType.OPENED ? "OPEN" : "RESOLVED";
        String title = "[%s] %s: %s %s".formatted(e.severity(), state, e.subjectType().name().toLowerCase(), e.subjectId());
        StringBuilder b = new StringBuilder();
        b.append("Rule: ").append(e.ruleName()).append(" (").append(e.kind()).append(")\n");
        b.append("Subject: ").append(e.subjectType()).append(' ').append(e.subjectId()).append('\n');
        if (!e.groupIds().isEmpty()) {
            b.append("Groups: ").append(String.join(", ", e.groupIds())).append('\n');
        }
        b.append("Triggered: ").append(e.triggeredAt()).append('\n');
        b.append("Opened: ").append(e.openedAt()).append('\n');
        if (e.type() == AlertEventType.RESOLVED) {
            b.append("Resolved: ").append(e.resolvedAt()).append(" (").append(e.resolveReason()).append(")\n");
            if (e.openedAt() != null && e.resolvedAt() != null) {
                b.append("Duration: ").append(Duration.between(e.openedAt(), e.resolvedAt())).append('\n');
            }
        }
        if (!e.context().isEmpty()) {
            b.append("Details:\n");
            for (Map.Entry<String, String> c : e.context().entrySet()) {
                b.append("  ").append(c.getKey()).append(" = ").append(c.getValue()).append('\n');
            }
        }
        b.append("Alert id: ").append(e.alertId());
        return new Notification(e, title, b.toString());
    }
}
