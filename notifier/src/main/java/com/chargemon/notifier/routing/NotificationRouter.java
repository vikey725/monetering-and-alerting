package com.chargemon.notifier.routing;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.ChannelRef;
import com.chargemon.notifier.config.NotifierProperties;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;

/** Rule channels win; otherwise the severity fallback from configuration (then DEFAULT). */
@Component
public class NotificationRouter {

    private final NotifierProperties props;

    public NotificationRouter(NotifierProperties props) {
        this.props = props;
    }

    public List<ChannelRef> route(AlertEvent e) {
        if (!e.channels().isEmpty()) {
            return e.channels();
        }
        String configured = props.getFallback().get(e.severity().name());
        if (configured == null || configured.isBlank()) {
            configured = props.getFallback().get("DEFAULT");
        }
        List<ChannelRef> out = new ArrayList<>();
        if (configured != null) {
            for (String s : configured.split(",")) {
                if (!s.isBlank()) {
                    out.add(ChannelRef.parse(s.trim()));
                }
            }
        }
        return out;
    }
}
