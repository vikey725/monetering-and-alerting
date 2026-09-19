package com.chargemon.notifier.channel.email;

import com.chargemon.notifier.channel.DeliveryException;
import com.chargemon.notifier.channel.Notification;
import com.chargemon.notifier.channel.NotificationChannel;
import com.chargemon.notifier.config.NotifierProperties;
import org.springframework.mail.MailAuthenticationException;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

/** Plain-text email to the target address. */
@Component
public class EmailChannel implements NotificationChannel {

    private final JavaMailSender sender;
    private final NotifierProperties props;

    public EmailChannel(JavaMailSender sender, NotifierProperties props) {
        this.sender = sender;
        this.props = props;
    }

    @Override
    public String type() {
        return "email";
    }

    @Override
    public void send(Notification n, String target) {
        SimpleMailMessage msg = new SimpleMailMessage();
        msg.setFrom(props.getEmail().getFrom());
        msg.setTo(target);
        msg.setSubject(n.title());
        msg.setText(n.body());
        try {
            sender.send(msg);
        } catch (MailAuthenticationException e) {
            throw DeliveryException.permanent("smtp auth failed: " + e.getMessage());
        } catch (MailException e) {
            throw DeliveryException.transientError("smtp: " + e.getMessage(), e);
        }
    }
}
