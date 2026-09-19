package com.chargemon.notifier.channel;

/** Transient (retry) vs permanent (give up, record failure) delivery errors. */
public class DeliveryException extends RuntimeException {

    private final boolean transientFailure;

    public DeliveryException(String message, boolean transientFailure) {
        super(message);
        this.transientFailure = transientFailure;
    }

    public DeliveryException(String message, boolean transientFailure, Throwable cause) {
        super(message, cause);
        this.transientFailure = transientFailure;
    }

    public boolean isTransient() {
        return transientFailure;
    }

    public static DeliveryException transientError(String message, Throwable cause) {
        return new DeliveryException(message, true, cause);
    }

    public static DeliveryException permanent(String message) {
        return new DeliveryException(message, false);
    }
}
