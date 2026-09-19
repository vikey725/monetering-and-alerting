package com.chargemon.flink.decode;

/** Undecodable input, routed to the dead-letter topic with the reason. */
public record DeadLetter(String sourceRef, String reason, String payload) {
}
