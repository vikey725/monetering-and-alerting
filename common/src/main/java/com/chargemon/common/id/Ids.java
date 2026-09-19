package com.chargemon.common.id;

import java.security.SecureRandom;
import java.util.UUID;

/** Time-ordered UUIDv7 generator for event and alert identifiers. */
public final class Ids {

    private static final SecureRandom RANDOM = new SecureRandom();

    private Ids() {
    }

    public static UUID uuidV7() {
        return uuidV7(System.currentTimeMillis());
    }

    public static UUID uuidV7(long epochMillis) {
        long randA = RANDOM.nextLong() & 0x0FFFL;
        long msb = (epochMillis << 16) | 0x7000L | randA;
        long lsb = (RANDOM.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(msb, lsb);
    }

    public static String uuidV7String() {
        return uuidV7().toString();
    }
}
