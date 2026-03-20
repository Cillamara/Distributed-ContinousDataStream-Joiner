package com.photon;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hybrid Logical Clock (HLC).
 * Upper 44 bits = wall time in milliseconds.
 * Lower 20 bits = logical counter.
 * Guarantees monotonically increasing timestamps even when
 * wall clock doesn't advance, bounding skew to NTP accuracy (~50ms).
 */
public class HybridLogicalClock {

    private static final int LOGICAL_BITS = 20;
    private static final long LOGICAL_MASK = (1L << LOGICAL_BITS) - 1;
    private static final long MAX_LOGICAL  = LOGICAL_MASK;

    private final AtomicLong state = new AtomicLong(pack(wallMs(), 0));

    // ── Send / generate a new timestamp ──────────────────────────────────

    public long now() {
        while (true) {
            long current  = state.get();
            long wallTime = wallMs();
            long hlcTime  = unpackTime(current);
            long logical  = unpackLogical(current);

            long newTime;
            long newLogical;

            if (wallTime > hlcTime) {
                newTime    = wallTime;
                newLogical = 0;
            } else {
                newTime    = hlcTime;
                newLogical = logical + 1;
                if (newLogical > MAX_LOGICAL) {
                    // Clock hasn't advanced — spin until it does
                    Thread.onSpinWait();
                    continue;
                }
            }

            long next = pack(newTime, newLogical);
            if (state.compareAndSet(current, next)) {
                return next;
            }
        }
    }

    // ── Receive / update on incoming message ─────────────────────────────

    public long update(long received) {
        while (true) {
            long current  = state.get();
            long wallTime = wallMs();
            long hlcTime  = unpackTime(current);
            long recvTime = unpackTime(received);

            long newTime    = Math.max(Math.max(wallTime, hlcTime), recvTime);
            long newLogical;

            if (newTime == hlcTime && newTime == recvTime) {
                newLogical = Math.max(unpackLogical(current), unpackLogical(received)) + 1;
            } else if (newTime == hlcTime) {
                newLogical = unpackLogical(current) + 1;
            } else if (newTime == recvTime) {
                newLogical = unpackLogical(received) + 1;
            } else {
                newLogical = 0;
            }

            if (newLogical > MAX_LOGICAL) {
                Thread.onSpinWait();
                continue;
            }

            long next = pack(newTime, newLogical);
            if (state.compareAndSet(current, next)) {
                return next;
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static long pack(long wallMs, long logical) {
        return (wallMs << LOGICAL_BITS) | (logical & LOGICAL_MASK);
    }

    public static long unpackTime(long hlc) {
        return hlc >>> LOGICAL_BITS;
    }

    public static long unpackLogical(long hlc) {
        return hlc & LOGICAL_MASK;
    }

    public static Instant toInstant(long hlc) {
        return Instant.ofEpochMilli(unpackTime(hlc));
    }

    private static long wallMs() {
        return Instant.now().toEpochMilli();
    }
}