package com.photon;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * PendingClick — a click event waiting to be (re)dispatched to a Joiner.
 *
 * Implements {@link Delayed} so instances can live in a {@link java.util.concurrent.DelayQueue}
 * and become eligible for retry only after their backoff period has elapsed.
 *
 * Kafka record conventions expected by the Dispatcher:
 *   record.key()                 → query_id  (String)
 *   header "click-id"            → EventId.value as hex string
 *   header "click-hlc"           → full HLC timestamp as decimal string
 *   record.value()               → raw click payload bytes
 */
public final class PendingClick implements Delayed {

    // ── Retry config ──────────────────────────────────────────────────────
    public static final int    MAX_ATTEMPTS      = 10;
    public static final long   INITIAL_BACKOFF_MS = 100;
    public static final double BACKOFF_MULTIPLIER  = 2.0;
    public static final long   MAX_BACKOFF_MS     = 30_000; // 30 s

    // ── Fields ────────────────────────────────────────────────────────────
    public final long   eventId;        // packed EventId.value — dedup key in IdRegistry
    public final long   hlcTimestamp;   // full HLC — used for age check
    public final String queryId;        // primary key to look up in EventStore
    public final byte[] payload;        // raw click bytes forwarded to Joiner

    public final int  attempts;         // how many times we've tried so far
    public final long nextRetryMs;      // absolute epoch-ms when this becomes eligible

    // ── Constructors ──────────────────────────────────────────────────────

    /** Create a fresh PendingClick eligible for immediate dispatch. */
    public PendingClick(long eventId, long hlcTimestamp, String queryId, byte[] payload) {
        this(eventId, hlcTimestamp, queryId, payload, 0, 0);
    }

    private PendingClick(long eventId, long hlcTimestamp, String queryId, byte[] payload,
                         int attempts, long nextRetryMs) {
        this.eventId       = eventId;
        this.hlcTimestamp  = hlcTimestamp;
        this.queryId       = queryId;
        this.payload       = payload;
        this.attempts      = attempts;
        this.nextRetryMs   = nextRetryMs;
    }

    // ── Retry factory ─────────────────────────────────────────────────────

    /**
     * Returns a new PendingClick scheduled for retry after exponential backoff.
     * The backoff doubles each attempt, capped at MAX_BACKOFF_MS.
     */
    public PendingClick scheduleRetry() {
        long backoff = (long) (INITIAL_BACKOFF_MS * Math.pow(BACKOFF_MULTIPLIER, attempts));
        backoff = Math.min(backoff, MAX_BACKOFF_MS);
        return new PendingClick(eventId, hlcTimestamp, queryId, payload,
                attempts + 1,
                System.currentTimeMillis() + backoff);
    }

    public boolean hasRetriesLeft() {
        return attempts < MAX_ATTEMPTS;
    }

    // ── Delayed ───────────────────────────────────────────────────────────

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(nextRetryMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        return Long.compare(nextRetryMs, ((PendingClick) other).nextRetryMs);
    }

    @Override
    public String toString() {
        return String.format("PendingClick{id=%s, query=%s, attempts=%d, nextRetry=%dms}",
                Long.toHexString(eventId), queryId, attempts,
                Math.max(0, nextRetryMs - System.currentTimeMillis()));
    }
}
