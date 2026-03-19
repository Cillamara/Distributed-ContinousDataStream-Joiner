package com.photon;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Globally unique event identifier.
 * Structure mirrors the paper: ServerIP + ProcessID + HLC Timestamp.
 * Encoded as a single long for cheap storage in etcd.
 *
 * Layout (64 bits):
 *   [0..19]  = lower 20 bits of IP (avoids IPv6 issues)
 *   [20..35] = lower 16 bits of PID
 *   [36..63] = upper 28 bits of HLC timestamp (ms precision, ~8 year range)
 */
public class EventId {

    private static final HybridLogicalClock HLC = new HybridLogicalClock();

    private static final long SERVER_IP;
    private static final long PROCESS_ID;

    static {
        try {
            byte[] addr = InetAddress.getLocalHost().getAddress();
            SERVER_IP = ((addr[2] & 0xFFL) << 8) | (addr[3] & 0xFFL);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Cannot resolve local host address", e);
        }
        PROCESS_ID = ProcessHandle.current().pid() & 0xFFFFL;
    }

    public final long value;
    public final long hlcTimestamp;

    private EventId(long value, long hlcTimestamp) {
        this.value        = value;
        this.hlcTimestamp = hlcTimestamp;
    }

    // ── Factory ───────────────────────────────────────────────────────────

    public static EventId generate() {
        long hlc     = HLC.now();
        long hlcLow = hlc & 0xFFFFFFFFFL;   // 36 bits
        long packed = (SERVER_IP << 48) | (PROCESS_ID << 36) | hlcLow;
        return new EventId(packed, hlc);
    }

    public static EventId fromLong(long value) {
        return new EventId(value, 0L);
    }

    // ── Shard assignment (deterministic) ─────────────────────────────────

    public int shard(int totalShards) {
        return (int) (Long.remainderUnsigned(value, totalShards));
    }

    // ── Utilities ─────────────────────────────────────────────────────────

    public long wallTimeMs() {
        return HybridLogicalClock.unpackTime(hlcTimestamp);
    }

    @Override
    public String toString() {
        return String.format("EventId{value=%016x, hlc=%d}", value, hlcTimestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventId other)) return false;
        return value == other.value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }
}