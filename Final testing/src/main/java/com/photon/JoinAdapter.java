package com.photon;

/**
 * JoinAdapter — pluggable business logic for combining a click with its query.
 *
 * Maps to the "adapter" library described in §3.3 of the Photon paper:
 *
 *   "Usually, the adapter simply combines the two events into one joined event
 *    and passes the result back to the joiner. However, it may also apply
 *    application-specific business logic for filtering or force the joiner to
 *    skip the click based on certain properties of the query."
 *
 * Isolating this logic here means you can swap join strategies (ad billing,
 * fraud detection, analytics, etc.) without touching the Joiner infrastructure.
 *
 * Return null from combine() to signal that this click should be skipped
 * (e.g. filtered out by business logic) — the Joiner will treat it as UNJOINABLE.
 */
public interface JoinAdapter {

    /**
     * Combine a click event with its corresponding query event.
     *
     * @param queryId      the shared key that linked these two events
     * @param clickPayload  raw bytes of the foreign event (click)
     * @param queryPayload  raw bytes of the primary event (query), from EventStore
     * @return the serialised joined event to write to the output topic,
     *         or null to skip this event (filtered by business logic)
     */
    byte[] combine(String queryId, byte[] clickPayload, byte[] queryPayload);

    // ── Default implementation ────────────────────────────────────────────

    /**
     * Returns a simple default adapter that produces a length-prefixed
     * concatenation of the two payloads:
     *
     *   [4 bytes: queryPayload length][queryPayload bytes][clickPayload bytes]
     *
     * This is intentionally minimal — real deployments will replace this with
     * Avro/Protobuf merging, JSON assembly, or domain-specific join logic.
     */
    static JoinAdapter defaultAdapter() {
        return (queryId, clickPayload, queryPayload) -> {
            // Encode as: 4-byte query length | query bytes | click bytes
            byte[] joined = new byte[4 + queryPayload.length + clickPayload.length];

            // Write query length as big-endian int
            int qLen = queryPayload.length;
            joined[0] = (byte) (qLen >>> 24);
            joined[1] = (byte) (qLen >>> 16);
            joined[2] = (byte) (qLen >>> 8);
            joined[3] = (byte)  qLen;

            System.arraycopy(queryPayload, 0, joined, 4,            queryPayload.length);
            System.arraycopy(clickPayload, 0, joined, 4 + qLen,     clickPayload.length);

            return joined;
        };
    }
}
