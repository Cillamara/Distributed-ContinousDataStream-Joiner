package com.photon;

import java.util.Optional;

/**
 * EventStore — looks up primary events (e.g. query) by their event ID.
 *
 * Maps to §3.4 of the Photon paper. The Joiner calls this to retrieve the
 * full primary event payload so it can be merged with the foreign event
 * (e.g. click) to produce a joined output record.
 *
 * Two implementations (composed by TieredEventStore):
 *   CacheEventStore  — Redis, in-memory, fast, short TTL  (§3.4.1)
 *   LogsEventStore   — HBase, persistent, slower, long TTL (§3.4.2)
 *
 * Event payloads are raw bytes — callers are responsible for
 * serialisation/deserialisation (e.g. JSON, Avro, Protobuf).
 */
public interface EventStore {

    /**
     * Look up an event by its query_id.
     *
     * @param queryId the unique identifier of the primary event
     * @return the raw event payload, or empty if not found
     */
    Optional<byte[]> lookup(String queryId);

    /**
     * Store an event so it can be retrieved later by the Joiner.
     * Called by the query log reader process as it consumes the primary stream.
     *
     * @param queryId    the unique identifier of the primary event
     * @param eventBytes the serialised event payload
     */
    void store(String queryId, byte[] eventBytes);

    /**
     * Close underlying connections. Implement AutoCloseable pattern.
     */
    void close();
}
