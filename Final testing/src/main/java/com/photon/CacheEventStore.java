package com.photon;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;

/**
 * CacheEventStore — Redis-backed hot path for EventStore lookups.
 *
 * Maps to §3.4.1 of the Photon paper ("CacheEventStore").
 *
 * Design:
 *   - Keys:   "photon:q:{query_id}"
 *   - Values: raw event bytes
 *   - TTL:    cacheTtlSeconds (default ~5 minutes; clicks arrive shortly after queries)
 *
 * The paper notes CacheEventStore is sharded by hash(query_id). In production,
 * swap RedisClient for RedisClusterClient (also available via Lettuce) and
 * configure consistent hashing at the cluster level — the lookup/store API
 * here is identical either way.
 *
 * This is a best-effort cache: a miss is not an error, it just falls through
 * to LogsEventStore (handled by TieredEventStore).
 */
public class CacheEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(CacheEventStore.class);

    private static final String KEY_PREFIX = "photon:q:";

    // ── Defaults ──────────────────────────────────────────────────────────
    /** How long a query event stays in Redis. Clicks must arrive within this window
     *  to get a cache hit. Set higher (e.g. 30 min) during high-latency catch-up. */
    public static final int DEFAULT_TTL_SECONDS = 300; // 5 minutes

    // ── State ─────────────────────────────────────────────────────────────
    private final RedisClient                                        client;
    private final StatefulRedisConnection<String, byte[]>           connection;
    private final RedisCommands<String, byte[]>                      commands;
    private final long                                               ttlSeconds;

    // ── Metrics ───────────────────────────────────────────────────────────
    private static final Counter LOOKUPS = Counter.build()
            .name("photon_cache_event_store_lookups_total")
            .help("CacheEventStore lookup results")
            .labelNames("result") // hit | miss | error
            .register();

    private static final Histogram LOOKUP_LATENCY = Histogram.build()
            .name("photon_cache_event_store_lookup_latency_seconds")
            .help("CacheEventStore lookup latency")
            .buckets(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05)
            .register();

    private static final Counter STORES = Counter.build()
            .name("photon_cache_event_store_stores_total")
            .help("CacheEventStore store attempts")
            .labelNames("status") // ok | error
            .register();

    // ── Constructor ───────────────────────────────────────────────────────

    public CacheEventStore(String redisHost, int redisPort, int ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
        this.client     = RedisClient.create(
                RedisURI.builder().withHost(redisHost).withPort(redisPort)
                        .withTimeout(Duration.ofSeconds(2))
                        .build());

        // Use a mixed codec: String keys, byte[] values — avoids double-encoding
        this.connection = client.connect(new MixedCodec());
        this.commands   = connection.sync();

        log.info("CacheEventStore connected | redis={}:{} | ttl={}s", redisHost, redisPort, ttlSeconds);
    }

    public CacheEventStore(String redisHost, int redisPort) {
        this(redisHost, redisPort, DEFAULT_TTL_SECONDS);
    }

    // ── EventStore ────────────────────────────────────────────────────────

    @Override
    public Optional<byte[]> lookup(String queryId) {
        Histogram.Timer timer = LOOKUP_LATENCY.startTimer();
        try {
            byte[] value = commands.get(key(queryId));
            if (value != null) {
                LOOKUPS.labels("hit").inc();
                return Optional.of(value);
            } else {
                LOOKUPS.labels("miss").inc();
                return Optional.empty();
            }
        } catch (Exception e) {
            log.warn("CacheEventStore lookup failed for queryId={}: {}", queryId, e.getMessage());
            LOOKUPS.labels("error").inc();
            return Optional.empty(); // degrade gracefully — fall through to LogsEventStore
        } finally {
            timer.observeDuration();
        }
    }

    @Override
    public void store(String queryId, byte[] eventBytes) {
        try {
            // SET key value EX ttl — atomic write with expiry
            commands.set(key(queryId), eventBytes, SetArgs.Builder.ex(ttlSeconds));
            STORES.labels("ok").inc();
        } catch (Exception e) {
            log.warn("CacheEventStore store failed for queryId={}: {}", queryId, e.getMessage());
            STORES.labels("error").inc();
            // Non-fatal: LogsEventStore will still have the event
        }
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown();
        log.info("CacheEventStore closed");
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static String key(String queryId) {
        return KEY_PREFIX + queryId;
    }

    /**
     * Lettuce codec that uses String encoding for keys and raw byte arrays for
     * values, avoiding unnecessary UTF-8 re-encoding of binary event payloads.
     */
    private static class MixedCodec implements RedisCodec<String, byte[]> {
        private final StringCodec    keyCodec   = StringCodec.UTF8;
        private final ByteArrayCodec valueCodec = ByteArrayCodec.INSTANCE;

        @Override public ByteBuffer encodeKey(String key)        { return keyCodec.encodeKey(key); }
        @Override public String      decodeKey(ByteBuffer bytes)  { return keyCodec.decodeKey(bytes); }
        @Override public ByteBuffer  encodeValue(byte[] value)    { return valueCodec.encodeValue(value); }
        @Override public byte[]      decodeValue(ByteBuffer bytes) { return valueCodec.decodeValue(bytes); }
    }
}
