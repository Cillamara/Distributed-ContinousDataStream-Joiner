package com.photon;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * TieredEventStore — composes CacheEventStore (Redis) and LogsEventStore (HBase).
 *
 * This is the EventStore implementation the Joiner actually uses. It mirrors
 * the two-tier lookup described in §3.4 of the Photon paper:
 *
 *   1. Try CacheEventStore (Redis) — fast, in-memory, ~ms latency.
 *      Handles the vast majority of lookups since clicks arrive shortly
 *      after the corresponding query (temporal locality).
 *
 *   2. On cache miss, fall back to LogsEventStore (HBase) — persistent,
 *      ~10–50ms latency. Handles catch-up after outages and stale events
 *      that have aged out of the cache TTL.
 *
 *   3. On HBase hit, backfill the cache — future lookups for the same
 *      query_id will hit Redis directly (warms the cache during catch-up).
 *
 * The paper notes that in production, ~90%+ of lookups hit the cache.
 * Cache misses spike during catch-up (§3.2.2) and then settle back down.
 *
 * Stores write to both tiers so both are consistent:
 *   - Redis: fast TTL-bounded write (best-effort, non-fatal on failure)
 *   - HBase: durable write (fatal on failure — required for correctness)
 */
public class TieredEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(TieredEventStore.class);

    private final EventStore cache;
    private final EventStore logs;

    // ── Metrics ───────────────────────────────────────────────────────────

    private static final Counter LOOKUPS = Counter.build()
            .name("photon_tiered_event_store_lookups_total")
            .help("TieredEventStore lookups by tier that served the result")
            .labelNames("tier") // cache_hit | logs_hit | not_found
            .register();

    private static final Gauge CACHE_HIT_RATE = Gauge.build()
            .name("photon_tiered_event_store_cache_hit_rate")
            .help("Rolling cache hit rate (cache_hits / total_lookups). " +
                  "Should be >0.9 in steady state; dips during catch-up.")
            .register();

    // Simple counters for hit-rate gauge computation
    private long totalLookups = 0;
    private long cacheHits    = 0;

    // ── Constructor ───────────────────────────────────────────────────────

    public TieredEventStore(EventStore cache, EventStore logs) {
        this.cache = cache;
        this.logs  = logs;
    }

    // ── EventStore ────────────────────────────────────────────────────────

    /**
     * Lookup with cache-first fallback.
     *
     * Hot path  (cache hit):  Redis GET → return
     * Cold path (cache miss): HBase GET → backfill Redis → return
     */
    @Override
    public Optional<byte[]> lookup(String queryId) {
        totalLookups++;

        // ── Tier 1: Redis ────────────────────────────────────────────────
        Optional<byte[]> cached = cache.lookup(queryId);
        if (cached.isPresent()) {
            cacheHits++;
            LOOKUPS.labels("cache_hit").inc();
            updateHitRate();
            return cached;
        }

        // ── Tier 2: HBase ────────────────────────────────────────────────
        Optional<byte[]> fromLogs = logs.lookup(queryId);

        if (fromLogs.isPresent()) {
            LOOKUPS.labels("logs_hit").inc();
            // Backfill the cache so subsequent lookups for the same query_id
            // are served from Redis. Critical during catch-up: the dispatcher
            // retries stale events that missed the cache window.
            cache.store(queryId, fromLogs.get());
        } else {
            LOOKUPS.labels("not_found").inc();
            log.debug("EventStore miss for queryId={} — query may not have arrived yet", queryId);
        }

        updateHitRate();
        return fromLogs;
    }

    /**
     * Store to both tiers.
     *
     * HBase write is durable and required for correctness (throws on failure).
     * Redis write is best-effort (logged but non-fatal — HBase is the source of truth).
     */
    @Override
    public void store(String queryId, byte[] eventBytes) {
        // HBase first — if this fails we haven't made any promises
        logs.store(queryId, eventBytes);
        // Redis second — best-effort cache warm
        cache.store(queryId, eventBytes);
    }

    @Override
    public void close() {
        cache.close();
        logs.close();
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private void updateHitRate() {
        if (totalLookups > 0) {
            CACHE_HIT_RATE.set((double) cacheHits / totalLookups);
        }
    }

    // ── Factory ───────────────────────────────────────────────────────────

    /**
     * Convenience factory that reads connection config from environment variables.
     *
     *   REDIS_HOST         (default: localhost)
     *   REDIS_PORT         (default: 6379)
     *   CACHE_TTL_SECONDS  (default: 300)
     *   ZK_QUORUM          (default: localhost)
     *   ZK_PORT            (default: 2181)
     */
    public static TieredEventStore fromEnv() throws Exception {
        String redisHost  = env("REDIS_HOST",        "localhost");
        int    redisPort  = Integer.parseInt(env("REDIS_PORT",  "6379"));
        int    cacheTtl   = Integer.parseInt(env("CACHE_TTL_SECONDS", "300"));
        String zkQuorum   = env("ZK_QUORUM",         "localhost");
        int    zkPort     = Integer.parseInt(env("ZK_PORT",     "2181"));

        CacheEventStore cache = new CacheEventStore(redisHost, redisPort, cacheTtl);
        LogsEventStore  logs  = new LogsEventStore(zkQuorum, zkPort);
        return new TieredEventStore(cache, logs);
    }

    private static String env(String key, String defaultValue) {
        return System.getenv().getOrDefault(key, defaultValue);
    }
}
