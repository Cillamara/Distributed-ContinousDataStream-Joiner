package com.photon;

import com.photon.proto.IdRegistryServiceGrpc;
import com.photon.proto.InsertRequest;
import com.photon.proto.InsertResponse;
import com.photon.proto.LookupRequest;
import com.photon.proto.LookupResponse;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * IdRegistryClient — used by the Dispatcher and Joiner to talk to IdRegistryServer.
 *
 * Two operations, matching the paper (§3.2 and §3.3):
 *
 *   isAlreadyJoined(eventId)
 *     Dispatcher pre-check before sending an event to a Joiner.
 *     Hits the Lookup RPC. Fast path — skips ~95% of Joiner work in production.
 *
 *   register(eventId, hlcTimestamp)
 *     Joiner calls this after a successful EventStore lookup.
 *     Hits the Insert RPC.  Returns one of:
 *       SUCCESS        → joiner may write to the output log
 *       ALREADY_EXISTS → joiner must drop this event (another pipeline beat us)
 *       RETRY          → joiner previously registered but crashed before writing;
 *                        safe to re-write the output (same token detected)
 *       TOO_OLD        → event is past the GC window; treat as unjoinable
 *
 * Retry policy (Resilience4j):
 *   Retries on UNAVAILABLE (transient etcd / network blip) with exponential
 *   backoff up to MAX_ATTEMPTS.  Does NOT retry on ALREADY_EXISTS, RESOURCE_EXHAUSTED
 *   (queue full — caller should throttle), or any non-retryable status.
 */
public class IDRegistryClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(IDRegistryClient.class);

    // ── Retry config ──────────────────────────────────────────────────────
    private static final int      MAX_ATTEMPTS        = 5;
    private static final Duration INITIAL_WAIT        = Duration.ofMillis(50);
    private static final double   BACKOFF_MULTIPLIER  = 2.0;
    private static final Duration MAX_WAIT            = Duration.ofSeconds(2);
    private static final Duration RPC_DEADLINE        = Duration.ofSeconds(5);

    // ── State ─────────────────────────────────────────────────────────────
    private final ManagedChannel                                    channel;
    private final IdRegistryServiceGrpc.IdRegistryServiceBlockingStub stub;
    private final Retry                                             retryPolicy;

    /**
     * Identifies this Joiner instance: host:pid:startTime.
     * Stable for the lifetime of the process — used as the crash-recovery token
     * stored alongside each event_id in etcd (§3.3.2).
     */
    private final String token;

    // ── Constructor ───────────────────────────────────────────────────────

    public IDRegistryClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()   // TLS termination is handled at the infrastructure layer
                .build();

        this.stub = IdRegistryServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(RPC_DEADLINE.toMillis(), TimeUnit.MILLISECONDS);

        this.token = buildToken();

        RetryConfig config = RetryConfig.custom()
                .maxAttempts(MAX_ATTEMPTS)
                .waitDuration(INITIAL_WAIT)
                .intervalFunction(io.github.resilience4j.core.IntervalFunction
                        .ofExponentialRandomBackoff(INITIAL_WAIT, BACKOFF_MULTIPLIER, MAX_WAIT))
                .retryOnException(IDRegistryClient::isRetryable)
                .build();

        this.retryPolicy = RetryRegistry.of(config).retry("id-registry");

        this.retryPolicy.getEventPublisher()
                .onRetry(e -> log.warn("IdRegistry RPC retry #{} after: {}",
                        e.getNumberOfRetryAttempts(), e.getLastThrowable().getMessage()));

        log.info("IdRegistryClient created | server={}:{} | token={}", host, port, token);
    }

    // ── Public API ────────────────────────────────────────────────────────

    /**
     * Dispatcher pre-check: has this event already been joined by any pipeline?
     *
     * Returns true if the event_id exists in the registry (already joined).
     * A false result does NOT guarantee the event is safe to join — a concurrent
     * pipeline may be joining it right now. The definitive gate is register().
     */
    public boolean isAlreadyJoined(long eventId) {
        return Retry.decorateSupplier(retryPolicy, () -> {
            LookupResponse response = stub.lookup(
                    LookupRequest.newBuilder().setEventId(eventId).build());
            return response.getExists();
        }).get();
    }

    /**
     * Joiner gate: register this event_id as "being joined by this joiner".
     *
     * Must be called before writing to the output log. The returned status
     * tells the joiner exactly what to do next — see InsertResponse.Status docs.
     *
     * @param eventId      the packed EventId.value
     * @param hlcTimestamp the full HLC from EventId.hlcTimestamp (for age check);
     *                     pass 0 to skip the age check (e.g. during catch-up replay)
     */
    public InsertResponse.Status register(long eventId, long hlcTimestamp) {
        return Retry.decorateSupplier(retryPolicy, () -> {
            InsertResponse response = stub.insert(InsertRequest.newBuilder()
                    .setEventId(eventId)
                    .setToken(token)
                    .setHlcTimestamp(hlcTimestamp)
                    .build());
            return response.getStatus();
        }).get();
    }

    /**
     * Convenience overload — skips the age check.
     */
    public InsertResponse.Status register(long eventId) {
        return register(eventId, 0L);
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /**
     * Only UNAVAILABLE is retried — it indicates a transient network issue or
     * a restarting IdRegistryServer shard.
     *
     * RESOURCE_EXHAUSTED (queue full) is NOT retried here; the Dispatcher uses
     * its own higher-level backoff loop (exponential + jitter) when the registry
     * is overloaded, to avoid overwhelming it further.
     */
    private static boolean isRetryable(Throwable t) {
        if (t instanceof StatusRuntimeException sre) {
            return sre.getStatus().getCode() == Status.Code.UNAVAILABLE;
        }
        return false;
    }

    /**
     * Build a stable token for this JVM process: "host:pid:start_epoch_ms".
     *
     * Stability matters: if the joiner crashes and restarts, it gets a NEW token
     * (different start_epoch_ms), so it correctly learns ALREADY_EXISTS instead
     * of RETRY — preventing it from re-writing output for a click that was
     * already joined by the previous joiner instance.
     */
    private static String buildToken() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            host = "unknown";
        }
        return host + ":" + ProcessHandle.current().pid() + ":" + Instant.now().toEpochMilli();
    }
}