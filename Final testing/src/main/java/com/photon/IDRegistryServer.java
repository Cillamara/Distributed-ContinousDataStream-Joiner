package com.photon;

import com.photon.proto.IdRegistryServiceGrpc;
import com.photon.proto.InsertRequest;
import com.photon.proto.InsertResponse;
import com.photon.proto.LookupRequest;
import com.photon.proto.LookupResponse;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.PutOption;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * IdRegistryServer — the exactly-once gate for the open-source Photon pipeline.
 *
 * Maps directly to §2 of the Photon paper. The key design points:
 *
 *  1. Storage backend: etcd (open-source PaxosDB equivalent).
 *     Each event_id is stored as an etcd key with a TTL lease (GC mechanism).
 *
 *  2. Conditional insert: etcd transaction "IF key absent THEN PUT".
 *     This is the read-modify-write primitive that makes exactly-once work.
 *
 *  3. Server-side batching (§2.2.1): a single registry thread drains an
 *     in-memory queue and fans out concurrent etcd transactions, amortising
 *     network latency across all callers without sacrificing per-key isolation.
 *
 *  4. Within-batch deduplication: if the same event_id appears multiple times
 *     in one batch, only the first is sent to etcd; the rest get ALREADY_EXISTS
 *     immediately (application-level conflict resolution, §2.2.1).
 *
 *  5. Crash-recovery token (§3.3.2): each insert stores a "joiner token"
 *     (host:pid:hlc) as the etcd value. If a joiner crashes after registering
 *     but before writing output, it can retry with the same token and get
 *     RETRY back, confirming it's safe to re-write without double-charging.
 *
 *  6. GC: etcd key TTL leases (GC_TTL_SECONDS) replace the background GC
 *     thread in the paper. Keys auto-expire; a single shared lease is renewed
 *     periodically rather than tracking individual key ages.
 */
public class IDRegistryServer {

    private static final Logger log = LoggerFactory.getLogger(IDRegistryServer.class);

    // ── Defaults (all overridable via constructor) ─────────────────────────
    public static final int    DEFAULT_GRPC_PORT     = 9090;
    public static final int    DEFAULT_METRICS_PORT  = 9091;
    public static final int    DEFAULT_GC_TTL_DAYS   = 3;
    public static final int    BATCH_MAX             = 500;
    public static final long   BATCH_LINGER_MS       = 1;        // wait up to 1ms for more items
    public static final int    QUEUE_CAPACITY        = 10_000;

    private static final String ETCD_KEY_PREFIX = "/photon/ids/";

    // ── Infrastructure ─────────────────────────────────────────────────────
    private final KV    kv;
    private final Lease leaseClient;
    private final long  gcTtlSeconds;

    /**
     * Shared etcd lease. All event_id keys are associated with this lease so
     * they auto-expire after GC_TTL_SECONDS without a separate GC thread.
     * The keep-alive thread renews it before it times out.
     */
    private volatile long             activeLeaseId = -1;
    private          io.etcd.jetcd.support.CloseableClient leaseKeepAlive;

    private final BlockingQueue<PendingInsert> insertQueue;
    private final ExecutorService              registryThread;
    private final Server                       grpcServer;
    private       HTTPServer                   metricsServer;

    // ── Prometheus metrics ─────────────────────────────────────────────────
    private static final Counter INSERTS = Counter.build()
            .name("photon_idregistry_inserts_total")
            .help("Insert attempts, labelled by outcome")
            .labelNames("status")
            .register();

    private static final Histogram BATCH_SIZES = Histogram.build()
            .name("photon_idregistry_batch_size_histogram")
            .help("How many requests the registry thread processes per batch")
            .buckets(1, 5, 10, 50, 100, 250, 500)
            .register();

    private static final Counter LOOKUPS = Counter.build()
            .name("photon_idregistry_lookups_total")
            .help("Lookup requests, labelled by outcome")
            .labelNames("result")
            .register();

    // ── Inner record ──────────────────────────────────────────────────────
    private record PendingInsert(InsertRequest request, StreamObserver<InsertResponse> observer) {}

    // ── Constructor ───────────────────────────────────────────────────────

    public IDRegistryServer(String etcdEndpoint, int grpcPort, int metricsPort, int gcTtlDays) {
        Client etcd = Client.builder().endpoints(etcdEndpoint).build();
        this.kv           = etcd.getKVClient();
        this.leaseClient  = etcd.getLeaseClient();
        this.gcTtlSeconds = (long) gcTtlDays * 86_400L;

        this.insertQueue   = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        this.registryThread = Executors.newSingleThreadExecutor(
                r -> new Thread(r, "registry-thread"));

        this.grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(new IdRegistryServiceImpl())
                .build();

        DefaultExports.initialize();
        try {
            this.metricsServer = new HTTPServer(metricsPort);
        } catch (IOException e) {
            log.warn("Could not start Prometheus metrics server on port {}: {}", metricsPort, e.getMessage());
        }
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────

    public void start() throws IOException, ExecutionException, InterruptedException {
        // Obtain a shared etcd lease. All keys written by this server shard
        // will use this lease, so they all expire at the GC boundary automatically.
        activeLeaseId  = leaseClient.grant(gcTtlSeconds).get().getID();
        leaseKeepAlive = leaseClient.keepAlive(activeLeaseId, new StreamObserver<>() {
            public void onNext(LeaseKeepAliveResponse r) { /* renewed */ }
            public void onError(Throwable t)  { log.error("Lease keep-alive failed — keys may expire early", t); }
            public void onCompleted()         { log.warn("Lease keep-alive stream completed"); }
        });

        registryThread.submit(this::registryLoop);
        grpcServer.start();
        log.info("IdRegistryServer started | port={} | lease={} | gc={}d",
                grpcServer.getPort(), activeLeaseId, gcTtlSeconds / 86_400);
    }

    public void awaitTermination() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    public void shutdown() {
        if (leaseKeepAlive != null) leaseKeepAlive.close();
        grpcServer.shutdownNow();
        registryThread.shutdownNow();
        if (metricsServer != null) metricsServer.close();
        log.info("IdRegistryServer shut down");
    }

    // ── Registry thread ───────────────────────────────────────────────────
    //
    // This is the heart of server-side batching (§2.2.1).
    //
    // The thread blocks on the queue until at least one insert arrives, then
    // drains up to BATCH_MAX items (with a short linger to collect stragglers).
    // It deduplicates within the batch at the application layer before touching
    // etcd, then fans out concurrent conditional-insert transactions to etcd
    // and waits for all of them to complete before pulling the next batch.

    private void registryLoop() {
        log.info("Registry thread started");
        final List<PendingInsert> batch = new ArrayList<>(BATCH_MAX);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                batch.clear();

                // Block until at least one item is available
                PendingInsert head = insertQueue.poll(100, TimeUnit.MILLISECONDS);
                if (head == null) continue;
                batch.add(head);

                // Drain more items up to BATCH_MAX with a short linger window
                // (BATCH_LINGER_MS) so that bursts of requests collapse into a
                // single etcd round-trip rather than many.
                long deadline = System.currentTimeMillis() + BATCH_LINGER_MS;
                while (batch.size() < BATCH_MAX) {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) break;
                    PendingInsert next = insertQueue.poll(remaining, TimeUnit.MILLISECONDS);
                    if (next == null) break;
                    batch.add(next);
                }

                BATCH_SIZES.observe(batch.size());
                processBatch(batch);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Unexpected error in registry thread", e);
                // Drain observers so callers aren't left hanging
                for (PendingInsert p : batch) {
                    safeError(p.observer(), Status.INTERNAL, e.getMessage());
                }
            }
        }
        log.info("Registry thread stopped");
    }

    private void processBatch(List<PendingInsert> batch) {
        // ── Step 1: within-batch deduplication ────────────────────────────
        //
        // If the same event_id appears twice in one batch (e.g., two dispatchers
        // retrying the same click simultaneously), only the first goes to etcd.
        // All others receive ALREADY_EXISTS immediately — no etcd round-trip.
        // This is "application-level conflict resolution" from the paper (§2.2.1).

        Map<Long, PendingInsert> winners = new LinkedHashMap<>(batch.size());
        List<PendingInsert>      losers  = new ArrayList<>();

        for (PendingInsert p : batch) {
            long id = p.request().getEventId();
            if (winners.putIfAbsent(id, p) != null) {
                losers.add(p);
            }
        }

        for (PendingInsert loser : losers) {
            respond(loser.observer(), InsertResponse.Status.ALREADY_EXISTS);
            INSERTS.labels("batch_duplicate").inc();
        }

        // ── Step 2: age check ─────────────────────────────────────────────
        //
        // Reject events older than the GC window before touching etcd.
        // Uses the full HLC timestamp in the request (0 = skip check).

        List<PendingInsert> live    = new ArrayList<>(winners.size());
        long                nowMs   = System.currentTimeMillis();

        for (PendingInsert p : winners.values()) {
            long hlc = p.request().getHlcTimestamp();
            if (hlc > 0) {
                long ageMs = nowMs - HybridLogicalClock.unpackTime(hlc);
                if (ageMs > gcTtlSeconds * 1_000L) {
                    respond(p.observer(), InsertResponse.Status.TOO_OLD);
                    INSERTS.labels("too_old").inc();
                    continue;
                }
            }
            live.add(p);
        }

        if (live.isEmpty()) return;

        // ── Step 3: fan-out concurrent conditional inserts to etcd ────────
        //
        // Each event_id is independent, so we issue all etcd transactions in
        // parallel (CompletableFutures) and then wait for all results.
        // This is what "batching" buys: N round-trips overlapping in time
        // instead of N sequential round-trips.

        List<CompletableFuture<Void>> futures = new ArrayList<>(live.size());
        for (PendingInsert p : live) {
            futures.add(conditionalInsert(p));
        }

        for (CompletableFuture<Void> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                // Individual errors are handled inside conditionalInsert;
                // we just ensure the future is resolved so the thread advances.
                log.debug("etcd future completed with error (handled inline)", e);
            }
        }
    }

    // ── Etcd operations ───────────────────────────────────────────────────

    /**
     * Executes: IF key(event_id) absent THEN PUT key=token WITH lease.
     *
     * On success  → SUCCESS   (joiner may write output log)
     * On conflict → triggers checkRetry to distinguish ALREADY_EXISTS vs RETRY
     */
    private CompletableFuture<Void> conditionalInsert(PendingInsert pending) {
        long           eventId = pending.request().getEventId();
        ByteSequence   key     = etcdKey(eventId);
        ByteSequence   value   = ByteSequence.from(pending.request().getToken(), StandardCharsets.UTF_8);
        PutOption      putOpt  = PutOption.builder().withLeaseId(activeLeaseId).build();

        return kv.txn()
                .If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0))) // key must not exist
                .Then(Op.put(key, value, putOpt))
                .commit()
                .thenCompose(txnResponse -> {
                    if (txnResponse.isSucceeded()) {
                        respond(pending.observer(), InsertResponse.Status.SUCCESS);
                        INSERTS.labels("success").inc();
                        return CompletableFuture.completedFuture(null);
                    } else {
                        // Key already exists — may be a genuine duplicate or a joiner retry
                        return checkRetry(key, pending);
                    }
                })
                .exceptionally(ex -> {
                    log.error("etcd txn failed for event_id={}", Long.toHexString(eventId), ex);
                    safeError(pending.observer(), Status.UNAVAILABLE, "etcd unavailable: " + ex.getMessage());
                    return null;
                });
    }

    /**
     * The conditional insert found the key already present.
     *
     * Paper §3.3.2: a joiner stores its token (host:pid:hlc) as the etcd value
     * alongside the event_id.  If the same joiner retries after a crash, the
     * stored token matches and we return RETRY so it can safely re-write output.
     * A different joiner (or the same event processed by a second datacenter
     * pipeline) gets ALREADY_EXISTS and must drop the event.
     */
    private CompletableFuture<Void> checkRetry(ByteSequence key, PendingInsert pending) {
        return kv.get(key)
                .thenAccept(getResponse -> {
                    if (getResponse.getKvs().isEmpty()) {
                        // Key disappeared between our txn and GET — it was GC'd, so
                        // the event is older than the retention window. Treat as TOO_OLD.
                        respond(pending.observer(), InsertResponse.Status.TOO_OLD);
                        INSERTS.labels("gc_race").inc();
                        return;
                    }
                    String storedToken  = getResponse.getKvs().get(0).getValue()
                            .toString(StandardCharsets.UTF_8);
                    String requestToken = pending.request().getToken();

                    if (storedToken.equals(requestToken)) {
                        // Same joiner is retrying — it's safe to re-write output
                        respond(pending.observer(), InsertResponse.Status.RETRY);
                        INSERTS.labels("retry").inc();
                    } else {
                        respond(pending.observer(), InsertResponse.Status.ALREADY_EXISTS);
                        INSERTS.labels("duplicate").inc();
                    }
                })
                .exceptionally(ex -> {
                    log.error("etcd GET failed during retry check", ex);
                    safeError(pending.observer(), Status.UNAVAILABLE, "etcd unavailable");
                    return null;
                });
    }

    // ── gRPC service implementation ───────────────────────────────────────

    private class IdRegistryServiceImpl extends IdRegistryServiceGrpc.IdRegistryServiceImplBase {

        /**
         * Lookup: synchronous etcd GET, bypasses the insert queue.
         *
         * Used by the Dispatcher as a pre-check before sending a click to a Joiner
         * (§3.2) — this single optimisation eliminates ~95% of Joiner work in
         * production (most clicks have already been joined in the other datacenter).
         */
        @Override
        public void lookup(LookupRequest request, StreamObserver<LookupResponse> observer) {
            ByteSequence key = etcdKey(request.getEventId());
            kv.get(key)
                    .thenAccept(response -> {
                        boolean exists = !response.getKvs().isEmpty();
                        observer.onNext(LookupResponse.newBuilder().setExists(exists).build());
                        observer.onCompleted();
                        LOOKUPS.labels(exists ? "hit" : "miss").inc();
                    })
                    .exceptionally(ex -> {
                        safeError(observer, Status.UNAVAILABLE, ex.getMessage());
                        return null;
                    });
        }

        /**
         * Insert: enqueues the request onto the registry thread's queue.
         *
         * The queue provides back-pressure: if the registry thread is behind,
         * callers get RESOURCE_EXHAUSTED and must retry with backoff (Resilience4j
         * handles this on the client side).
         */
        @Override
        public void insert(InsertRequest request, StreamObserver<InsertResponse> observer) {
            boolean queued = insertQueue.offer(new PendingInsert(request, observer));
            if (!queued) {
                // Queue full — signal caller to retry later (dispatcher will back off)
                safeError(observer, Status.RESOURCE_EXHAUSTED,
                        "IdRegistry insert queue full — retry with backoff");
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static ByteSequence etcdKey(long eventId) {
        return ByteSequence.from(
                ETCD_KEY_PREFIX + Long.toHexString(eventId),
                StandardCharsets.UTF_8);
    }

    private static void respond(StreamObserver<InsertResponse> observer,
                                InsertResponse.Status status) {
        observer.onNext(InsertResponse.newBuilder().setStatus(status).build());
        observer.onCompleted();
    }

    private static void safeError(StreamObserver<?> observer,
                                  Status grpcStatus, String message) {
        try {
            observer.onError(grpcStatus.withDescription(message).asRuntimeException());
        } catch (Exception ignored) {
            // observer may already be closed
        }
    }

    // ── Entry point ───────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        String etcdEndpoint = System.getenv().getOrDefault("ETCD_ENDPOINT", "http://localhost:2379");
        int    grpcPort     = Integer.parseInt(System.getenv().getOrDefault("GRPC_PORT",    String.valueOf(DEFAULT_GRPC_PORT)));
        int    metricsPort  = Integer.parseInt(System.getenv().getOrDefault("METRICS_PORT", String.valueOf(DEFAULT_METRICS_PORT)));
        int    gcDays       = Integer.parseInt(System.getenv().getOrDefault("GC_TTL_DAYS",  String.valueOf(DEFAULT_GC_TTL_DAYS)));

        IDRegistryServer server = new IDRegistryServer(etcdEndpoint, grpcPort, metricsPort, gcDays);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown, "shutdown-hook"));
        server.awaitTermination();
    }
}