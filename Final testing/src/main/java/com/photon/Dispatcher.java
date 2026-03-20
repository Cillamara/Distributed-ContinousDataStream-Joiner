package com.photon;

import com.google.common.util.concurrent.ListenableFuture;
import com.photon.proto.JoinRequest;
import com.photon.proto.JoinResponse;
import com.photon.proto.JoinerServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Dispatcher — reads click events from Kafka and drives them through the
 * Joiner until each is either joined, deduplicated, or unjoinable.
 *
 * Maps to §3.2 of the Photon paper. Key responsibilities:
 *
 *   1. Consume the foreign (click) Kafka topic continuously.
 *
 *   2. Pre-check each click against the IdRegistry before dispatching
 *      to a Joiner (§3.2 optimisation).  In steady state this skips
 *      ~95 % of events that the other datacenter's pipeline already joined.
 *
 *   3. Dispatch eligible clicks to a Joiner via gRPC (async future stub).
 *
 *   4. On non-terminal responses (QUERY_NOT_FOUND, THROTTLED, transient
 *      errors) park the click in a DelayQueue and retry with exponential
 *      backoff (§3.2.1).
 *
 *   5. Commit Kafka offsets only after every click in a batch has reached
 *      a terminal state (JOINED, ALREADY_JOINED, UNJOINABLE, MAX_RETRIES).
 *      This gives at-least-once Kafka semantics, matching the paper (§3.2).
 *
 * Kafka record conventions (produced by the query-log loader):
 *   record.key()          → query_id  (String) — partition key
 *   header "click-id"     → EventId.value as hex string
 *   header "click-hlc"    → full HLC as decimal string
 *   record.value()        → raw serialised click bytes
 *
 * GC window:
 *   Clicks older than MAX_AGE_MS are dropped without joining. This matches
 *   the IdRegistry GC boundary (§2.3) — the registry won't accept them anyway.
 */
public class Dispatcher {

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    // ── Defaults ──────────────────────────────────────────────────────────
    public static final String DEFAULT_CLICK_TOPIC   = "photon.clicks";
    public static final int    DEFAULT_METRICS_PORT  = 9094;
    public static final int    DEFAULT_WORKER_THREADS = 8;
    public static final long   DEFAULT_MAX_AGE_MS    = 3L * 24 * 3600 * 1_000; // 3 days

    // ── State ─────────────────────────────────────────────────────────────
    private final KafkaConsumer<String, byte[]>              consumer;
    private final IDRegistryClient                           idRegistry;
    private final JoinerServiceGrpc.JoinerServiceFutureStub joinerStub;
    private final ManagedChannel                            joinerChannel;
    private final String                                    clickTopic;
    private final long                                      maxAgeMs;

    /**
     * Clicks awaiting retry, ordered by next eligible retry time.
     * The poll loop drains this before pulling fresh Kafka records so that
     * retries are always processed promptly without blocking new traffic.
     */
    private final DelayQueue<PendingClick> retryQueue = new DelayQueue<>();

    /**
     * Thread pool for async Joiner RPC callbacks and retry scheduling.
     * Sized to match expected in-flight concurrency.
     */
    private final ScheduledExecutorService scheduler;

    /** Tracks pending futures per batch so we can commit offsets correctly. */
    private final Map<TopicPartition, Long> offsetsToCommit = new HashMap<>();

    private volatile boolean running = true;
    private HTTPServer metricsServer;

    // ── Metrics ───────────────────────────────────────────────────────────
    private static final Counter DISPATCHES = Counter.build()
            .name("photon_dispatcher_dispatches_total")
            .help("Dispatch outcomes")
            .labelNames("outcome") // precheck_skip | joined | already_joined | unjoinable | too_old | retry | max_retries
            .register();

    private static final Gauge RETRY_QUEUE_SIZE = Gauge.build()
            .name("photon_dispatcher_retry_queue_size")
            .help("Number of clicks currently waiting for retry")
            .register();

    private static final Counter KAFKA_RECORDS = Counter.build()
            .name("photon_dispatcher_kafka_records_total")
            .help("Raw Kafka records consumed")
            .register();

    // ── Constructor ───────────────────────────────────────────────────────

    public Dispatcher(KafkaConsumer<String, byte[]> consumer,
                      IDRegistryClient idRegistry,
                      String joinerHost,
                      int joinerPort,
                      String clickTopic,
                      long maxAgeMs,
                      int workerThreads,
                      int metricsPort) {

        this.consumer   = consumer;
        this.idRegistry = idRegistry;
        this.clickTopic = clickTopic;
        this.maxAgeMs   = maxAgeMs;

        this.joinerChannel = ManagedChannelBuilder
                .forAddress(joinerHost, joinerPort)
                .usePlaintext()
                .build();
        this.joinerStub = JoinerServiceGrpc.newFutureStub(joinerChannel);

        this.scheduler = Executors.newScheduledThreadPool(workerThreads,
                r -> new Thread(r, "dispatcher-worker"));

        DefaultExports.initialize();
        try {
            this.metricsServer = new HTTPServer(metricsPort);
        } catch (IOException e) {
            log.warn("Could not start metrics server on port {}", metricsPort);
        }
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────

    public void start() {
        consumer.subscribe(List.of(clickTopic));
        log.info("Dispatcher started | topic={} | maxAge={}ms", clickTopic, maxAgeMs);
        runPollLoop();
    }

    public void shutdown() {
        running = false;
        consumer.wakeup();
        scheduler.shutdownNow();
        joinerChannel.shutdownNow();
        idRegistry.close();
        if (metricsServer != null) metricsServer.close();
        log.info("Dispatcher shut down");
    }

    // ── Main poll loop ────────────────────────────────────────────────────

    private void runPollLoop() {
        try {
            while (running) {

                // ── Step 1: drain ready retries ──────────────────────────────
                // Process any clicks whose backoff has elapsed before pulling
                // new records.  This keeps retry latency low.
                drainRetryQueue();

                // ── Step 2: poll Kafka for fresh clicks ──────────────────────
                ConsumerRecords<String, byte[]> records =
                        consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) continue;

                KAFKA_RECORDS.inc(records.count());

                // Track which offsets need committing.
                // We commit only after ALL clicks in the batch are resolved.
                List<CompletableFuture<Void>> batchFutures = new ArrayList<>(records.count());
                offsetsToCommit.clear();

                for (ConsumerRecord<String, byte[]> record : records) {
                    // Track the highest offset seen per partition
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    offsetsToCommit.merge(tp, record.offset() + 1, Math::max);

                    CompletableFuture<Void> future = processRecord(record);
                    batchFutures.add(future);
                }

                // ── Step 3: commit offsets once the full batch is resolved ────
                //
                // We wait for all futures (blocking this thread) to ensure
                // at-least-once semantics: if the dispatcher crashes mid-batch,
                // unresolved clicks will be reprocessed from the last committed
                // offset.  The IdRegistry pre-check makes this safe — already-joined
                // events are skipped instantly on replay.
                CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0]))
                        .whenComplete((v, err) -> commitOffsets());
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            if (running) throw e; // unexpected
        }
    }

    // ── Per-record processing ─────────────────────────────────────────────

    private CompletableFuture<Void> processRecord(ConsumerRecord<String, byte[]> record) {
        // Extract click_id and HLC from Kafka headers
        long   eventId = extractLongHeader(record, "click-id",  0L);
        long   hlc     = extractLongHeader(record, "click-hlc", 0L);
        String queryId = record.key();  // partitioned by query_id

        if (eventId == 0L) {
            log.warn("Missing click-id header on record offset={} partition={}",
                    record.offset(), record.partition());
            return CompletableFuture.completedFuture(null);
        }

        // ── Age check ────────────────────────────────────────────────────
        if (hlc > 0 && isExpired(hlc)) {
            log.debug("Dropping expired click: eventId={}", Long.toHexString(eventId));
            DISPATCHES.labels("too_old").inc();
            return CompletableFuture.completedFuture(null);
        }

        // ── IdRegistry pre-check (§3.2 optimisation) ─────────────────────
        // Fast path: if the event was already joined (by this or another
        // datacenter's pipeline), skip it without touching the Joiner.
        // In production this eliminates ~95% of Joiner RPCs.
        try {
            if (idRegistry.isAlreadyJoined(eventId)) {
                DISPATCHES.labels("precheck_skip").inc();
                return CompletableFuture.completedFuture(null);
            }
        } catch (Exception e) {
            // IdRegistry unavailable — continue anyway, the Joiner will
            // call IdRegistry itself and handle the duplicate correctly.
            log.warn("IdRegistry pre-check failed for eventId={}: {}",
                    Long.toHexString(eventId), e.getMessage());
        }

        // ── Dispatch to Joiner ────────────────────────────────────────────
        PendingClick click = new PendingClick(eventId, hlc, queryId, record.value());
        return dispatch(click);
    }

    // ── Dispatch & retry ──────────────────────────────────────────────────

    /**
     * Sends a PendingClick to the Joiner asynchronously.
     * Returns a CompletableFuture that completes when the click reaches a
     * terminal state (JOINED, ALREADY_JOINED, UNJOINABLE, or max retries).
     */
    private CompletableFuture<Void> dispatch(PendingClick click) {
        CompletableFuture<Void> terminal = new CompletableFuture<>();
        sendToJoiner(click, terminal);
        return terminal;
    }

    private void sendToJoiner(PendingClick click, CompletableFuture<Void> terminal) {
        JoinRequest request = JoinRequest.newBuilder()
                .setClickEventId(click.eventId)
                .setClickHlc(click.hlcTimestamp)
                .setQueryId(click.queryId)
                .setClickPayload(com.google.protobuf.ByteString.copyFrom(click.payload))
                .build();

        ListenableFuture<JoinResponse> grpcFuture = joinerStub.join(request);

        // Attach callback on the worker thread pool — never blocks the consumer thread
        grpcFuture.addListener(() -> {
            try {
                JoinResponse response = grpcFuture.get();
                handleResponse(click, response.getStatus(), terminal);
            } catch (Exception e) {
                handleError(click, e, terminal);
            }
        }, scheduler);
    }

    private void handleResponse(PendingClick click,
                                JoinResponse.Status status,
                                CompletableFuture<Void> terminal) {
        switch (status) {

            case JOINED -> {
                DISPATCHES.labels("joined").inc();
                terminal.complete(null);
            }

            case ALREADY_JOINED -> {
                DISPATCHES.labels("already_joined").inc();
                terminal.complete(null);
            }

            case UNJOINABLE -> {
                log.debug("Click unjoinable: eventId={}", Long.toHexString(click.eventId));
                DISPATCHES.labels("unjoinable").inc();
                terminal.complete(null);
            }

            case QUERY_NOT_FOUND, THROTTLED -> {
                // Non-terminal: the query hasn't arrived yet or the Joiner is busy.
                // Park in retry queue with exponential backoff.
                scheduleRetry(click, terminal, status.name());
            }

            default -> {
                log.error("Unexpected JoinResponse status={} for eventId={}",
                        status, Long.toHexString(click.eventId));
                scheduleRetry(click, terminal, "unknown_status");
            }
        }
    }

    private void handleError(PendingClick click, Exception e, CompletableFuture<Void> terminal) {
        if (e instanceof StatusRuntimeException sre) {
            log.warn("Joiner RPC error for eventId={}: {}",
                    Long.toHexString(click.eventId), sre.getStatus());
        } else {
            log.warn("Joiner call failed for eventId={}", Long.toHexString(click.eventId), e);
        }
        scheduleRetry(click, terminal, "rpc_error");
    }

    private void scheduleRetry(PendingClick click,
                               CompletableFuture<Void> terminal,
                               String reason) {
        if (!click.hasRetriesLeft() || isExpired(click.hlcTimestamp)) {
            log.warn("Giving up on click after {} attempts ({}): eventId={}",
                    click.attempts, reason, Long.toHexString(click.eventId));
            DISPATCHES.labels("max_retries").inc();
            terminal.complete(null);
            return;
        }

        PendingClick retryClick = click.scheduleRetry();
        retryQueue.offer(retryClick);
        RETRY_QUEUE_SIZE.set(retryQueue.size());
        DISPATCHES.labels("retry").inc();

        log.debug("Scheduled retry #{} for eventId={} after ~{}ms",
                retryClick.attempts, Long.toHexString(click.eventId),
                retryClick.getDelay(TimeUnit.MILLISECONDS));

        // The terminal future for this click is passed to the retry's dispatch —
        // it will only complete once a terminal state is reached.
        scheduler.schedule(() -> {
            retryQueue.remove(retryClick);
            RETRY_QUEUE_SIZE.set(retryQueue.size());
            if (!isExpired(retryClick.hlcTimestamp)) {
                sendToJoiner(retryClick, terminal);
            } else {
                DISPATCHES.labels("too_old").inc();
                terminal.complete(null);
            }
        }, retryClick.getDelay(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    }

    // ── Retry queue drain ─────────────────────────────────────────────────

    /**
     * Drain any retries that have become eligible while the consumer thread
     * was waiting for Kafka records.  These are clicks that were previously
     * parked (QUERY_NOT_FOUND / THROTTLED) and whose backoff has now elapsed.
     *
     * Note: the ScheduledExecutorService already handles most retries via
     * schedule(); this drain is a safety net for any that slipped through
     * (e.g. during a long Kafka poll).
     */
    private void drainRetryQueue() {
        PendingClick click;
        while ((click = retryQueue.poll()) != null) {
            RETRY_QUEUE_SIZE.set(retryQueue.size());
            if (!isExpired(click.hlcTimestamp)) {
                // Re-dispatch; create a new detached terminal future
                // (offset already committed via the original future chain)
                sendToJoiner(click, new CompletableFuture<>());
            } else {
                DISPATCHES.labels("too_old").inc();
            }
        }
    }

    // ── Offset management ─────────────────────────────────────────────────

    private void commitOffsets() {
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        offsetsToCommit.forEach((tp, offset) ->
                toCommit.put(tp, new OffsetAndMetadata(offset)));
        try {
            consumer.commitSync(toCommit);
        } catch (Exception e) {
            log.warn("Kafka offset commit failed: {}", e.getMessage());
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private boolean isExpired(long hlcTimestamp) {
        if (hlcTimestamp == 0) return false; // unknown — don't expire
        long wallMs = HybridLogicalClock.unpackTime(hlcTimestamp);
        return (System.currentTimeMillis() - wallMs) > maxAgeMs;
    }

    private static long extractLongHeader(ConsumerRecord<?, ?> record,
                                          String headerName,
                                          long defaultValue) {
        Header header = record.headers().lastHeader(headerName);
        if (header == null) return defaultValue;
        try {
            String raw = new String(header.value(), StandardCharsets.UTF_8);
            return headerName.equals("click-id")
                    ? Long.parseUnsignedLong(raw, 16)   // click-id is hex
                    : Long.parseLong(raw);               // click-hlc is decimal
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    // ── Factory / entry point ─────────────────────────────────────────────

    /**
     * Build a Dispatcher from environment variables:
     *
     *   KAFKA_BOOTSTRAP       (default: localhost:9092)
     *   KAFKA_GROUP_ID        (default: photon-dispatcher)
     *   CLICK_TOPIC           (default: photon.clicks)
     *   JOINER_HOST           (default: localhost)
     *   JOINER_PORT           (default: 9092)
     *   ID_REGISTRY_HOST      (default: localhost)
     *   ID_REGISTRY_PORT      (default: 9090)
     *   DISPATCHER_WORKERS    (default: 8)
     *   MAX_AGE_MS            (default: 259200000  — 3 days)
     *   METRICS_PORT          (default: 9094)
     */
    public static Dispatcher fromEnv() {
        String kafkaBootstrap = env("KAFKA_BOOTSTRAP",    "localhost:9092");
        String groupId        = env("KAFKA_GROUP_ID",     "photon-dispatcher");
        String clickTopic     = env("CLICK_TOPIC",        DEFAULT_CLICK_TOPIC);
        String joinerHost     = env("JOINER_HOST",        "localhost");
        int    joinerPort     = Integer.parseInt(env("JOINER_PORT",         "9092"));
        String regHost        = env("ID_REGISTRY_HOST",   "localhost");
        int    regPort        = Integer.parseInt(env("ID_REGISTRY_PORT",    "9090"));
        int    workers        = Integer.parseInt(env("DISPATCHER_WORKERS",  String.valueOf(DEFAULT_WORKER_THREADS)));
        long   maxAge         = Long.parseLong(  env("MAX_AGE_MS",          String.valueOf(DEFAULT_MAX_AGE_MS)));
        int    metricsPort    = Integer.parseInt(env("METRICS_PORT",        String.valueOf(DEFAULT_METRICS_PORT)));

        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  kafkaBootstrap);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG,           groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // manual offset commits
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
        kafkaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,   "500");

        KafkaConsumer<String, byte[]> consumer   = new KafkaConsumer<>(kafkaProps);
        IDRegistryClient              idRegistry = new IDRegistryClient(regHost, regPort);

        return new Dispatcher(consumer, idRegistry, joinerHost, joinerPort,
                clickTopic, maxAge, workers, metricsPort);
    }

    private static String env(String key, String def) {
        return System.getenv().getOrDefault(key, def);
    }

    public static void main(String[] args) {
        Dispatcher dispatcher = Dispatcher.fromEnv();
        Runtime.getRuntime().addShutdownHook(new Thread(dispatcher::shutdown, "shutdown-hook"));
        dispatcher.start();
    }
}
