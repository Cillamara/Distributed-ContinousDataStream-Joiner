package com.photon;

import com.photon.proto.InsertResponse;
import com.photon.proto.JoinRequest;
import com.photon.proto.JoinResponse;
import com.photon.proto.JoinerServiceGrpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Joiner — stateless gRPC server that joins foreign events (clicks) with
 * primary events (queries).
 *
 * Maps to §3.3 of the Photon paper. The Joiner is intentionally stateless so
 * multiple instances can run behind a load balancer and any can handle any request.
 *
 * Processing pipeline for each incoming click (§3.3.1):
 *
 *   1. Throttle check   — reject if too many requests in flight (back-pressure)
 *   2. EventStore lookup — find the corresponding query by query_id
 *                          → QUERY_NOT_FOUND if missing (dispatcher retries later)
 *   3. Adapter combine   — merge click + query bytes via pluggable JoinAdapter
 *                          → null result means adapter filtered this click (UNJOINABLE)
 *   4. IdRegistry gate   — exactly-once check via conditional etcd insert
 *                          → ALREADY_EXISTS: drop (another pipeline beat us)
 *                          → RETRY: joiner previously registered, safe to re-write
 *                          → SUCCESS: proceed to write output
 *   5. Kafka write       — publish joined event to output topic
 *
 * The lack of state means failure recovery is simple: the dispatcher retries
 * with another joiner instance and the IdRegistry prevents double-joining.
 */
public class Joiner {

    private static final Logger log = LoggerFactory.getLogger(Joiner.class);

    // ── Defaults ──────────────────────────────────────────────────────────
    public static final int    DEFAULT_GRPC_PORT    = 9092;
    public static final int    DEFAULT_METRICS_PORT = 9093;
    public static final int    DEFAULT_MAX_IN_FLIGHT = 500;
    public static final String DEFAULT_OUTPUT_TOPIC  = "photon.joined";

    // ── State ─────────────────────────────────────────────────────────────
    private final TieredEventStore              eventStore;
    private final IDRegistryClient              idRegistry;
    private final KafkaProducer<String, byte[]> outputProducer;
    private final String                        outputTopic;
    private final JoinAdapter                   adapter;
    private final int                           maxInFlight;
    private final AtomicInteger                 inFlight = new AtomicInteger(0);
    private final Server                        grpcServer;
    private       HTTPServer                    metricsServer;

    // ── Metrics ───────────────────────────────────────────────────────────
    private static final Counter JOIN_OUTCOMES = Counter.build()
            .name("photon_joiner_outcomes_total")
            .help("Join outcomes by status")
            .labelNames("status") // joined | query_not_found | already_joined | unjoinable | throttled | error
            .register();

    private static final Histogram JOIN_LATENCY = Histogram.build()
            .name("photon_joiner_latency_seconds")
            .help("End-to-end join latency per request")
            .buckets(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0)
            .register();

    private static final Gauge IN_FLIGHT_GAUGE = Gauge.build()
            .name("photon_joiner_in_flight_requests")
            .help("Number of join requests currently being processed")
            .register();

    // ── Constructor ───────────────────────────────────────────────────────

    public Joiner(TieredEventStore eventStore,
                  IDRegistryClient idRegistry,
                  KafkaProducer<String, byte[]> outputProducer,
                  String outputTopic,
                  JoinAdapter adapter,
                  int grpcPort,
                  int metricsPort,
                  int maxInFlight) {

        this.eventStore     = eventStore;
        this.idRegistry     = idRegistry;
        this.outputProducer = outputProducer;
        this.outputTopic    = outputTopic;
        this.adapter        = adapter;
        this.maxInFlight    = maxInFlight;

        this.grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(new JoinerServiceImpl())
                .build();

        DefaultExports.initialize();
        try {
            this.metricsServer = new HTTPServer(metricsPort);
        } catch (IOException e) {
            log.warn("Could not start metrics server on port {}", metricsPort);
        }
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────

    public void start() throws IOException {
        grpcServer.start();
        log.info("Joiner started | port={} | maxInFlight={} | outputTopic={}",
                grpcServer.getPort(), maxInFlight, outputTopic);
    }

    public void awaitTermination() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    public void shutdown() {
        grpcServer.shutdownNow();
        outputProducer.close();
        eventStore.close();
        idRegistry.close();
        if (metricsServer != null) metricsServer.close();
        log.info("Joiner shut down");
    }

    // ── gRPC service ──────────────────────────────────────────────────────

    private class JoinerServiceImpl extends JoinerServiceGrpc.JoinerServiceImplBase {

        @Override
        public void join(JoinRequest request, StreamObserver<JoinResponse> observer) {
            Histogram.Timer timer = JOIN_LATENCY.startTimer();

            try {
                processJoin(request, observer);
            } catch (Exception e) {
                log.error("Unexpected error processing join for click_id={}",
                        Long.toHexString(request.getClickEventId()), e);
                JOIN_OUTCOMES.labels("error").inc();
                observer.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
            } finally {
                timer.observeDuration();
            }
        }
    }

    // ── Core join logic ───────────────────────────────────────────────────

    private void processJoin(JoinRequest request, StreamObserver<JoinResponse> observer) {

        // ── Step 1: Throttle ────────────────────────────────────────────────
        //
        // If too many requests are in-flight, reject immediately.
        // The dispatcher sees THROTTLED and applies exponential backoff,
        // preventing a slow EventStore or IdRegistry from cascading into
        // an unbounded queue on the joiner.
        int current = inFlight.incrementAndGet();
        IN_FLIGHT_GAUGE.set(current);

        if (current > maxInFlight) {
            inFlight.decrementAndGet();
            IN_FLIGHT_GAUGE.dec();
            log.warn("Joiner throttling: in_flight={} > max={}", current, maxInFlight);
            respond(observer, JoinResponse.Status.THROTTLED);
            JOIN_OUTCOMES.labels("throttled").inc();
            return;
        }

        try {
            doJoin(request, observer);
        } finally {
            inFlight.decrementAndGet();
            IN_FLIGHT_GAUGE.dec();
        }
    }

    private void doJoin(JoinRequest request, StreamObserver<JoinResponse> observer) {

        String queryId    = request.getQueryId();
        long   clickId    = request.getClickEventId();
        long   clickHlc   = request.getClickHlc();

        // ── Step 2: EventStore lookup ───────────────────────────────────────
        //
        // Find the primary event (query) corresponding to this click.
        // Tries Redis (CacheEventStore) first; falls back to HBase (LogsEventStore).
        // Returns QUERY_NOT_FOUND if the query hasn't arrived yet — the
        // dispatcher will retry after a backoff period (§3.2.1).
        Optional<byte[]> queryPayload = eventStore.lookup(queryId);

        if (queryPayload.isEmpty()) {
            log.debug("Query not found in EventStore: queryId={}, clickId={}",
                    queryId, Long.toHexString(clickId));
            respond(observer, JoinResponse.Status.QUERY_NOT_FOUND);
            JOIN_OUTCOMES.labels("query_not_found").inc();
            return;
        }

        // ── Step 3: Adapter combine ─────────────────────────────────────────
        //
        // Merge click + query via the pluggable JoinAdapter.
        // A null result means the adapter filtered this event out — e.g.
        // business logic determined the click is invalid (fraud, test traffic).
        byte[] joinedEvent = adapter.combine(queryId, request.getClickPayload().toByteArray(),
                queryPayload.get());

        if (joinedEvent == null) {
            log.debug("Adapter filtered click: queryId={}, clickId={}", queryId, Long.toHexString(clickId));
            respond(observer, JoinResponse.Status.UNJOINABLE);
            JOIN_OUTCOMES.labels("unjoinable").inc();
            return;
        }

        // ── Step 4: IdRegistry gate (exactly-once) ──────────────────────────
        //
        // Attempt to register this click_id. This is the critical exactly-once
        // gate — only one joiner across all datacenters will succeed.
        InsertResponse.Status regStatus = idRegistry.register(clickId, clickHlc);

        switch (regStatus) {

            case SUCCESS -> {
                // We own this click — write to output and confirm.
                writeToKafka(clickId, joinedEvent);
                respond(observer, JoinResponse.Status.JOINED);
                JOIN_OUTCOMES.labels("joined").inc();
            }

            case RETRY -> {
                // This joiner previously registered the click but crashed before
                // writing to Kafka. The same token is in etcd — safe to re-write.
                log.info("Retry detected for clickId={} — re-writing output", Long.toHexString(clickId));
                writeToKafka(clickId, joinedEvent);
                respond(observer, JoinResponse.Status.JOINED);
                JOIN_OUTCOMES.labels("joined").inc();
            }

            case ALREADY_EXISTS -> {
                // Another pipeline (or another joiner) already joined this click.
                respond(observer, JoinResponse.Status.ALREADY_JOINED);
                JOIN_OUTCOMES.labels("already_joined").inc();
            }

            case TOO_OLD -> {
                respond(observer, JoinResponse.Status.UNJOINABLE);
                JOIN_OUTCOMES.labels("unjoinable").inc();
            }

            default -> {
                log.error("Unknown IdRegistry status {} for clickId={}", regStatus, Long.toHexString(clickId));
                observer.onError(Status.INTERNAL.withDescription("Unknown registry status").asRuntimeException());
            }
        }
    }

    // ── Kafka output ──────────────────────────────────────────────────────

    /**
     * Write the joined event to the output Kafka topic.
     *
     * Uses click_id (hex) as the Kafka message key so that all events for the
     * same click land on the same partition — useful for downstream consumers
     * that need ordering guarantees per click.
     *
     * The send is synchronous (get()) to ensure the event is durably committed
     * before we respond SUCCESS to the dispatcher.  In high-throughput scenarios
     * this can be made async with a callback, but synchronous is safer here
     * because the dispatcher must know the write succeeded before it can stop
     * retrying.
     */
    private void writeToKafka(long clickId, byte[] joinedEvent) {
        String key = Long.toHexString(clickId);
        try {
            outputProducer.send(
                new ProducerRecord<>(outputTopic, key, joinedEvent)
            ).get(); // synchronous — ensures durability before responding
        } catch (Exception e) {
            log.error("Kafka write failed for clickId={}", key, e);
            // Propagate — the dispatcher will retry and the IdRegistry RETRY
            // mechanism will let us re-write without double-joining.
            throw new RuntimeException("Kafka write failed", e);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static void respond(StreamObserver<JoinResponse> observer, JoinResponse.Status status) {
        observer.onNext(JoinResponse.newBuilder().setStatus(status).build());
        observer.onCompleted();
    }

    // ── Factory / entry point ─────────────────────────────────────────────

    /**
     * Build a Joiner from environment variables:
     *
     *   ID_REGISTRY_HOST    (default: localhost)
     *   ID_REGISTRY_PORT    (default: 9090)
     *   KAFKA_BOOTSTRAP     (default: localhost:9092)
     *   OUTPUT_TOPIC        (default: photon.joined)
     *   GRPC_PORT           (default: 9092)  ← Joiner's own listen port
     *   METRICS_PORT        (default: 9093)
     *   MAX_IN_FLIGHT       (default: 500)
     *   REDIS_HOST / REDIS_PORT / CACHE_TTL_SECONDS / ZK_QUORUM / ZK_PORT
     *     (forwarded to TieredEventStore.fromEnv())
     */
    public static Joiner fromEnv() throws Exception {
        String registryHost = env("ID_REGISTRY_HOST", "localhost");
        int    registryPort = Integer.parseInt(env("ID_REGISTRY_PORT", "9090"));
        String kafkaBootstrap = env("KAFKA_BOOTSTRAP",  "localhost:9092");
        String outputTopic    = env("OUTPUT_TOPIC",     DEFAULT_OUTPUT_TOPIC);
        int    grpcPort       = Integer.parseInt(env("GRPC_PORT",     String.valueOf(DEFAULT_GRPC_PORT)));
        int    metricsPort    = Integer.parseInt(env("METRICS_PORT",  String.valueOf(DEFAULT_METRICS_PORT)));
        int    maxInFlight    = Integer.parseInt(env("MAX_IN_FLIGHT", String.valueOf(DEFAULT_MAX_IN_FLIGHT)));

        TieredEventStore eventStore = TieredEventStore.fromEnv();
        IDRegistryClient idRegistry = new IDRegistryClient(registryHost, registryPort);

        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  ByteArraySerializer.class.getName());
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "all");           // strongest durability guarantee
        kafkaProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // exactly-once Kafka producer

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(kafkaProps);

        return new Joiner(eventStore, idRegistry, producer, outputTopic,
                JoinAdapter.defaultAdapter(), grpcPort, metricsPort, maxInFlight);
    }

    private static String env(String key, String def) {
        return System.getenv().getOrDefault(key, def);
    }

    public static void main(String[] args) throws Exception {
        Joiner joiner = Joiner.fromEnv();
        joiner.start();
        Runtime.getRuntime().addShutdownHook(new Thread(joiner::shutdown, "shutdown-hook"));
        joiner.awaitTermination();
    }
}
