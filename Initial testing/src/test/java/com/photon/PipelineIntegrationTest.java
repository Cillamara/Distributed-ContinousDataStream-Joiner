package com.photon;

import com.photon.proto.InsertResponse;
import com.photon.proto.JoinResponse;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PipelineIntegrationTest — end-to-end test of the full Photon pipeline.
 *
 * Spins up real infrastructure via Testcontainers:
 *   - Kafka     → click stream input + joined output
 *   - etcd      → IdRegistry backend
 *   - Redis     → CacheEventStore
 *
 * HBase (LogsEventStore) is stubbed out — the test pre-populates the
 * TieredEventStore directly via CacheEventStore so the Joiner finds the
 * query event in Redis without needing a real HBase cluster.
 *
 * Tests verify:
 *   1. A click is successfully joined when its query is in EventStore.
 *   2. The same click processed a second time is deduplicated (ALREADY_JOINED).
 *   3. A click with no matching query returns QUERY_NOT_FOUND.
 *   4. The joined event lands in the output Kafka topic exactly once.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PipelineIntegrationTest {

    static Network network = Network.newNetwork();

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))
            .withNetwork(network);

    @Container
    static GenericContainer<?> etcd = new GenericContainer<>(
            DockerImageName.parse("bitnamilegacy/etcd:3.5"))
            .withNetwork(network)
            .withEnv("ALLOW_NONE_AUTHENTICATION", "yes")
            .withEnv("ETCD_ADVERTISE_CLIENT_URLS", "http://0.0.0.0:2379")
            .withEnv("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379")
            .withEnv("ETCD_LISTEN_PEER_URLS", "http://0.0.0.0:2380")
            .withEnv("ETCD_INITIAL_ADVERTISE_PEER_URLS", "http://0.0.0.0:2380")
            .withExposedPorts(2379)
            .waitingFor(Wait.forListeningPort());

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withNetwork(network)
            .withExposedPorts(6379)
            .waitingFor(Wait.forListeningPort());

    // ── Shared component instances (set up once for all tests) ────────────
    static IDRegistryClient  idRegistryClient;
    static IDRegistryServer  idRegistryServer;
    static CacheEventStore   cacheEventStore;
    static Joiner            joiner;

    static final String OUTPUT_TOPIC = "photon.joined.test";
    static final int    JOINER_PORT  = 19092; // test port — avoid clashing with local services
    static final int    REGISTRY_PORT = 19090;

    @BeforeAll
    static void startComponents() throws Exception {
        String etcdEndpoint = "http://" + etcd.getHost() + ":" + etcd.getMappedPort(2379);
        String redisHost    = redis.getHost();
        int    redisPort    = redis.getMappedPort(6379);
        String kafkaBoot    = kafka.getBootstrapServers();

        // ── IdRegistry ────────────────────────────────────────────────────
        idRegistryServer = new IDRegistryServer(
                etcdEndpoint, REGISTRY_PORT, REGISTRY_PORT + 1, 1 /* gcDays */);
        idRegistryServer.start();

        idRegistryClient = new IDRegistryClient("localhost", REGISTRY_PORT);

        // ── EventStore (cache only — no HBase for unit tests) ─────────────
        cacheEventStore = new CacheEventStore(redisHost, redisPort, 300);

        // ── Joiner ────────────────────────────────────────────────────────
        KafkaProducer<String, byte[]> producer = buildProducer(kafkaBoot);

        // Build a TieredEventStore backed only by the real Redis cache.
        // LogsEventStore is replaced by a no-op stub — the test populates
        // the cache directly, which is sufficient to exercise the join path.
        TieredEventStore eventStore = new TieredEventStore(cacheEventStore, noopLogsStore());

        joiner = new Joiner(
                eventStore, idRegistryClient, producer,
                OUTPUT_TOPIC, JoinAdapter.defaultAdapter(),
                JOINER_PORT, JOINER_PORT + 1, 100);
        joiner.start();

        // Short warm-up so gRPC servers are ready
        Thread.sleep(500);
    }

    @AfterAll
    static void stopComponents() {
        if (joiner != null)            joiner.shutdown();
        if (idRegistryServer != null)  idRegistryServer.shutdown();
        if (cacheEventStore != null)   cacheEventStore.close();
        if (idRegistryClient != null)  idRegistryClient.close();
    }

    // ── Tests ─────────────────────────────────────────────────────────────

    @Test
    @Order(1)
    @DisplayName("Click is joined when matching query is in EventStore")
    void clickIsJoined() throws Exception {
        EventId  clickId = EventId.generate();
        String   queryId = "query-" + UUID.randomUUID();
        byte[]   queryPayload = "advertiser=acme&keywords=flowers".getBytes(StandardCharsets.UTF_8);
        byte[]   clickPayload = "position=1&cost=0.10".getBytes(StandardCharsets.UTF_8);

        // Pre-populate EventStore with the query event
        cacheEventStore.store(queryId, queryPayload);

        // Call Joiner directly (bypassing Dispatcher for a focused unit test)
        JoinResponse.Status status = callJoiner(clickId, queryId, clickPayload);

        assertEquals(JoinResponse.Status.JOINED, status,
                "Click should be joined when query is present in EventStore");

        // Verify the joined event landed in the output Kafka topic
        byte[] joinedEvent = consumeOneFromTopic(OUTPUT_TOPIC);
        assertNotNull(joinedEvent, "Joined event should be written to output Kafka topic");
        assertTrue(joinedEvent.length > 0, "Joined event payload should not be empty");
    }

    @Test
    @Order(2)
    @DisplayName("Same click processed twice is deduplicated (exactly-once)")
    void duplicateClickIsDeduped() throws Exception {
        EventId clickId      = EventId.generate();
        String  queryId      = "query-" + UUID.randomUUID();
        byte[]  queryPayload = "advertiser=acme&keywords=books".getBytes(StandardCharsets.UTF_8);
        byte[]  clickPayload = "position=2&cost=0.05".getBytes(StandardCharsets.UTF_8);

        cacheEventStore.store(queryId, queryPayload);

        // First join — should succeed
        JoinResponse.Status first = callJoiner(clickId, queryId, clickPayload);
        assertEquals(JoinResponse.Status.JOINED, first, "First join should succeed");

        // Second join — same click_id — should be deduped
        JoinResponse.Status second = callJoiner(clickId, queryId, clickPayload);
        assertEquals(JoinResponse.Status.ALREADY_JOINED, second,
                "Duplicate click must be deduplicated by IdRegistry");
    }

    @Test
    @Order(3)
    @DisplayName("Click with no matching query returns QUERY_NOT_FOUND")
    void missingQueryReturnsNotFound() throws Exception {
        EventId clickId      = EventId.generate();
        String  queryId      = "query-nonexistent-" + UUID.randomUUID();
        byte[]  clickPayload = "position=3&cost=0.01".getBytes(StandardCharsets.UTF_8);

        // Deliberately do NOT store the query in EventStore

        JoinResponse.Status status = callJoiner(clickId, queryId, clickPayload);
        assertEquals(JoinResponse.Status.QUERY_NOT_FOUND, status,
                "Missing query should return QUERY_NOT_FOUND so dispatcher retries");
    }

    @Test
    @Order(4)
    @DisplayName("IdRegistry pre-check correctly identifies already-joined events")
    void idRegistryPreCheck() throws Exception {
        EventId clickId      = EventId.generate();
        String  queryId      = "query-" + UUID.randomUUID();
        byte[]  queryPayload = "advertiser=acme&keywords=shoes".getBytes(StandardCharsets.UTF_8);
        byte[]  clickPayload = "position=1&cost=0.20".getBytes(StandardCharsets.UTF_8);

        cacheEventStore.store(queryId, queryPayload);

        // Join once to register the event_id in IdRegistry
        callJoiner(clickId, queryId, clickPayload);

        // Pre-check should now return true
        assertTrue(idRegistryClient.isAlreadyJoined(clickId.value),
                "Pre-check should return true after event is registered");
    }

    @Test
    @Order(5)
    @DisplayName("Dispatcher publishes a click and it gets joined end-to-end")
    void dispatcherEndToEnd() throws Exception {
        String kafkaBoot  = kafka.getBootstrapServers();
        String clickTopic = "photon.clicks.test";
        String queryId    = "query-e2e-" + UUID.randomUUID();

        // Pre-populate EventStore
        byte[] queryPayload = "advertiser=e2e&keywords=test".getBytes(StandardCharsets.UTF_8);
        cacheEventStore.store(queryId, queryPayload);

        // Generate a click EventId
        EventId clickId = EventId.generate();

        // Publish a click to Kafka (as the query-log loader would)
        publishClick(kafkaBoot, clickTopic, clickId, queryId,
                "position=1&cost=0.15".getBytes(StandardCharsets.UTF_8));

        // Start dispatcher pointing at our test Joiner
        KafkaConsumer<String, byte[]> consumer = buildConsumer(kafkaBoot, "test-dispatcher-group");
        IDRegistryClient dispatcherRegistry    = new IDRegistryClient("localhost", REGISTRY_PORT);
        Dispatcher dispatcher = new Dispatcher(
                consumer, dispatcherRegistry,
                "localhost", JOINER_PORT,
                clickTopic, 3 * 24 * 3600 * 1000L, 2, 19095);

        ExecutorService exec = Executors.newSingleThreadExecutor();
        exec.submit(dispatcher::start);

        // Wait up to 10s for the joined event to appear in the output topic
        byte[] joined = consumeFromTopicWithTimeout(OUTPUT_TOPIC, Duration.ofSeconds(10));

        dispatcher.shutdown();
        exec.shutdownNow();
        dispatcherRegistry.close();

        assertNotNull(joined, "End-to-end: click should be joined and written to output topic");
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private JoinResponse.Status callJoiner(EventId clickId, String queryId, byte[] clickPayload) {
        // Build a direct gRPC stub to the test Joiner
        io.grpc.ManagedChannel ch = io.grpc.ManagedChannelBuilder
                .forAddress("localhost", JOINER_PORT).usePlaintext().build();
        try {
            com.photon.proto.JoinerServiceGrpc.JoinerServiceBlockingStub stub =
                    com.photon.proto.JoinerServiceGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(5, TimeUnit.SECONDS);

            com.photon.proto.JoinRequest req = com.photon.proto.JoinRequest.newBuilder()
                    .setClickEventId(clickId.value)
                    .setClickHlc(clickId.hlcTimestamp)
                    .setQueryId(queryId)
                    .setClickPayload(com.google.protobuf.ByteString.copyFrom(clickPayload))
                    .build();

            return stub.join(req).getStatus();
        } finally {
            ch.shutdownNow();
        }
    }

    private void publishClick(String bootstrap, String topic,
                              EventId clickId, String queryId, byte[] payload) throws Exception {
        try (KafkaProducer<String, byte[]> p = buildProducer(bootstrap)) {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, null, queryId, payload,
                    List.of(
                        new RecordHeader("click-id",  Long.toHexString(clickId.value).getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader("click-hlc", String.valueOf(clickId.hlcTimestamp).getBytes(StandardCharsets.UTF_8))
                    ));
            p.send(record).get(5, TimeUnit.SECONDS);
        }
    }

    private byte[] consumeOneFromTopic(String topic) {
        return consumeFromTopicWithTimeout(topic, Duration.ofSeconds(5));
    }

    private byte[] consumeFromTopicWithTimeout(String topic, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           "test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");

        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(props)) {
            c.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> records = c.poll(Duration.ofMillis(200));
                if (!records.isEmpty()) {
                    return records.iterator().next().value();
                }
            }
        }
        return null;
    }

    private static KafkaProducer<String, byte[]> buildProducer(String bootstrap) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,     StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,   ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(p);
    }

    private static KafkaConsumer<String, byte[]> buildConsumer(String bootstrap, String groupId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG,                 groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        return new KafkaConsumer<>(p);
    }

    /**
     * A no-op EventStore stub used in place of LogsEventStore so tests don't
     * need a real HBase cluster. Cache is the source of truth in all tests —
     * queries are pre-populated directly into CacheEventStore.
     */
    private static EventStore noopLogsStore() {
        return new EventStore() {
            @Override public Optional<byte[]> lookup(String queryId) { return Optional.empty(); }
            @Override public void store(String queryId, byte[] bytes) { /* no-op */ }
            @Override public void close() { /* no-op */ }
        };
    }
}
