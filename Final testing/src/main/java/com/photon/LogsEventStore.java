package com.photon;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * LogsEventStore — HBase-backed cold path for EventStore lookups.
 *
 * Maps to §3.4.2 of the Photon paper ("LogsEventStore").
 *
 * In the paper, LogsEventStore uses a BigTable "log file map" that maps an
 * event_id to a (GFS file, byte offset), then does a sequential read of that
 * file to retrieve the event. In our open-source stack we use Kafka for the
 * log stream, so we instead store event payloads directly in HBase keyed by
 * query_id. This gives the same lookup semantics with a simpler architecture.
 *
 * HBase schema:
 *   Table:        photon_queries
 *   Row key:      query_id  (UTF-8 string — consistent with how events are indexed)
 *   Column family: e        (short name keeps storage overhead low)
 *   Columns:
 *     e:payload  — raw event bytes (serialised however the producer chooses)
 *     e:ts       — wall-clock ms at write time (used for TTL auditing / GC scans)
 *
 * HBase TTL:
 *   Set the column family TTL to match your GC window (e.g. 3 days = 259200 s):
 *
 *     hbase shell> alter 'photon_queries', {NAME => 'e', TTL => 259200}
 *
 *   HBase will automatically evict rows older than TTL during compaction,
 *   replacing the GC background thread described in the paper (§2.3).
 */
public class LogsEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(LogsEventStore.class);

    // ── HBase schema constants ─────────────────────────────────────────────
    static final TableName TABLE        = TableName.valueOf("photon_queries");
    static final byte[]    CF           = Bytes.toBytes("e");
    static final byte[]    COL_PAYLOAD  = Bytes.toBytes("payload");
    static final byte[]    COL_TS       = Bytes.toBytes("ts");

    // ── State ─────────────────────────────────────────────────────────────
    private final Connection hbaseConnection;

    // ── Metrics ───────────────────────────────────────────────────────────
    private static final Counter LOOKUPS = Counter.build()
            .name("photon_logs_event_store_lookups_total")
            .help("LogsEventStore lookup results")
            .labelNames("result") // found | not_found | error
            .register();

    private static final Histogram LOOKUP_LATENCY = Histogram.build()
            .name("photon_logs_event_store_lookup_latency_seconds")
            .help("LogsEventStore lookup latency (expected to be slower than cache)")
            .buckets(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0)
            .register();

    private static final Counter STORES = Counter.build()
            .name("photon_logs_event_store_stores_total")
            .help("LogsEventStore store attempts")
            .labelNames("status")
            .register();

    // ── Constructor ───────────────────────────────────────────────────────

    /**
     * @param zookeeperQuorum  ZooKeeper quorum string, e.g. "localhost" or
     *                         "zk1,zk2,zk3" for a cluster
     * @param zookeeperPort    ZooKeeper client port (default 2181)
     */
    public LogsEventStore(String zookeeperQuorum, int zookeeperPort) throws IOException {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",        zookeeperQuorum);
        conf.set("hbase.zookeeper.property.clientPort", String.valueOf(zookeeperPort));

        this.hbaseConnection = ConnectionFactory.createConnection(conf);
        ensureTableExists();

        log.info("LogsEventStore connected | zk={}:{}", zookeeperQuorum, zookeeperPort);
    }

    // ── EventStore ────────────────────────────────────────────────────────

    @Override
    public Optional<byte[]> lookup(String queryId) {
        Histogram.Timer timer = LOOKUP_LATENCY.startTimer();
        try (Table table = hbaseConnection.getTable(TABLE)) {

            Get get = new Get(Bytes.toBytes(queryId))
                    .addColumn(CF, COL_PAYLOAD);

            Result result = table.get(get);

            if (result.isEmpty()) {
                LOOKUPS.labels("not_found").inc();
                return Optional.empty();
            }

            byte[] payload = result.getValue(CF, COL_PAYLOAD);
            LOOKUPS.labels("found").inc();
            return Optional.ofNullable(payload);

        } catch (IOException e) {
            log.error("LogsEventStore lookup failed for queryId={}", queryId, e);
            LOOKUPS.labels("error").inc();
            return Optional.empty();
        } finally {
            timer.observeDuration();
        }
    }

    @Override
    public void store(String queryId, byte[] eventBytes) {
        try (Table table = hbaseConnection.getTable(TABLE)) {

            long nowMs = System.currentTimeMillis();

            Put put = new Put(Bytes.toBytes(queryId))
                    .addColumn(CF, COL_PAYLOAD, eventBytes)
                    .addColumn(CF, COL_TS,      Bytes.toBytes(nowMs));

            table.put(put);
            STORES.labels("ok").inc();

        } catch (IOException e) {
            log.error("LogsEventStore store failed for queryId={}", queryId, e);
            STORES.labels("error").inc();
            // Propagate — if we can't persist the event, the Joiner won't be
            // able to look it up later and clicks will become unjoinable.
            throw new RuntimeException("LogsEventStore store failed", e);
        }
    }

    @Override
    public void close() {
        try {
            hbaseConnection.close();
            log.info("LogsEventStore closed");
        } catch (IOException e) {
            log.warn("Error closing HBase connection", e);
        }
    }

    // ── Admin ─────────────────────────────────────────────────────────────

    /**
     * Creates the photon_queries table if it doesn't already exist.
     * In production, table creation should be managed via HBase shell or
     * an ops runbook — this is provided for dev/test convenience.
     */
    private void ensureTableExists() throws IOException {
        try (Admin admin = hbaseConnection.getAdmin()) {
            if (admin.tableExists(TABLE)) return;

            log.info("Creating HBase table {}", TABLE.getNameAsString());

            ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder
                    .newBuilder(CF)
                    .setMaxVersions(1)            // we only care about the latest payload
                    .setCompressionType(org.apache.hadoop.hbase.io.compress.Compression.Algorithm.SNAPPY)
                    .build();

            TableDescriptor descriptor = TableDescriptorBuilder
                    .newBuilder(TABLE)
                    .setColumnFamily(cf)
                    .build();

            admin.createTable(descriptor);
            log.info("Table {} created — remember to set TTL via HBase shell: " +
                    "alter '{}', {{NAME => 'e', TTL => <seconds>}}",
                    TABLE.getNameAsString(), TABLE.getNameAsString());
        }
    }
}
