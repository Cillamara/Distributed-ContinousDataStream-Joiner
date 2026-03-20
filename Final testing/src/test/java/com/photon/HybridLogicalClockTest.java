package com.photon;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class HybridLogicalClockTest {

    @Test
    void timestampsAreMonotonicallyIncreasing() {
        HybridLogicalClock hlc = new HybridLogicalClock();
        long prev = hlc.now();
        for (int i = 0; i < 10_000; i++) {
            long next = hlc.now();
            assertTrue(next > prev, "HLC must be increasing");
            prev = next;
        }
    }

    @Test
    void updateAdvancesClockOnLaterReceived() {
        HybridLogicalClock hlc = new HybridLogicalClock();
        long local    = hlc.now();
        long farFuture = local + (1000L << 20); // 1 second ahead
        long updated  = hlc.update(farFuture);
        assertTrue(HybridLogicalClock.unpackTime(updated) >=
                HybridLogicalClock.unpackTime(farFuture));
    }

    @Test
    void eventIdsAreUnique() {
        var ids = new java.util.HashSet<Long>();
        for (int i = 0; i < 100_000; i++) {
            ids.add(EventId.generate().value);
        }
        assertEquals(100_000, ids.size(), "EIds aren't unique dumb ass");
    }

    @Test
    void shardAssignmentIsDeterministic() {
        EventId id = EventId.generate();
        int shards = 120;
        assertEquals(id.shard(shards), id.shard(shards),
                "Same event must always map to same shard");
    }
}