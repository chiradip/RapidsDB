package org.spacerf.rapidsdb.operations;

import org.junit.jupiter.api.Test;
import org.spacerf.rapidsdb.datastructures.skiplist.SkipList;

import java.io.IOException;
import java.util.Map;

class FlushSkipListTest {
    SkipList<String, String> skipList = new SkipList<>();
    SkipList<String, Map<String, String>> skipList2 = new SkipList<>();
    @Test
    void flush() throws IOException {
        skipList.put("AA","ZZ");
        skipList.put("AB","ZX");
        skipList.put("AC","ZY");
        skipList.put("AD","ZW");
        FlushSkipList<String, String> flushSkipList = new FlushSkipList<>(skipList, 6, 1.2f);
        flushSkipList.flush("second.sst");
        skipList2.put("AA1", Map.of("A1", "B1"));
        skipList2.put("AA2", Map.of("A2", "B2"));
        skipList2.put("AA2", Map.of("A3", "B3"));
        FlushSkipList<String, Map<String, String>> flushSkipList2 = new FlushSkipList<>(skipList2, 6, 1.2f);
        flushSkipList2.flush("third.sst");
    }
}