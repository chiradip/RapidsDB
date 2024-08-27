package org.spacerf.rapidsdb.operations;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.spacerf.rapidsdb.datastructures.skiplist.SkipList;
import org.spacerf.rapidsdb.datastructures.sst.BloomFilter;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class SearchSSTTest {
    static SkipList<String, String> skipList = new SkipList<>();
    static int numRecords = 1000;
    static BloomFilter<String> bloomFilter = new BloomFilter<>((float)0.0001, numRecords);
    static String[][] data = {{"AAK", "AAV"}, {"ABK", "ABV"}, {"ACK", "ACV"},
            {"ADK", "ADV"}, {"AEK", "AEV"}, {"AFK", "AFV"}, {"AGK", "AGV"}, {"AHK", "AHV"}, {"AIK", "AIV"},
            {"AJK", "AJV"}, {"AKK", "AKV"}, {"ALK", "ALV"}, {"AMK", "AMV"}, {"ANK", "ANV"}, {"AOK", "AOV"},
            {"APK", "APV"}, {"AQK", "AQV"}, {"ARK", "ARV"}, {"ASK", "ASV"}, {"ATK", "ATV"}, {"AUK", "AUV"},
            {"AVK", "AVV"}, {"AWK", "AWV"}, {"AXK", "AXV"}, {"AYK", "AYV"}, {"AZK", "AZVVVVV"}};
    static String fileName = "qpwoei0192";
    static Map<String, String> kvs = new ConcurrentHashMap<>();

    @BeforeAll
    static void init() throws IOException {
        for (int i = 0; i < numRecords; i++) {
            String generatedKey = RandomStringUtils.random(3, true, true);
            String generatedValue = RandomStringUtils.random(7, true, false);
            var res = skipList.put(generatedKey, generatedValue);
            if (res != null) {
                kvs.computeIfPresent(generatedKey, (_,_) -> generatedValue);
                //System.out.println("res = " + res);
                //System.out.println("generatedValue = " + generatedValue);
            }
            bloomFilter.add(generatedKey);
            int random = new Random().nextInt(100);
            if (random != 0 && i % random == 0) kvs.put(generatedKey, generatedValue);
        }
        bloomFilter.getMetaInfo().serializeToFile(fileName + ".bloom");
        FlushSkipList<String, String> flushSkipList = new FlushSkipList<>(skipList, 12, 1.2f);
        flushSkipList.flush(fileName + ".sst");
    }
    @AfterAll
    static void cleanup() {
        File bloomFile = new File(fileName + ".bloom");
        File sstFile = new File(fileName + ".sst");
        File indexFile = new File(fileName + ".index");
        var bloomFileDeleted = bloomFile.delete();
        var sstFileDeleted = sstFile.delete();
        var indexFileDeleted = indexFile.delete();
        System.out.println("bloomFileDeleted = " + bloomFileDeleted);
        System.out.println("sstFileDeleted = " + sstFileDeleted);
        System.out.println("indexFileDeleted = " + indexFileDeleted);
    }

    @Test
    void checkBloomAndSkipListContent() {
        int falsePositiveCount = 0;
        for (String key: kvs.keySet()) {
            String value = skipList.get(key);
            assertEquals(kvs.get(key), value);
            boolean contains = bloomFilter.contains(key.getBytes(StandardCharsets.UTF_8));
            // this may fail - since it is probabilistic
            if (!contains) falsePositiveCount++;
            if (falsePositiveCount > 0) {
                System.out.println("contains = " + contains);
            }
            boolean doesNotContain = bloomFilter.contains("RANDOMSOMETHNG+++ooo".getBytes(StandardCharsets.UTF_8));
            assertFalse(doesNotContain);
        }
    }

    @Test
    void search() throws IOException, ClassNotFoundException {
        SearchSST<String> searchSST = new SearchSST<>(fileName + ".sst",
                fileName + ".bloom", fileName+".index");
        for (Map.Entry<String, String> pair: kvs.entrySet()) {
            System.out.println("pair.getKey() = " + pair.getKey());
            String value = searchSST.search(pair.getKey());
            assertEquals(pair.getValue(), value);
        }
    }
}