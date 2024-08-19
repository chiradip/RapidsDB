package org.spacerf.rapidsdb.datastructures.skiplist;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class SkipListTwoTest {

    SkipList<String, String> skipList  = new SkipList<>();
    Map<String, String> kvs = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() {
        for (int i = 0; i < 1000000; i++) {
            String generatedKey = RandomStringUtils.random(3, true, true);
            String generatedValue = RandomStringUtils.random(7, true, false);
            var res = skipList.put(generatedKey, generatedValue);
            if (res != null) {
                kvs.computeIfPresent(generatedKey, (_,_) -> generatedValue);
            }
            int random = new Random().nextInt(100);
            if (random != 0 && i % random == 0) kvs.put(generatedKey, generatedValue);
        }
    }

    @Test
    void search() {
        System.out.println("kvs.size() = " + kvs.size());
        for (String key : kvs.keySet()) {
            String value = skipList.get(key);
            String expectedValue = kvs.get(key);
            assertEquals(expectedValue, value);
        }
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void prettyPrint() {
        //SkipListPrintUtils<String, String> S = new SkipListPrintUtils<>(skipList);
        //S.printVertical();
    }
}