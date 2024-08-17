package org.spacerf.rapidsdb.datastructures.sst;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static java.lang.StrictMath.log;
import static java.lang.StrictMath.pow;
import static org.junit.jupiter.api.Assertions.*;

class BloomFilterTest {

    BloomFilter<String> bloomFilter = new BloomFilter<>((float)0.01, 100);
    @Test
    void createHash() {
        bloomFilter.add("AA".getBytes(StandardCharsets.UTF_8));
        boolean contains = bloomFilter.contains("AA".getBytes(StandardCharsets.UTF_8));
        assertTrue(contains);
        boolean doesNotContains = bloomFilter.contains("AAA".getBytes(StandardCharsets.UTF_8));
        assertFalse(doesNotContains);
    }
    
    @Test
    void metaInfo() {
        int m = bloomFilter.getMetaInfo().getM();
        int n = bloomFilter.getMetaInfo().getN();
        float e = bloomFilter.getMetaInfo().getE();
        int calculatedM = - (int) ( n * log(e) / pow(log(2), 2));
        assertEquals(m, calculatedM);
    }
}