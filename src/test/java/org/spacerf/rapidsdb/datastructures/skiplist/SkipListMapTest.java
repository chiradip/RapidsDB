package org.spacerf.rapidsdb.datastructures.skiplist;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SkipListMapTest {

    SkipListMap<Integer, String> map = new SkipListMap<>();

    @Test
    void containsKey() {
        map.put(12, "MM");
        boolean res = map.containsKey(12);
        assertTrue(res);
        map.put(13, "MM");
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        for (Map.Entry<Integer, String> entry : entrySet) {
            if (entry == null) continue;
            int key = entry.getKey();
            String value = entry.getValue();
            System.out.println("key = " + key);
            System.out.println("value = " + value);
        }
    }

    @Test
    void containsValue() {
        map.put(12, "MM");
        boolean res = map.containsValue("MM");
        assertTrue(res);
        map.put(13, "NN");
        res = map.containsValue("NNM");
        assertFalse(res);
        res = map.containsValue(null); // null value is not allowed in the map
        assertFalse(res);
    }
    @Test
    void remove() {
        map.put(12, "MM");
        boolean res = map.containsKey(12);
        assertTrue(res);
        res = map.containsValue("MM");
        assertTrue(res);
        res = map.containsKey(13);
        assertFalse(res);
        res = map.containsValue("NN");
        assertFalse(res);
        map.put(13, "NN");
        res = map.containsKey(13);
        assertTrue(res);
        var result = map.get(13);
        map.remove(13);
        res = map.containsKey(13);
        assertFalse(res);
    }

    @Test
    void get() {
    }
}