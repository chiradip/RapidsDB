package org.spacerf.rapidsdb.datastructures.skiplist;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SkipListTest {
    SkipList<String, Integer> skipList  = new SkipList<>();
    SkipListPrintUtils<String, Integer> S = new SkipListPrintUtils<>(skipList);
    @Test
    void emptyRemove() {
        Integer res = skipList.remove("ABC");
        assertNull(res);
    }
    
    @Test
    void singleValueAddRemove() {
        Integer res = skipList.put("ABC", 987);
        assertNull(res);
        int size = skipList.size();
        assertEquals(1, size);
        res = skipList.remove("ABC");
        assertEquals(987, res);
        size = skipList.size();
        assertEquals(0, size);
    }
    @Test
    void get() {
        S.printHorizontal();
        System.out.println("------");

        skipList.put("ABC", 123);
        S.printHorizontal();
        System.out.println("------");

        skipList.put("DEF", 123);
        S.printHorizontal();
        System.out.println("------");

        skipList.put("KLM", 123);
        S.printHorizontal();
        System.out.println("------");

        skipList.put("HIJ", 123);
        S.printHorizontal();
        System.out.println("------");

        skipList.put("GHJ", 123);
        S.printHorizontal();
        System.out.println("------");

        skipList.put("AAA", 123);
        S.printHorizontal();
        System.out.println("------");
        S.printVertical();
        System.out.println("======");
        skipList.put("AAA", 123);
        S.printHorizontal();
        System.out.println("------");
        skipList.put("AAA", 124);
        SkipListPrintUtils<String, Integer> S = new SkipListPrintUtils<>(skipList);
        S.printHorizontal();
        System.out.println("------");
        int a = skipList.get("AAA");
        System.out.println("a = " + a);
        Integer e = skipList.remove("GHJ");
        System.out.println("e = " + e);
        S = new SkipListPrintUtils<>(skipList);
        S.printHorizontal();
        
    }

    @Test
    void put() {
        skipList.put("ABC", 2);
        skipList.put("ABC", 3);
        skipList.put("ABD", 20);
        Integer x = skipList.put("ABG", 21);
        int a = skipList.get("ABC");
        int b = skipList.get("ABC");
        int c = skipList.get("ABD");
        int d = skipList.get("ABG");
        System.out.println("a = " + a);
        System.out.println("b = " + b);
        System.out.println("c = " + c);
        System.out.println("d = " + d);
        SkipListPrintUtils<String, Integer> S = new SkipListPrintUtils<>(skipList);
        S.printHorizontal();
        int e = skipList.remove("ABG");
        System.out.println("e = " + e);
        S = new SkipListPrintUtils<>(skipList);
        S.printHorizontal();
    }
}