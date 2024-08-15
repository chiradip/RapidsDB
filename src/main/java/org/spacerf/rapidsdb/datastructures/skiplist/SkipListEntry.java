package org.spacerf.rapidsdb.datastructures.skiplist;

import java.util.Objects;

public class SkipListEntry<K extends Comparable<K>, V> {
    public K key;
    public V value;
    public int pos;      // only for pretty printing
    public SkipListEntry<K, V> up, down, left, right;
    public static Object leftFence = null;
    public static Object rightFence = null;
    public SkipListEntry(K k, V v) {
        key = k; value = v;
        up = down = left = right = null;
    }
    public V getValue() { return value; }
    public K getKey() { return key; }
    public V setValue(V val) {
        V oldValue = value;
        value = val;
        return oldValue;
    }
    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
        if (getClass() != o.getClass()) return false;
        SkipListEntry<K, V> ent = (SkipListEntry<K, V>) o;
        return (Objects.equals(ent.getKey(), key)) && (Objects.equals(ent.getValue(), value));
    }
    public String toString() {
        return "(" + key + "," + value + ")";
    }
}