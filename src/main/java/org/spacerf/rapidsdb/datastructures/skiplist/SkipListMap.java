package org.spacerf.rapidsdb.datastructures.skiplist;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class SkipListMap<K extends Comparable<K>,V> /*extends SkipList<K,V>*/ implements Map<K,V> {
    SkipList<K, V> skipList = new SkipList<>();

    @Override
    public int size() {
        return skipList.size();
    }

    @Override
    public boolean isEmpty() {
        return skipList.isEmpty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsKey(Object key) {
        V v = skipList.get((K)key);
        return v != null;
    }
    @Override
    //@SuppressWarnings("unchecked")
    public boolean containsValue(Object value) {
        SkipListEntry<K, V> head = skipList.head;
        while (head.down != null) head = head.down;
        while (head.right != null) {
            head = head.right;
            if (head.value != null && head.value == value) return true;
        }
        return false;
    }
    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        return skipList.get((K)key);
    }

    @Override
    public V put(K key, V value) {
        if (key == null || value == null) return null; // no null value allowed. No null keys.
        V response = skipList.put(key, value);
        return response == null ? value : response;
    }
    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        skipList.remove((K)key);
        return null;
    }
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        if (m == null) return;
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            SkipListEntry<K, V> listEntry = new SkipListEntry<>(entry.getKey(), entry.getValue());
            skipList.put(listEntry.key, listEntry.value);
        }
    }
    @Override
    public void clear() {
        throw new RuntimeException("Not implemented");
    }
    @Override
    public Set<K> keySet() {
        Set<K> keySet = new HashSet<>();
        SkipListEntry<K, V> head = skipList.head;
        while (head.down != null) head = head.down;
        while (head.right != null) {
            head = head.right;
            keySet.add(head.key);
        }
        return keySet;
    }
    @Override
    public Collection<V> values() {
        Collection<V> values = new HashSet<>();
        SkipListEntry<K, V> head = skipList.head;
        while (head.down != null) head = head.down;
        while (head.right != null) {
            head = head.right;
            values.add(head.value);
        }
        return values;
    }
    @Override
    public Set<Entry<K, V>> entrySet() {
        Set<Entry<K, V>> entrySet = new HashSet<>();
        SkipListEntry<K, V> head = skipList.head;
        while (head.down != null) head = head.down;
        while (head.right != null) {
            head = head.right;
            if(head.key != null && head.value != null) entrySet.add(new AbstractMap.SimpleEntry<>(head.key, head.value));
        }
        return entrySet;
    }
    @Override
    @SuppressWarnings("unchecked")
    public V getOrDefault(Object key, V defaultValue) {
        V value = skipList.get((K)key);
        return value == null ? defaultValue : value;
    }
    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        SkipListEntry<K, V> head = skipList.head;
        while (head.down != null) head = head.down;
        while (head.right != null) {
            head = head.right;
            if (head.key != null) action.accept(head.key, head.value);
        }
    }
    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        //SkipListIntf.super.replaceAll(function);
        throw new RuntimeException("Not Implemented");
    }
    @Override
    public V putIfAbsent(K key, V value) {
        if (skipList.get(key) != null) return null;
        V response = skipList.put(key, value);
        return response == null ? value : null;
    }
    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object key, Object value) {
        return skipList.remove((K)key)  != null;
    }
    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        if (skipList.get(key) != oldValue) return false;
        return skipList.put(key, newValue) != null;
    }

    /**
     *
     * @param key key with which the specified value is associated
     * @param value value to be associated with the specified key
     * @return oldValue: V
     */
    @Override
    public V replace(K key, V value) {
        if (skipList.get(key) == null) return null;
        return skipList.put(key, value);
    }
    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        if (mappingFunction == null) throw new NullPointerException("mappingFunction == null");
        V v = skipList.get(key);
        if (v != null) return v; // key exists, no compute needed
        v = mappingFunction.apply(key);
        V response = skipList.put(key, v); // successful new entry returns null
        return response == null ? v : null; // upon successful insertion the computed value is returned
    }
    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        if (remappingFunction == null) throw new NullPointerException("remappingFunction == null");
        V v = skipList.get(key);
        if (v == null) return null; // key does not exist, no compute needed
        V recomputedValue = remappingFunction.apply(key, v);
        V response = skipList.put(key, recomputedValue); // successful new entry returns null
        return response == null ? v : null; // upon successful insertion the computed value is returned
    }
    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        if (remappingFunction == null) throw new NullPointerException("remappingFunction == null");
        V existingValue = skipList.get(key);
        V recalculatedValue = remappingFunction.apply(key, existingValue);
        V response = skipList.put(key, recalculatedValue);
        // check response ?
        return recalculatedValue;
    }
    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        if (remappingFunction == null) throw new NullPointerException("remappingFunction == null");
        V existingValue = skipList.get(key);
        V remappedValue =  remappingFunction.apply(value, existingValue);
        V response = skipList.put(key, remappedValue);
        // response is null if the key did not exist oldValue if existed
        return remappedValue;
    }
}
