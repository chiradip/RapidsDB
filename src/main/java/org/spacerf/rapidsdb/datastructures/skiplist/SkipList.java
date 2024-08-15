package org.spacerf.rapidsdb.datastructures.skiplist;

import java.util.Objects;
import java.util.Random;

public class SkipList<K extends Comparable<K>, V> {
    public SkipListEntry<K, V> head;
    public SkipListEntry<K, V> tail;
    public int n = 0;   // size
    public int h = 0;   // height
    public Random r = new Random();

    @SuppressWarnings("unchecked")
    public SkipList() {
        this.head = new SkipListEntry<>((K)SkipListEntry.leftFence, null);
        this.tail = new SkipListEntry<>((K)SkipListEntry.rightFence, null);
        head.right = tail;
        tail.left = head;
    }
    /**
     * @return number of entries in the Skip List
     */
    public int size()  {
        return n;
    }
    /**
     * @return if the Skip List is empty
     */
    public boolean isEmpty() {
        return (n == 0);
    }

    /**
     * findEntry(k): finds the largest key x <= k
     * on the lowest level of the Skip List
     * @param k - the search key
     * @return SkipListEntry
     */
    public SkipListEntry<K, V> findEntry(K k) {
        SkipListEntry<K, V> p = head;
        while (true) {
            // go right until a larger key is found
            while (!Objects.equals(p.right.key, SkipListEntry.rightFence) &&
                    p.right.key.compareTo(k) <= 0) {
                p = p.right;
            }

            // go all the way down
            if (p.down != null) p = p.down;
            else break;
        }
        return (p);
    }
    /**
     * Returns the value associated with a key.
     */
    public V get(K k) {
        SkipListEntry<K, V> p = findEntry(k);
        if (k.equals(p.getKey()))
            return (p.value);
        else
            return (null);
    }
    /**
     * @param k - key
     * @param v - value
     * @return - Integer, null if the entry(key) is new, old value if the key existed
     */
    @SuppressWarnings("unchecked")
    public V put(K k, V v) {
        if (k==null) return null; // null keys are terminal keys
        SkipListEntry<K, V> p = findEntry(k);
        if (k.equals(p.getKey())) {
            V old = p.value;
            p.value = v;
            return (old);
        }

        // Create the new Entry and insert it right after p
        SkipListEntry<K, V> q = new SkipListEntry<>(k, v);
        q.left = p;
        q.right = p.right;
        p.right.left = q;
        p.right = q;

        int i = 0;                   // Current level = 0

        while (r.nextDouble() < 0.5) { // the coin flip 0/1
            // if height exceeds the current height, create the NEW EMPTY UPPER level
            if (i >= h) {
                SkipListEntry<K, V> p1, p2;
                h = h + 1;
                // creating the left most and right most element for the new level
                // reassigning head and tail with newly created elements
                p1 = new SkipListEntry<>((K)SkipListEntry.leftFence, null);
                p2 = new SkipListEntry<>((K)SkipListEntry.rightFence, null);

                p1.right = p2; p1.down = head;
                p2.left = p1; p2.down = tail;

                head.up = p1; tail.up = p2;
                head = p1; tail = p2;
            }
            // walk backward until the node (p) has an up link
            while (p.up == null) p = p.left;
            p = p.up;
            SkipListEntry<K, V> e = new SkipListEntry<>(k, null); // It's for the vertical lane - value not necessary
            e.left = p; e.right = p.right; e.down = q;
            p.right.left = e; p.right = e; q.up = e;
            q = e;        // Set q up for the next iteration
            i = i + 1;    // Current level increased by 1
        }
        n = n + 1;
        return (null);   // No old value
    }
    /**
     * Removes the key-value pair with a specified key.
     */
    public V remove(K key) {
        SkipListEntry<K, V> p = findEntry(key);
        if (!Objects.equals(p.getKey(), key))
            return null;
        V ret = p.getValue();
        // does it have a tower?
        while (p.up != null) {
            p.left.right = p.right;
            p.right.left = p.left;
            p = p.up;
        }
        p.left.right = p.right;
        p.right.left = p.left;
        n = n -1; // decrease the size
        return ret;
    }
}