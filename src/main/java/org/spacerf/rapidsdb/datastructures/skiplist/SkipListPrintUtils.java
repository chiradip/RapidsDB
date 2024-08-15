package org.spacerf.rapidsdb.datastructures.skiplist;

public class SkipListPrintUtils<K extends Comparable<K>,V> {
    SkipListEntry<K, V> head;
    public SkipListPrintUtils(SkipList<K,V> skipList) {
        head = skipList.head;
    }
    public void printHorizontal() {
        String s = "";
        int i;
        SkipListEntry<K, V> p = head;
        while (p.down != null) {
            p = p.down;
        }
        i = 0;
        while (p != null) {
            p.pos = i++;
            p = p.right;
        }
        p = head;
        while (p != null) {
            s = getOneRow(p);
            System.out.println(s);
            p = p.down;
        }
    }

    public String getOneRow(SkipListEntry<K, V> p) {
        StringBuilder s;
        int a, b, i;
        a = 0;
        s = new StringBuilder(p.key == null ? "~oo-" : p.key.toString());
        p = p.right;
        while (p != null) {
            SkipListEntry<K, V> q = p;
            while (q.down != null)
                q = q.down;
            b = q.pos;
            s.append(" <-");
            for (i = a + 1; i < b; i++)
                s.append("--------");
            s.append("> ").append(p.key == null ? "+oo~" : p.key);
            a = b;
            p = p.right;
        }
        return (s.toString());
    }

    public void printVertical() {
        String s = "";
        SkipListEntry<K, V> p = head;
        while (p.down != null)
            p = p.down;
        while (p != null) {
            s = getOneColumn(p);
            System.out.println(s);
            p = p.right;
        }
    }

    public String getOneColumn(SkipListEntry<K, V> p) {
        StringBuilder s = new StringBuilder();
        while (p != null) {
            s.append(" ").append(p.key);
            p = p.up;
        }
        return (s.toString());
    }
}
