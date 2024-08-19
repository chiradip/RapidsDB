package org.spacerf.rapidsdb.operations;

import org.spacerf.rapidsdb.datastructures.skiplist.SkipList;
import org.spacerf.rapidsdb.datastructures.skiplist.SkipListEntry;

import java.io.*;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FlushSkipList<K extends Comparable<K>, V> {
    private final SkipListEntry<K, V> head;
    private final int skipListSize;
    final int bytesPerRecord;
    final float padding;
    public FlushSkipList(SkipList<K, V> skipList, int bytesPerRecord, float padding) {
        this.skipListSize = skipList.size();
        this.bytesPerRecord = bytesPerRecord;
        this.padding = padding;
        skipList.setWriteProtected(true);
        SkipListEntry<K, V> head = skipList.head;
        while(head.down != null) {
            head = head.down;
        }
        this.head = head;
    }

    private ByteBuffer getByteBuffer() throws IOException {
        int initialCapacity = (int)(skipListSize * bytesPerRecord * 8 * padding);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(initialCapacity);
        SkipListEntry<K, V> head = this.head;
        while (head.right != null) {
            head = head.right;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            if(head.value != null) {
                oos.writeObject("|"+head.getKey()+"|");
                oos.writeObject("|"+head.getValue()+"|");
                //oos.writeObject("->");
            }
            oos.writeObject("\n");
            oos.flush();
            byte [] data = bos.toByteArray();
            try {
                if (byteBuffer.remaining() <= data.length) {
                    initialCapacity = (int)(initialCapacity * 1.5);
                    ByteBuffer byteBuffer2 = ByteBuffer.allocateDirect(initialCapacity);
                    byteBuffer.flip();
                    byteBuffer2.put(byteBuffer);
                    byteBuffer2.put(data);
                    byteBuffer = byteBuffer2;
                } else byteBuffer.put(data);
            } catch (BufferOverflowException e) {
                // TODO: readjust the initial-capacity of the byteBuffer
                // DONE: ABOVE
                System.out.println("e.toString() = " + e.toString());
            }
        }
        return byteBuffer;
    }
    void flush(String fileName) throws IOException {
        ByteBuffer byteBuffer = getByteBuffer();
        byteBuffer.flip();
        try (FileOutputStream fc = new FileOutputStream(fileName)) {
            FileChannel channel = fc.getChannel();
            int res = channel.write(byteBuffer);
            // log the res
        }
    }
}
