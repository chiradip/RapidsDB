package org.spacerf.rapidsdb.operations;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.spacerf.rapidsdb.datastructures.skiplist.SkipList;
import org.spacerf.rapidsdb.datastructures.skiplist.SkipListEntry;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FlushSkipListInArrow<K extends Comparable<K>, V> {
    private final SkipListEntry<K, V> head;
    private final int skipListSize;
    final int bytesPerRecord;
    final float padding;
    private final Schema schema;
    public FlushSkipListInArrow(SkipList<K, V> skipList, int bytesPerRecord, float padding) {
        this.skipListSize = skipList.size();
        this.bytesPerRecord = bytesPerRecord;
        this.padding = padding;
        schema = skipList.getSchema();
        skipList.setWriteProtected(true);
        SkipListEntry<K, V> head = skipList.head;
        while(head.down != null) {
            head = head.down;
        }
        this.head = head;
    }

    private VectorSchemaRoot getVectorSchemaRoot() throws NoSuchFieldException, IllegalAccessException {
        if (schema == null) throw new NullPointerException("Schema is NULL");
        SkipListEntry<K, V> head = this.head;
        try (BufferAllocator allocator = new RootAllocator()) {
            try(VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)) {
                while (head.right != null) {
                    head = head.right;
                    if(head.value != null) {
                        for(Field field: schema.getFields()) {
                            V v = head.getValue();
                            java.lang.reflect.Field f = v.getClass().getField(field.getName());
                            Object value = f.get(v);
                            FieldVector fieldVector = vectorSchemaRoot.getVector(f.getName());
                            FieldType fieldType = field.getFieldType();
                            ArrowType arrowType = fieldType.getType();
                            if (arrowType.getTypeID() == ArrowType.ArrowTypeID.Utf8) {

                            }
                        }

                    }
                }
            }
        }
        return null;
    }

    /*
    Replace this with VectorSchemaRoot
     */
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
