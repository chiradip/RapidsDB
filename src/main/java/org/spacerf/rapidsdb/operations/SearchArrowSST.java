package org.spacerf.rapidsdb.operations;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.SerializationUtils;
import org.spacerf.rapidsdb.datastructures.sst.BloomFilter;
import org.spacerf.rapidsdb.datastructures.sst.BloomFilterInfo;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class SearchArrowSST <K extends Comparable<K>, V>{
    private final String sstFileName;
    private final String bloomFilerFileName;
    private final String indexFileName;
    private TreeMap<K, Long> inMemoryIndex = new TreeMap<>();/// = new TreeMap<>();
    private final short keySizeInBytes;
    private final String keyFieldName;
    public SearchArrowSST(String sstFileName, String bloomFilerFileName, String indexFileName, short keySizeInBytes, String keyFieldName) throws IOException {
        this.sstFileName = sstFileName;
        this.bloomFilerFileName = bloomFilerFileName;
        this.indexFileName = indexFileName;
        this.keySizeInBytes = keySizeInBytes;
        this.keyFieldName = keyFieldName;
        File indexFile = new File(indexFileName);
        if (indexFile.exists() && indexFile.length() > 10) {
            inMemoryIndex = loadIndex();
        } else inMemoryIndex = buildIndex();
    }

    private TreeMap<K, Long> loadIndex() throws IOException {
        try (FileInputStream fis = new FileInputStream(indexFileName)) {
            return SerializationUtils.deserialize(fis.readAllBytes());
        }
    }

    @SuppressWarnings("unchecked")
    private TreeMap<K, Long> buildIndex() throws IOException {
        TreeMap<K, Long> sparseIndex = new TreeMap<>();
        File file = new File(indexFileName);
        if (file.exists() && file.length() > 10) return loadIndex();
        /**/
        try(BufferAllocator rootAllocator = new RootAllocator();
            FileInputStream fileInputStream = new FileInputStream(sstFileName);
            FileChannel inChannel = fileInputStream.getChannel();
            FileOutputStream fc = new FileOutputStream(indexFileName);
            FileChannel outChannel = fc.getChannel();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ArrowFileReader reader = new ArrowFileReader(inChannel, rootAllocator)) {
                System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
                for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                    long offset = arrowBlock.getOffset();
                    reader.loadRecordBatch(arrowBlock);
                    VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                    Field keyField = vectorSchemaRootRecover.getSchema().findField(keyFieldName);
                    FieldVector fieldVector = vectorSchemaRootRecover.getVector(keyField);
                    Object firstKey = fieldVector.getObject(0);
                    sparseIndex.put((K)firstKey, offset);
                }
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(keySizeInBytes + Long.BYTES + 4);  // 4- padding
                try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
                    out.writeObject(sparseIndex);
                    out.flush();
                    byte[] byteArray = bos.toByteArray();
                    byteBuffer.put(byteArray);
                    int sizeWritten = outChannel.write(byteBuffer);
                    System.out.println("sizeWritten = " + sizeWritten);
                }
        }
        return sparseIndex;
    }
    private BloomFilterInfo<K> getBloomFilterInfo() throws ClassNotFoundException, IOException {
        BloomFilterInfo<K> metaInfo;
        try (FileInputStream inFile = new FileInputStream(bloomFilerFileName)) {
            ObjectInputStream in = new ObjectInputStream(inFile);
            @SuppressWarnings("unchecked")
            BloomFilterInfo<K> metaInfo1 = (BloomFilterInfo<K>) in.readObject();
            metaInfo = metaInfo1;
        }
        return metaInfo;
    }

    public Map<String, Object> search(K key) throws ClassNotFoundException, IOException {
        BloomFilterInfo<K> bloomFilterInfo = getBloomFilterInfo();
        BloomFilter<K> filter = bloomFilterInfo.getInstance();
        if (!filter.contains(key)) return null;
        //String value = null;
        K floorKey = inMemoryIndex.floorKey(key);
        long floorPos = (floorKey == null) ? 0L : inMemoryIndex.get(floorKey);
        System.out.println("floorKey = " + floorKey);
        System.out.println("floorPos = " + floorPos);
        /**/
        try(BufferAllocator rootAllocator = new RootAllocator();
            FileInputStream fileInputStream = new FileInputStream(sstFileName);
            ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)){
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                long offset = arrowBlock.getOffset();
                if (offset < floorPos) continue;
                // process
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                Schema schema = vectorSchemaRootRecover.getSchema();
                List<Field> fields = schema.getFields();
                List<FieldVector> fieldVectors = vectorSchemaRootRecover.getFieldVectors();
                int rowCount = vectorSchemaRootRecover.getRowCount();
                for (int i  = 0; i < rowCount; i++) {
                    if (fieldVectors.getFirst().getObject(i).equals(key)) {
                        Map<String, Object> rowMap = new HashMap<>();
                        for (int j = 0; j < fieldVectors.size(); j++) {
                            String fieldName = fields.get(j).getName();
                            Object fieldValue = fieldVectors.get(j).getObject(i);
                            rowMap.put(fieldName, fieldValue);
                        }
                        return rowMap;
                    }
                }
            }
        }
        return null;
    }
}
