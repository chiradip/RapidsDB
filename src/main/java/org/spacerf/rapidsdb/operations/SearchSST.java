package org.spacerf.rapidsdb.operations;

import org.spacerf.rapidsdb.datastructures.sst.BloomFilter;
import org.spacerf.rapidsdb.datastructures.sst.BloomFilterInfo;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.TreeMap;

public class SearchSST <K extends Comparable<K>, V> {
    private final String sstFileName;
    private final String bloomFilerFileName;
    private final String indexFileName;
    private final TreeMap<String, Long> inMemoryIndex = new TreeMap<>();
    public SearchSST(String sstFileName, String bloomFilerFileName, String indexFileName) throws IOException {
        this.sstFileName = sstFileName;
        this.bloomFilerFileName = bloomFilerFileName;
        this.indexFileName = indexFileName;
        buildIndex();
        loadIndex();
    }

    private void loadIndex() throws IOException {
        try (RandomAccessFile raFile = new RandomAccessFile(indexFileName, "r");
             FileInputStream fc = new FileInputStream(indexFileName)) {
            FileChannel fileChannel = fc.getChannel();
            String line = raFile.readLine();
            while (line != null) {
                String[] elements = line.split(">");
                long pos = elements[1].trim().isEmpty() ? 0L : Long.parseLong(elements[1].trim());
                inMemoryIndex.put(elements[0].trim(), pos);
                line = raFile.readLine();
            }
        }
    }

    private void buildIndex() throws IOException {
        File file = new File(indexFileName);
        if (file.exists() && file.length() > 10) return;
        try (RandomAccessFile raFile = new RandomAccessFile(sstFileName, "r");
             FileOutputStream fc = new FileOutputStream(indexFileName)) {
            FileChannel channel = fc.getChannel();
            long pos = 0;
            channel.position(pos);
            String line = raFile.readLine();
            int count = 0;
            while (line != null) {
                if (count % 10 != 0) {
                    pos = channel.position();
                    line = raFile.readLine();
                    count ++;
                    continue;
                }
                count ++;
                String[] elements = line.strip().split("\\|");
                if (elements.length < 2) {
                    pos = channel.position();
                    line = raFile.readLine();
                    continue;
                }
                String key = elements[1].trim();
                ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES + key.length() + 3 * Character.BYTES);
                byteBuffer.put(key.getBytes());
                byteBuffer.putChar('>');
                byteBuffer.put((""+pos).getBytes());
                //System.out.println("pos = " + pos);
                byteBuffer.put("\n".getBytes());
                byteBuffer.flip();
                int numBytesWritten = channel.write(byteBuffer);
                pos = channel.position();
                //System.out.println("pos 2= " + pos);
                line = raFile.readLine();
            }
        }
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

    public String search(K key) throws ClassNotFoundException, IOException {
        BloomFilterInfo<K> bloomFilterInfo = getBloomFilterInfo();
        BloomFilter<K> filter = bloomFilterInfo.getInstance();
        if (!filter.contains(key)) return null;
        //String value = null;
        String floorKey = inMemoryIndex.floorKey(key.toString());
        long floorPos = floorKey == null ? 0L : inMemoryIndex.get(floorKey);
        System.out.println("floorKey = " + floorKey);
        System.out.println("floorPos = " + floorPos);
        try (RandomAccessFile raFile = new RandomAccessFile(sstFileName, "r")) {
            raFile.seek(floorPos);
            String line = raFile.readLine();
            while (line != null) {
                if (line.contains(key.toString())) {
                    String[] elements = line.strip().split("\\|");
                    //value = elements[3].strip();
                    if (elements[1].trim().equals(key)) return elements[3].strip();;
                }
                line = raFile.readLine();
            }
        }
        return null;
    }
}
