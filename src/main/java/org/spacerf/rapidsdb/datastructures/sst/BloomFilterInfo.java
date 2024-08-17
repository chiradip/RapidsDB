package org.spacerf.rapidsdb.datastructures.sst;

import java.io.Serializable;
import java.util.BitSet;

public class BloomFilterInfo<E> implements Serializable {
    private final BitSet bitSet;
    private final int m; // m = bitSetSize
    private final int n; // n = expectedNumberOfRecords
    private final int k; // k = number of hash functions
    private final String charset;
    private final String messageDigest;
    public final float e; // false positive probability
    public final int bitsPerRecord;
    protected int size; // number of records in the filter
    public BloomFilterInfo(BitSet bitSet, int m, int n, int k, float e, int bitsPerRecord,
                           String charset, String messageDigest) {
        this.bitSet = bitSet;
        this.m = m; this.n = n; this.k = k; this.e = e;
        this.bitsPerRecord = bitsPerRecord;
        this.charset = charset;
        this.messageDigest = messageDigest;
    }

    public int getM() { return m; }
    public int getN() { return n; }
    public int getK() { return k; }
    public String getCharset() { return charset; }
    public String getMessageDigest() { return messageDigest; }
    public float getE() { return e; }
    public int getBitsPerRecord() { return bitsPerRecord; }
    public int getSize() { return size; }

    public double getFalsePositiveProbability() {
        // (1 - e^(-k * n / m)) ^ k
        return Math.pow((1 - Math.exp(-k * (double) size
                / (double) m)), k);
    }
    public BitSet getBitSet() { return bitSet; }

    public BloomFilter<E> getInstance() {
        return new BloomFilter<>(bitSet, this);
    }
}
