package org.spacerf.rapidsdb.datastructures.sst;

import java.nio.charset.Charset;
import java.security.MessageDigest;

public class BloomFilterInfo {
    private final int m; // m = bitSetSize
    private final int n; // n = expectedNumberOfRecords
    private final int k; // k = number of hash functions
    private final Charset charset;

    private final MessageDigest messageDigest;


    // Instance variables - getter setter do not make sense
    public final float e; // false positive probability
    public final int bitsPerRecord;
    protected int size; // number of records in the filter
    public BloomFilterInfo(int m, int n, int k, float e, int bitsPerRecord, Charset charset, MessageDigest messageDigest) {
        this.m = m;
        this.n = n;
        this.k = k;
        this.e = e;
        this.bitsPerRecord = bitsPerRecord;
        this.charset = charset;
        this.messageDigest = messageDigest;
    }

    public int getM() {
        return m;
    }

    public int getN() {
        return n;
    }

    public int getK() {
        return k;
    }

    public Charset getCharset() {
        return charset;
    }

    public MessageDigest getMessageDigest() {
        return messageDigest;
    }

    public float getE() {
        return e;
    }

    public int getBitsPerRecord() {
        return bitsPerRecord;
    }

    public int getSize() {
        return size;
    }

    public double getFalsePositiveProbability() {
        // (1 - e^(-k * n / m)) ^ k
        return Math.pow((1 - Math.exp(-k * (double) size
                / (double) m)), k);
    }
}
