package org.spacerf.rapidsdb.datastructures.sst;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Objects;

import static java.lang.StrictMath.log;
import static java.lang.StrictMath.pow;

public class BloomFilter<E> implements Serializable {

    private BitSet bitSet;
    private static final Charset charset = StandardCharsets.UTF_8;
    private static final MessageDigest messageDigest;
    static {
        try {
            messageDigest = MessageDigest.getInstance("SHA1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private final BloomFilterInfo<E> metaInfo;
    /**
     * @param e - false positive probability
     * @param n - number of expected records
     */
    public BloomFilter(float e, int n) { //
        // e = falsePositiveProbability
        // m - bitset size, n - number of inserted/expected elements
        // k = - ln(E) / ln(2)
        // m = - n ln(E) / (ln(2))^2 // m - bitset size, n - number of inserted elements
        //int numOfHashFunctions = - ((int) Math.round(log(falsePositiveProbability) / log(2.0)));
        //int bitSetSize = (int) (- n * log(e) / (log(2)*log(2)));
        this((int) (- n * log(e) / (log(2)*log(2))), n, e);
    }

    /**
     * @param m - BitSet size
     * @param n - expected number of records in the bloom filter
     */
    public BloomFilter(int m, int n) {
        //int bitsPerRecord;
        // k = m/n ln(2)
        //int numOfHashFunctions = (int) Math.round((double)(m / n) * log(2.0));
        //int bitsPerRecord = m / n;
        //float e = (float) pow(((float) 1 /2), ((double) m /n)*log(2.0));
        //this(m, bitsPerRecord, n, numOfHashFunctions, e);
        this(m, m/n, n, (int) Math.round((double)(m / n) * log(2.0)),
                (float) pow(((float) 1 /2), ((double) m /n)*log(2.0)));
    }

    /**
     * This Constructor creates a bloom filter by copying from another bloom filter
     * @param m - Size Of BitSet
     * @param n - Expected Number Of Filter Records
     * @param n1 - actualNumberOfFilterElements
     * @param data - data to be copied from the source bloom filter
     */

    public BloomFilter(int m, int n, int n1, BitSet data) {
        this(m, n); bitSet = data; metaInfo.size = n1;
    }


    /**
     * @param m - Size Of BitSet
     * @param bitsPerRecord - bit.s.Per.Record.
     * @param n - Expected Number Of Filter Records
     * @param k - Number of Hash Functions
     * @param e - Probability of False Positive
     */
    private BloomFilter(int m, int bitsPerRecord, int n, int k, float e) {
        //this.bitsPerRecord = bitsPerRecord;
        //this.m = m; this.n = n; this.k = k;
        bitSet = new BitSet(n * bitsPerRecord);
        //this.e = e;
        metaInfo = new BloomFilterInfo<>(bitSet, m, n, k, e, bitsPerRecord, charset.displayName(), messageDigest.getAlgorithm());
    }
    private BloomFilter(int m, int bitsPerRecord, int n, int k, float e, BitSet bitSet) {
        //this.bitsPerRecord = bitsPerRecord;
        //this.m = m; this.n = n; this.k = k;
        this.bitSet = bitSet; //new BitSet(n * bitsPerRecord);
        //this.e = e;
        metaInfo = new BloomFilterInfo<>(bitSet, m, n, k, e, bitsPerRecord, charset.displayName(), messageDigest.getAlgorithm());
    }
    /**
     * @param m - BitSet size
     * @param n - expected number of records in the bloom filter
     * @param e - probability of false positive
     */

    private BloomFilter(int m, int n, float e) {
        //int bitsPerRecord;
        // k = m/n ln(2)
        //int numOfHashFunctions = (int) Math.round((double)(m / n) * log(2.0));
        //bitsPerRecord = m / n;
        this(m, m/n, n, (int) Math.round((double)(m / n) * log(2.0)), e);
    }

    public static int createHash(String key, Charset charset) {
        return createHash(key.getBytes(charset));
    }

    public static int createHash(String val) {
        return createHash(val, charset);
    }

    private static int createHash(byte[] bytes) {
        return createHashes(bytes, 1)[0];
    }

    public static int[] createHashes(byte[] data, int hashes) {
        int[] result = new int[hashes];
        int k = 0;
        byte salt = 0;
        while (k < hashes) {
            byte[] digest;
            synchronized (messageDigest) {
                messageDigest.update(salt);
                salt++;
                digest = messageDigest.digest(data);
            }

            for (int i = 0; i < digest.length/4 && k < hashes; i++) {
                int h = 0;
                for (int j = (i*4); j < (i*4)+4; j++) {
                    h <<= 8;
                    h |= ((int) digest[j]) & 0xFF;
                }
                result[k] = h;
                k++;
            }
        }
        return result;
    }
    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final BloomFilter<E> other = (BloomFilter<E>) obj;
        if (this.metaInfo.getN() != other.metaInfo.getN() || this.metaInfo.getK() != other.metaInfo.getK()
                || this.metaInfo.getM() != other.metaInfo.getM()) {
            return false;
        }
        return Objects.equals(this.bitSet, other.bitSet);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 61 * hash + (this.bitSet != null ? this.bitSet.hashCode() : 0);
        hash = 61 * hash + this.metaInfo.getN();
        hash = 61 * hash + this.metaInfo.getM();
        hash = 61 * hash + this.metaInfo.getK();
        return hash;
    }

    /**
     * Sets all bits to false in the Bloom filter.
     */
    public void clear() {
        bitSet.clear();
        metaInfo.size = 0;
    }

    public void add(E element) {
        add(element.toString().getBytes(charset));
    }
    public void add(byte[] bytes) {
        int[] hashes = createHashes(bytes, metaInfo.getK());
        for (int hash : hashes)
            bitSet.set(Math.abs(hash % metaInfo.getM()), true);
        metaInfo.size ++;
    }
    public void addAll(Collection<? extends E> c) {
        for (E element : c)
            add(element);
    }
    public boolean contains(E element) {
        return contains(element.toString().getBytes(charset));
    }
    public boolean contains(byte[] bytes) {
        int[] hashes = createHashes(bytes, metaInfo.getK());
        for (int hash : hashes) {
            if (!bitSet.get(Math.abs(hash % metaInfo.getM()))) {
                return false;
            }
        }
        return true;
    }

    public boolean containsAll(Collection<? extends E> c) {
        for (E element : c)
            if (!contains(element))
                return false;
        return true;
    }
    public boolean getBit(int bit) {
        return bitSet.get(bit);
    }
    public void setBit(int bit, boolean value) {
        bitSet.set(bit, value);
    }
    public BitSet getBitSet() {
        return bitSet;
    }
    public void setBitSet(BitSet bitSet) {
        this.bitSet = bitSet;
    }

    public BloomFilterInfo<E> getMetaInfo() {
        return metaInfo;
    }

    public BloomFilter(BitSet bitSet, BloomFilterInfo<E> metaInfo) {
        this.bitSet = bitSet;
        this.metaInfo = metaInfo;
    }
}
