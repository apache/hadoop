/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.onelab.filter.BloomFilter;
import org.onelab.filter.Key;

/**
 * On write, all keys are added to a bloom filter.  On read, all keys are
 * tested first against bloom filter. Keys are HStoreKey.  If passed bloom
 * filter is null, just passes invocation to parent.
 */
// TODO should be fixed generic warnings from MapFile methods
@SuppressWarnings("unchecked")
public class BloomFilterMapFile extends HBaseMapFile {
  @SuppressWarnings("hiding")
  static final Log LOG = LogFactory.getLog(BloomFilterMapFile.class);
  protected static final String BLOOMFILTER_FILE_NAME = "filter";

  public static class Reader extends HBaseReader {
    private final BloomFilter bloomFilter;

    /**
     * @param fs
     * @param dirName
     * @param conf
     * @param filter
     * @param blockCacheEnabled
     * @param hri
     * @throws IOException
     */
    public Reader(FileSystem fs, String dirName, Configuration conf,
        final boolean filter, final boolean blockCacheEnabled, 
        HRegionInfo hri)
    throws IOException {
      super(fs, dirName, conf, blockCacheEnabled, hri);
      if (filter) {
        this.bloomFilter = loadBloomFilter(fs, dirName);
      } else {
        this.bloomFilter = null;
      }
    }

    private BloomFilter loadBloomFilter(FileSystem fs, String dirName)
    throws IOException {
      Path filterFile = new Path(dirName, BLOOMFILTER_FILE_NAME);
      if(!fs.exists(filterFile)) {
        LOG.warn("FileNotFound: " + filterFile + "; proceeding without");
        return null;
      }
      BloomFilter filter = new BloomFilter();
      FSDataInputStream in = fs.open(filterFile);
      try {
        filter.readFields(in);
      } finally {
        in.close();
      }
      return filter;
    }
    
    @Override
    public Writable get(WritableComparable key, Writable val)
    throws IOException {
      if (bloomFilter == null) {
        return super.get(key, val);
      }
      if(bloomFilter.membershipTest(getBloomFilterKey(key))) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("bloom filter reported that key exists");
        }
        return super.get(key, val);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("bloom filter reported that key does not exist");
      }
      return null;
    }

    @Override
    public WritableComparable getClosest(WritableComparable key,
        Writable val) throws IOException {
      if (bloomFilter == null) {
        return super.getClosest(key, val);
      }
      // Note - the key being passed to us is always a HStoreKey
      if(bloomFilter.membershipTest(getBloomFilterKey(key))) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("bloom filter reported that key exists");
        }
        return super.getClosest(key, val);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("bloom filter reported that key does not exist");
      }
      return null;
    }
    
    /**
     * @return size of the bloom filter
     */
   public int getBloomFilterSize() {
      return bloomFilter == null ? 0 : bloomFilter.getVectorSize();
    }
  }
  
  public static class Writer extends HBaseWriter {
    private static final double DEFAULT_NUMBER_OF_HASH_FUNCTIONS = 4.0;
    private final BloomFilter bloomFilter;
    private final String dirName;
    private final FileSystem fs;
    
    /**
     * @param conf
     * @param fs
     * @param dirName
     * @param compression
     * @param filter
     * @param nrows
     * @param hri
     * @throws IOException
     */
    public Writer(Configuration conf, FileSystem fs, String dirName,
      SequenceFile.CompressionType compression, final boolean filter,
      int nrows, final HRegionInfo hri)
    throws IOException {
      super(conf, fs, dirName, compression, hri);
      this.dirName = dirName;
      this.fs = fs;
      if (filter) {
        /* 
         * There is no way to automatically determine the vector size and the
         * number of hash functions to use. In particular, bloom filters are
         * very sensitive to the number of elements inserted into them. For
         * HBase, the number of entries depends on the size of the data stored
         * in the column. Currently the default region size is 256MB, so the
         * number of entries is approximately 
         * 256MB / (average value size for column).
         * 
         * If m denotes the number of bits in the Bloom filter (vectorSize),
         * n denotes the number of elements inserted into the Bloom filter and
         * k represents the number of hash functions used (nbHash), then
         * according to Broder and Mitzenmacher,
         * 
         * ( http://www.eecs.harvard.edu/~michaelm/NEWWORK/postscripts/BloomFilterSurvey.pdf )
         * 
         * the probability of false positives is minimized when k is
         * approximately m/n ln(2).
         * 
         * If we fix the number of hash functions and know the number of
         * entries, then the optimal vector size m = (k * n) / ln(2)
         */
        BloomFilter f = null;
        try {
          f  = new BloomFilter(
            (int) Math.ceil(
                (DEFAULT_NUMBER_OF_HASH_FUNCTIONS * (1.0 * nrows)) /
                Math.log(2.0)),
            (int) DEFAULT_NUMBER_OF_HASH_FUNCTIONS,
            Hash.getHashType(conf)
          );
        } catch (IllegalArgumentException e) {
          LOG.warn("Failed creating bloomfilter; proceeding without", e);
        }
        this.bloomFilter = f;
      } else {
        this.bloomFilter = null;
      }
    }

    @Override
    public void append(WritableComparable key, Writable val)
    throws IOException {
      if (bloomFilter != null) {
        bloomFilter.add(getBloomFilterKey(key));
      }
      super.append(key, val);
    }

    @Override
    public synchronized void close() throws IOException {
      super.close();
      if (this.bloomFilter != null) {
        flushBloomFilter();
      }
    }
    
    /**
     * Flushes bloom filter to disk
     * 
     * @throws IOException
     */
    private void flushBloomFilter() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("flushing bloom filter for " + this.dirName);
      }
      FSDataOutputStream out =
        fs.create(new Path(dirName, BLOOMFILTER_FILE_NAME));
      try {
        bloomFilter.write(out);
      } finally {
        out.close();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("flushed bloom filter for " + this.dirName);
      }
    }
  }

  /**
   * Custom bloom filter key maker.
   * @param key
   * @return Key made of bytes of row only.
   */
  protected static Key getBloomFilterKey(WritableComparable key) {
    return new Key(((HStoreKey) key).getRow());
  }
}