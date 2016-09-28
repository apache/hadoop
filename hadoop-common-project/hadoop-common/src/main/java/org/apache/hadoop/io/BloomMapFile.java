/**
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

package org.apache.hadoop.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.bloom.DynamicBloomFilter;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_ERROR_RATE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_ERROR_RATE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_SIZE_KEY;

/**
 * This class extends {@link MapFile} and provides very much the same
 * functionality. However, it uses dynamic Bloom filters to provide
 * quick membership test for keys, and it offers a fast version of 
 * {@link Reader#get(WritableComparable, Writable)} operation, especially in
 * case of sparsely populated MapFile-s.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BloomMapFile {
  private static final Log LOG = LogFactory.getLog(BloomMapFile.class);
  public static final String BLOOM_FILE_NAME = "bloom";
  public static final int HASH_COUNT = 5;
  
  public static void delete(FileSystem fs, String name) throws IOException {
    Path dir = new Path(name);
    Path data = new Path(dir, MapFile.DATA_FILE_NAME);
    Path index = new Path(dir, MapFile.INDEX_FILE_NAME);
    Path bloom = new Path(dir, BLOOM_FILE_NAME);

    fs.delete(data, true);
    fs.delete(index, true);
    fs.delete(bloom, true);
    fs.delete(dir, true);
  }

  private static byte[] byteArrayForBloomKey(DataOutputBuffer buf) {
    int cleanLength = buf.getLength();
    byte [] ba = buf.getData();
    if (cleanLength != ba.length) {
      ba = new byte[cleanLength];
      System.arraycopy(buf.getData(), 0, ba, 0, cleanLength);
    }
    return ba;
  }
  
  public static class Writer extends MapFile.Writer {
    private DynamicBloomFilter bloomFilter;
    private int numKeys;
    private int vectorSize;
    private Key bloomKey = new Key();
    private DataOutputBuffer buf = new DataOutputBuffer();
    private FileSystem fs;
    private Path dir;
    
    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
        Class<? extends WritableComparable> keyClass,
        Class<? extends Writable> valClass, CompressionType compress,
        CompressionCodec codec, Progressable progress) throws IOException {
      this(conf, new Path(dirName), keyClass(keyClass), valueClass(valClass), 
           compression(compress, codec), progressable(progress));
    }

    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
        Class<? extends WritableComparable> keyClass,
        Class valClass, CompressionType compress,
        Progressable progress) throws IOException {
      this(conf, new Path(dirName), keyClass(keyClass), valueClass(valClass), 
           compression(compress), progressable(progress));
    }

    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
        Class<? extends WritableComparable> keyClass,
        Class valClass, CompressionType compress)
        throws IOException {
      this(conf, new Path(dirName), keyClass(keyClass), valueClass(valClass), 
           compression(compress));
    }

    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
        WritableComparator comparator, Class valClass,
        CompressionType compress, CompressionCodec codec, Progressable progress)
        throws IOException {
      this(conf, new Path(dirName), comparator(comparator), 
           valueClass(valClass), compression(compress, codec), 
           progressable(progress));
    }

    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
        WritableComparator comparator, Class valClass,
        CompressionType compress, Progressable progress) throws IOException {
      this(conf, new Path(dirName), comparator(comparator), 
           valueClass(valClass), compression(compress),
           progressable(progress));
    }

    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
        WritableComparator comparator, Class valClass, CompressionType compress)
        throws IOException {
      this(conf, new Path(dirName), comparator(comparator), 
           valueClass(valClass), compression(compress));
    }

    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
        WritableComparator comparator, Class valClass) throws IOException {
      this(conf, new Path(dirName), comparator(comparator), 
           valueClass(valClass));
    }

    @Deprecated
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  Class<? extends WritableComparable> keyClass,
                  Class valClass) throws IOException {
      this(conf, new Path(dirName), keyClass(keyClass), valueClass(valClass));
    }

    public Writer(Configuration conf, Path dir, 
                  SequenceFile.Writer.Option... options) throws IOException {
      super(conf, dir, options);
      this.fs = dir.getFileSystem(conf);
      this.dir = dir;
      initBloomFilter(conf);
    }

    private synchronized void initBloomFilter(Configuration conf) {
      numKeys = conf.getInt(
          IO_MAPFILE_BLOOM_SIZE_KEY, IO_MAPFILE_BLOOM_SIZE_DEFAULT);
      // vector size should be <code>-kn / (ln(1 - c^(1/k)))</code> bits for
      // single key, where <code> is the number of hash functions,
      // <code>n</code> is the number of keys and <code>c</code> is the desired
      // max. error rate.
      // Our desired error rate is by default 0.005, i.e. 0.5%
      float errorRate = conf.getFloat(
          IO_MAPFILE_BLOOM_ERROR_RATE_KEY, IO_MAPFILE_BLOOM_ERROR_RATE_DEFAULT);
      vectorSize = (int)Math.ceil((double)(-HASH_COUNT * numKeys) /
          Math.log(1.0 - Math.pow(errorRate, 1.0/HASH_COUNT)));
      bloomFilter = new DynamicBloomFilter(vectorSize, HASH_COUNT,
          Hash.getHashType(conf), numKeys);
    }

    @Override
    public synchronized void append(WritableComparable key, Writable val)
        throws IOException {
      super.append(key, val);
      buf.reset();
      key.write(buf);
      bloomKey.set(byteArrayForBloomKey(buf), 1.0);
      bloomFilter.add(bloomKey);
    }

    @Override
    public synchronized void close() throws IOException {
      super.close();
      DataOutputStream out = fs.create(new Path(dir, BLOOM_FILE_NAME), true);
      try {
        bloomFilter.write(out);
        out.flush();
        out.close();
        out = null;
      } finally {
        IOUtils.closeStream(out);
      }
    }

  }
  
  public static class Reader extends MapFile.Reader {
    private DynamicBloomFilter bloomFilter;
    private DataOutputBuffer buf = new DataOutputBuffer();
    private Key bloomKey = new Key();

    public Reader(Path dir, Configuration conf,
                  SequenceFile.Reader.Option... options) throws IOException {
      super(dir, conf, options);
      initBloomFilter(dir, conf);
    }

    @Deprecated
    public Reader(FileSystem fs, String dirName, Configuration conf)
        throws IOException {
      this(new Path(dirName), conf);
    }

    @Deprecated
    public Reader(FileSystem fs, String dirName, WritableComparator comparator,
        Configuration conf, boolean open) throws IOException {
      this(new Path(dirName), conf, comparator(comparator));
    }

    @Deprecated
    public Reader(FileSystem fs, String dirName, WritableComparator comparator,
        Configuration conf) throws IOException {
      this(new Path(dirName), conf, comparator(comparator));
    }
    
    private void initBloomFilter(Path dirName, 
                                 Configuration conf) {
      
      DataInputStream in = null;
      try {
        FileSystem fs = dirName.getFileSystem(conf);
        in = fs.open(new Path(dirName, BLOOM_FILE_NAME));
        bloomFilter = new DynamicBloomFilter();
        bloomFilter.readFields(in);
        in.close();
        in = null;
      } catch (IOException ioe) {
        LOG.warn("Can't open BloomFilter: " + ioe + " - fallback to MapFile.");
        bloomFilter = null;
      } finally {
        IOUtils.closeStream(in);
      }
    }
    
    /**
     * Checks if this MapFile has the indicated key. The membership test is
     * performed using a Bloom filter, so the result has always non-zero
     * probability of false positives.
     * @param key key to check
     * @return  false iff key doesn't exist, true if key probably exists.
     * @throws IOException
     */
    public boolean probablyHasKey(WritableComparable key) throws IOException {
      if (bloomFilter == null) {
        return true;
      }
      buf.reset();
      key.write(buf);
      bloomKey.set(byteArrayForBloomKey(buf), 1.0);
      return bloomFilter.membershipTest(bloomKey);
    }
    
    /**
     * Fast version of the
     * {@link MapFile.Reader#get(WritableComparable, Writable)} method. First
     * it checks the Bloom filter for the existence of the key, and only if
     * present it performs the real get operation. This yields significant
     * performance improvements for get operations on sparsely populated files.
     */
    @Override
    public synchronized Writable get(WritableComparable key, Writable val)
        throws IOException {
      if (!probablyHasKey(key)) {
        return null;
      }
      return super.get(key, val);
    }
    
    /**
     * Retrieve the Bloom filter used by this instance of the Reader.
     * @return a Bloom filter (see {@link Filter})
     */
    public Filter getBloomFilter() {
      return bloomFilter;
    }
  }
}
