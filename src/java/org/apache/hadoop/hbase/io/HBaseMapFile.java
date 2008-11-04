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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.onelab.filter.Key;

/**
 * Hbase customizations of MapFile.
 */
public class HBaseMapFile extends MapFile {
  private static final Log LOG = LogFactory.getLog(HBaseMapFile.class);
  public static final Class<? extends Writable>  VALUE_CLASS =
    ImmutableBytesWritable.class;

  /**
   * Custom bloom filter key maker.
   * @param key
   * @return Key made of bytes of row only.
   */
  protected static Key getBloomFilterKey(WritableComparable key) {
    return new Key(((HStoreKey) key).getRow());
  }

  /**
   * A reader capable of reading and caching blocks of the data file.
   */
  public static class HBaseReader extends MapFile.Reader {
    private final boolean blockCacheEnabled;
    private final HStoreKey firstKey;
    private final HStoreKey finalKey;
    private final String dirName;

    /**
     * @param fs
     * @param dirName
     * @param conf
     * @param hri
     * @throws IOException
     */
    public HBaseReader(FileSystem fs, String dirName, Configuration conf,
      HRegionInfo hri)
    throws IOException {
      this(fs, dirName, conf, false, hri);
    }
    
    /**
     * @param fs
     * @param dirName
     * @param conf
     * @param blockCacheEnabled
     * @param hri
     * @throws IOException
     */
    public HBaseReader(FileSystem fs, String dirName, Configuration conf,
        boolean blockCacheEnabled, HRegionInfo hri)
    throws IOException {
      super(fs, dirName, new HStoreKey.HStoreKeyWritableComparator(hri), 
          conf, false); // defer opening streams
      this.dirName = dirName;
      this.blockCacheEnabled = blockCacheEnabled;
      open(fs, dirName, new HStoreKey.HStoreKeyWritableComparator(hri), conf);
      
      // Force reading of the mapfile index by calling midKey.
      // Reading the index will bring the index into memory over
      // here on the client and then close the index file freeing
      // up socket connection and resources in the datanode. 
      // Usually, the first access on a MapFile.Reader will load the
      // index force the issue in HStoreFile MapFiles because an
      // access may not happen for some time; meantime we're
      // using up datanode resources.  See HADOOP-2341.
      // Of note, midKey just goes to index.  Does not seek.
      midKey();

      // Read in the first and last key.  Cache them.  Make sure we are at start
      // of the file.
      reset();
      HStoreKey key = new HStoreKey();
      super.next(key, new ImmutableBytesWritable());
      key.setHRegionInfo(hri);
      this.firstKey = key;
      // Set us back to start of file.  Call to finalKey restores whatever
      // the previous position.
      reset();

      // Get final key.
      key = new HStoreKey();
      super.finalKey(key);
      key.setHRegionInfo(hri);
      this.finalKey = key;
    }

    @Override
    protected org.apache.hadoop.io.SequenceFile.Reader createDataFileReader(
        FileSystem fs, Path dataFile, Configuration conf)
    throws IOException {
      if (!blockCacheEnabled) {
        return super.createDataFileReader(fs, dataFile, conf);
      }
      final int blockSize = conf.getInt("hbase.hstore.blockCache.blockSize",
          64 * 1024);
      return new SequenceFile.Reader(fs, dataFile,  conf) {
        @Override
        protected FSDataInputStream openFile(FileSystem fs, Path file,
            int bufferSize, long length) throws IOException {
          
          return new FSDataInputStream(new BlockFSInputStream(
                  super.openFile(fs, file, bufferSize, length), length,
                  blockSize));
        }
      };
    }
    
    @Override
    public synchronized void finalKey(final WritableComparable fk)
    throws IOException {
      Writables.copyWritable(this.finalKey, fk);
    }
    
    /**
     * @param hsk
     * @return True if the file *could* contain <code>hsk</code> and false if
     * outside the bounds of this files' start and end keys.
     */
    public boolean containsKey(final HStoreKey hsk) {
      return this.firstKey.compareTo(hsk) <= 0 &&
         this.finalKey.compareTo(hsk) >= 0;
    }
    
    public String toString() {
      HStoreKey midkey = null;
      try {
        midkey = (HStoreKey)midKey();
      } catch (IOException ioe) {
        LOG.warn("Failed get of midkey", ioe);
      }
      return "dirName=" + this.dirName + ", firstKey=" +
        this.firstKey.toString() + ", midKey=" + midkey +
          ", finalKey=" + this.finalKey;
    }
    
    /**
     * @return First key in this file.  Can be null around construction time.
     */
    public HStoreKey getFirstKey() {
      return this.firstKey;
    }

    /**
     * @return Final key in file.  Can be null around construction time.
     */
    public HStoreKey getFinalKey() {
      return this.finalKey;
    }
    
    @Override
    public synchronized WritableComparable getClosest(WritableComparable key,
        Writable value, boolean before)
    throws IOException {
      if ((!before && ((HStoreKey)key).compareTo(this.finalKey) > 0) ||
          (before && ((HStoreKey)key).compareTo(this.firstKey) < 0)) {
        return null;
      }
      return  super.getClosest(key, value, before);
    }
  }
  
  public static class HBaseWriter extends MapFile.Writer {
    /**
     * @param conf
     * @param fs
     * @param dirName
     * @param compression
     * @param hri
     * @throws IOException
     */
    public HBaseWriter(Configuration conf, FileSystem fs, String dirName,
        SequenceFile.CompressionType compression, final HRegionInfo hri)
    throws IOException {
      super(conf, fs, dirName, new HStoreKey.HStoreKeyWritableComparator(hri),
         VALUE_CLASS, compression);
      // Default for mapfiles is 128.  Makes random reads faster if we
      // have more keys indexed and we're not 'next'-ing around in the
      // mapfile.
      setIndexInterval(conf.getInt("hbase.io.index.interval", 128));
    }
  }
}