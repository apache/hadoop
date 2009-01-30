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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.io.Writable;

/**
 * HBase customizations of MapFile.
 */
public class HBaseMapFile extends MapFile {
  // TODO not used. remove?!
  //  private static final Log LOG = LogFactory.getLog(HBaseMapFile.class);
  
  /**
   * Values are instances of this class.
   */
  public static final Class<? extends Writable>  VALUE_CLASS =
    ImmutableBytesWritable.class;

  /**
   * A reader capable of reading and caching blocks of the data file.
   */
  public static class HBaseReader extends MapFile.Reader {
    private final boolean blockCacheEnabled;

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
      this.blockCacheEnabled = blockCacheEnabled;
      open(fs, dirName, new HStoreKey.HStoreKeyWritableComparator(hri), conf);
      
      // Force reading of the mapfile index by calling midKey. Reading the
      // index will bring the index into memory over here on the client and
      // then close the index file freeing up socket connection and resources
      // in the datanode. Usually, the first access on a MapFile.Reader will
      // load the index force the issue in HStoreFile MapFiles because an
      // access may not happen for some time; meantime we're using up datanode
      // resources (See HADOOP-2341). midKey() goes to index. Does not seek.
      midKey();
    }

    @Override
    protected SequenceFile.Reader createDataFileReader(
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
          int bufferSize, long length)
        throws IOException {
          return new FSDataInputStream(new BlockFSInputStream(
                  super.openFile(fs, file, bufferSize, length), length,
                  blockSize));
        }
      };
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
