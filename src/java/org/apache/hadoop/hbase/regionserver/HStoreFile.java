/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.BlockFSInputStream;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.onelab.filter.BloomFilter;
import org.onelab.filter.Key;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HStoreKey;

/**
 * A HStore data file.  HStores usually have one or more of these files.  They
 * are produced by flushing the memcache to disk.
 *
 * <p>Each HStore maintains a bunch of different data files. The filename is a
 * mix of the parent dir, the region name, the column name, and a file
 * identifier. The name may also be a reference to a store file located
 * elsewhere. This class handles all that path-building stuff for you.
 * 
 * <p>An HStoreFile usually tracks 4 things: its parent dir, the region
 * identifier, the column family, and the file identifier.  If you know those
 * four things, you know how to obtain the right HStoreFile.  HStoreFiles may
 * also refernce store files in another region serving either from
 * the top-half of the remote file or from the bottom-half.  Such references
 * are made fast splitting regions.
 * 
 * <p>Plain HStoreFiles are named for a randomly generated id as in:
 * <code>1278437856009925445</code>  A file by this name is made in both the
 * <code>mapfiles</code> and <code>info</code> subdirectories of a
 * HStore columnfamily directoy: E.g. If the column family is 'anchor:', then
 * under the region directory there is a subdirectory named 'anchor' within
 * which is a 'mapfiles' and 'info' subdirectory.  In each will be found a
 * file named something like <code>1278437856009925445</code>, one to hold the
 * data in 'mapfiles' and one under 'info' that holds the sequence id for this
 * store file.
 * 
 * <p>References to store files located over in some other region look like
 * this:
 * <code>1278437856009925445.hbaserepository,qAReLZD-OyQORZWq_vqR1k==,959247014679548184</code>:
 * i.e. an id followed by the name of the referenced region.  The data
 * ('mapfiles') of HStoreFile references are empty. The accompanying
 * <code>info</code> file contains the
 * midkey, the id of the remote store we're referencing and whether we're
 * to serve the top or bottom region of the remote store file.  Note, a region
 * is not splitable if it has instances of store file references (References
 * are cleaned up by compactions).
 * 
 * <p>When merging or splitting HRegions, we might want to modify one of the 
 * params for an HStoreFile (effectively moving it elsewhere).
 */
public class HStoreFile implements HConstants {
  static final Log LOG = LogFactory.getLog(HStoreFile.class.getName());
  static final byte INFO_SEQ_NUM = 0;
  static final String HSTORE_DATFILE_DIR = "mapfiles";
  static final String HSTORE_INFO_DIR = "info";
  static final String HSTORE_FILTER_DIR = "filter";
  
  /** 
   * For split HStoreFiles, specifies if the file covers the lower half or
   * the upper half of the key range
   */
  public static enum Range {
    /** HStoreFile contains upper half of key range */
    top,
    /** HStoreFile contains lower half of key range */
    bottom
  }
  
  private final static Random rand = new Random();

  private final Path basedir;
  private final int encodedRegionName;
  private final byte [] colFamily;
  private final long fileId;
  private final HBaseConfiguration conf;
  private final FileSystem fs;
  private final Reference reference;

  /**
   * Constructor that fully initializes the object
   * @param conf Configuration object
   * @param basedir qualified path that is parent of region directory
   * @param encodedRegionName file name friendly name of the region
   * @param colFamily name of the column family
   * @param fileId file identifier
   * @param ref Reference to another HStoreFile.
   * @throws IOException
   */
  HStoreFile(HBaseConfiguration conf, FileSystem fs, Path basedir,
      int encodedRegionName, byte [] colFamily, long fileId,
      final Reference ref) throws IOException {
    this.conf = conf;
    this.fs = fs;
    this.basedir = basedir;
    this.encodedRegionName = encodedRegionName;
    this.colFamily = colFamily;
    
    long id = fileId;
    if (id == -1) {
      Path mapdir = HStoreFile.getMapDir(basedir, encodedRegionName, colFamily);
      Path testpath = null;
      do {
        id = Math.abs(rand.nextLong());
        testpath = new Path(mapdir, createHStoreFilename(id, -1));
      } while(fs.exists(testpath));
    }
    this.fileId = id;
    
    // If a reference, construction does not write the pointer files.  Thats
    // done by invocations of writeReferenceFiles(hsf, fs).  Happens at fast
    // split time.
    this.reference = ref;
  }

  /** @return the region name */
  boolean isReference() {
    return reference != null;
  }
  
  Reference getReference() {
    return reference;
  }

  int getEncodedRegionName() {
    return this.encodedRegionName;
  }

  /** @return the column family */
  byte [] getColFamily() {
    return colFamily;
  }

  /** @return the file identifier */
  long getFileId() {
    return fileId;
  }

  // Build full filenames from those components
  
  /** @return path for MapFile */
  Path getMapFilePath() {
    if (isReference()) {
      return getMapFilePath(encodedRegionName, fileId,
          reference.getEncodedRegionName());
    }
    return getMapFilePath(this.encodedRegionName, fileId);
  }

  private Path getMapFilePath(final Reference r) {
    if (r == null) {
      return getMapFilePath();
    }
    return getMapFilePath(r.getEncodedRegionName(), r.getFileId());
  }

  private Path getMapFilePath(final int encodedName, final long fid) {
    return getMapFilePath(encodedName, fid, HRegionInfo.NO_HASH);
  }
  
  private Path getMapFilePath(final int encodedName, final long fid,
      final int ern) {
    return new Path(HStoreFile.getMapDir(basedir, encodedName, colFamily), 
      createHStoreFilename(fid, ern));
  }

  /** @return path for info file */
  Path getInfoFilePath() {
    if (isReference()) {
      return getInfoFilePath(encodedRegionName, fileId,
          reference.getEncodedRegionName());
 
    }
    return getInfoFilePath(encodedRegionName, fileId);
  }
  
  private Path getInfoFilePath(final int encodedName, final long fid) {
    return getInfoFilePath(encodedName, fid, HRegionInfo.NO_HASH);
  }
  
  private Path getInfoFilePath(final int encodedName, final long fid,
      final int ern) {
    return new Path(HStoreFile.getInfoDir(basedir, encodedName, colFamily), 
      createHStoreFilename(fid, ern));
  }

  // File handling

  /*
   * Split by making two new store files that reference top and bottom regions
   * of original store file.
   * @param midKey
   * @param dstA
   * @param dstB
   * @param fs
   * @param c
   * @throws IOException
   *
   * @param midKey the key which will be the starting key of the second region
   * @param dstA the file which will contain keys from the start of the source
   * @param dstB the file which will contain keys from midKey to end of source
   * @param fs file system
   * @param c configuration
   * @throws IOException
   */
  void splitStoreFile(final HStoreFile dstA, final HStoreFile dstB,
      final FileSystem fs)
  throws IOException {
    dstA.writeReferenceFiles(fs);
    dstB.writeReferenceFiles(fs);
  }
  
  void writeReferenceFiles(final FileSystem fs)
  throws IOException {
    createOrFail(fs, getMapFilePath());
    writeSplitInfo(fs);
  }
  
  /*
   * If reference, create and write the remote store file id, the midkey and
   * whether we're going against the top file region of the referent out to
   * the info file. 
   * @param p Path to info file.
   * @param hsf
   * @param fs
   * @throws IOException
   */
  private void writeSplitInfo(final FileSystem fs) throws IOException {
    Path p = getInfoFilePath();
    if (fs.exists(p)) {
      throw new IOException("File already exists " + p.toString());
    }
    FSDataOutputStream out = fs.create(p);
    try {
      reference.write(out);
    } finally {
      out.close();
   }
  }

  /**
   * @see #writeSplitInfo(FileSystem fs)
   */
  static HStoreFile.Reference readSplitInfo(final Path p, final FileSystem fs)
  throws IOException {
    FSDataInputStream in = fs.open(p);
    try {
      HStoreFile.Reference r = new HStoreFile.Reference();
      r.readFields(in);
      return r;
    } finally {
      in.close();
    }
  }

  private void createOrFail(final FileSystem fs, final Path p)
  throws IOException {
    if (fs.exists(p)) {
      throw new IOException("File already exists " + p.toString());
    }
    if (!fs.createNewFile(p)) {
      throw new IOException("Failed create of " + p);
    }
  }

  /** 
   * Reads in an info file
   *
   * @param fs file system
   * @return The sequence id contained in the info file
   * @throws IOException
   */
  long loadInfo(FileSystem fs) throws IOException {
    Path p = null;
    if (isReference()) {
      p = getInfoFilePath(reference.getEncodedRegionName(), reference.getFileId());
    } else {
      p = getInfoFilePath();
    }
    DataInputStream in = new DataInputStream(fs.open(p));
    try {
      byte flag = in.readByte();
      if(flag == INFO_SEQ_NUM) {
        return in.readLong();
      }
      throw new IOException("Cannot process log file: " + p);
    } finally {
      in.close();
    }
  }
  
  /**
   * Writes the file-identifier to disk
   * 
   * @param fs file system
   * @param infonum file id
   * @throws IOException
   */
  void writeInfo(FileSystem fs, long infonum) throws IOException {
    Path p = getInfoFilePath();
    FSDataOutputStream out = fs.create(p);
    try {
      out.writeByte(INFO_SEQ_NUM);
      out.writeLong(infonum);
    } finally {
      out.close();
    }
  }
  
  /**
   * Delete store map files.
   * @throws IOException 
   */
  public void delete() throws IOException {
    fs.delete(getMapFilePath(), true);
    fs.delete(getInfoFilePath(), true);
  }
  
  /**
   * Renames the mapfiles and info directories under the passed
   * <code>hsf</code> directory.
   * @param fs
   * @param hsf
   * @return True if succeeded.
   * @throws IOException
   */
  public boolean rename(final FileSystem fs, final HStoreFile hsf)
  throws IOException {
    Path src = getMapFilePath();
    if (!fs.exists(src)) {
      throw new FileNotFoundException(src.toString());
    }
    boolean success = fs.rename(src, hsf.getMapFilePath());
    if (!success) {
      LOG.warn("Failed rename of " + src + " to " + hsf.getMapFilePath());
    } else {
      src = getInfoFilePath();
      if (!fs.exists(src)) {
        throw new FileNotFoundException(src.toString());
      }
      success = fs.rename(src, hsf.getInfoFilePath());
      if (!success) {
        LOG.warn("Failed rename of " + src + " to " + hsf.getInfoFilePath());
      }
    }
    return success;
  }
  
  /**
   * Get reader for the store file map file.
   * Client is responsible for closing file when done.
   * @param fs
   * @param bloomFilter If true, a bloom filter exists
   * @param blockCacheEnabled If true, MapFile blocks should be cached.
   * @return BloomFilterMapFile.Reader
   * @throws IOException
   */
  public synchronized BloomFilterMapFile.Reader getReader(final FileSystem fs,
      final boolean bloomFilter, final boolean blockCacheEnabled)
  throws IOException {
    if (isReference()) {
      return new HStoreFile.HalfMapFileReader(fs,
          getMapFilePath(reference).toString(), conf, 
          reference.getFileRegion(), reference.getMidkey(), bloomFilter,
          blockCacheEnabled);
    }
    return new BloomFilterMapFile.Reader(fs, getMapFilePath().toString(),
        conf, bloomFilter, blockCacheEnabled);
  }

  /**
   * Get a store file writer.
   * Client is responsible for closing file when done.
   * @param fs
   * @param compression Pass <code>SequenceFile.CompressionType.NONE</code>
   * for none.
   * @param bloomFilter If true, create a bloom filter
   * @param nrows number of rows expected. Required if bloomFilter is true.
   * @return MapFile.Writer
   * @throws IOException
   */
  public MapFile.Writer getWriter(final FileSystem fs,
      final SequenceFile.CompressionType compression,
      final boolean bloomFilter, int nrows)
  throws IOException {
    if (isReference()) {
      throw new IOException("Illegal Access: Cannot get a writer on a" +
        "HStoreFile reference");
    }
    return new BloomFilterMapFile.Writer(conf, fs,
      getMapFilePath().toString(), compression, bloomFilter, nrows);
  }

  /**
   * @return Length of the store map file.  If a reference, size is
   * approximation.
   * @throws IOException
   */
  public long length() throws IOException {
    Path p = new Path(getMapFilePath(reference), MapFile.DATA_FILE_NAME);
    long l = p.getFileSystem(conf).getFileStatus(p).getLen();
    return (isReference())? l / 2: l;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return encodedRegionName + "/" + Bytes.toString(colFamily) + "/" + fileId +
      (isReference()? "-" + reference.toString(): "");
  }
  
  static boolean isTopFileRegion(final Range r) {
    return r.equals(Range.top);
  }

  private static String createHStoreFilename(final long fid,
      final int encodedRegionName) {
    return Long.toString(fid) + 
      ((encodedRegionName != HRegionInfo.NO_HASH)?
        "." + encodedRegionName : "");
  }

  /**
   * @param dir Base directory
   * @param encodedRegionName Encoding of region name.
   * @param f Column family.
   * @return path for map file directory
   */
  public static Path getMapDir(Path dir, int encodedRegionName,
      final byte [] f) {
    return getFamilySubDir(dir, encodedRegionName, f, HSTORE_DATFILE_DIR);
  }

  /**
   * @param dir Base directory
   * @param encodedRegionName Encoding of region name.
   * @param f Column family.
   * @return the info directory path
   */
  public static Path getInfoDir(Path dir, int encodedRegionName, byte [] f) {
    return getFamilySubDir(dir, encodedRegionName, f, HSTORE_INFO_DIR);
  }

  /**
   * @param dir Base directory
   * @param encodedRegionName Encoding of region name.
   * @param f Column family.
   * @return the bloom filter directory path
   */
  @Deprecated
  public static Path getFilterDir(Path dir, int encodedRegionName,
      final byte [] f) {
    return getFamilySubDir(dir, encodedRegionName, f, HSTORE_FILTER_DIR);
  }
  
  /*
   * @param base Base directory
   * @param encodedRegionName Encoding of region name.
   * @param f Column family.
   * @param subdir Subdirectory to create under column family/store directory.
   * @return
   */
  private static Path getFamilySubDir(final Path base,
      final int encodedRegionName, final byte [] f, final String subdir) {
    return new Path(base, new Path(Integer.toString(encodedRegionName),
        new Path(Bytes.toString(f), subdir)));
  }

  /*
   * Data structure to hold reference to a store file over in another region.
   */
  static class Reference implements Writable {
    private int encodedRegionName;
    private long fileid;
    private Range region;
    private HStoreKey midkey;
    
    Reference(final int ern, final long fid, final HStoreKey m,
        final Range fr) {
      this.encodedRegionName = ern;
      this.fileid = fid;
      this.region = fr;
      this.midkey = m;
    }
    
    Reference() {
      this(-1, -1, null, Range.bottom);
    }

    long getFileId() {
      return fileid;
    }

    Range getFileRegion() {
      return region;
    }
    
    HStoreKey getMidkey() {
      return midkey;
    }
    
    int getEncodedRegionName() {
      return this.encodedRegionName;
    }
   
    /** {@inheritDoc} */
    @Override
    public String toString() {
      return encodedRegionName + "/" + fileid + "/" + region;
    }

    // Make it serializable.

    /** {@inheritDoc} */
    public void write(DataOutput out) throws IOException {
      // Write out the encoded region name as a String.  Doing it as a String
      // keeps a Reference's serialziation backword compatible with
      // pre-HBASE-82 serializations.  ALternative is rewriting all
      // info files in hbase (Serialized References are written into the
      // 'info' file that accompanies HBase Store files).
      out.writeUTF(Integer.toString(encodedRegionName));
      out.writeLong(fileid);
      // Write true if we're doing top of the file.
      out.writeBoolean(isTopFileRegion(region));
      midkey.write(out);
    }

    /** {@inheritDoc} */
    public void readFields(DataInput in) throws IOException {
      this.encodedRegionName = Integer.parseInt(in.readUTF());
      fileid = in.readLong();
      boolean tmp = in.readBoolean();
      // If true, set region to top.
      region = tmp? Range.top: Range.bottom;
      midkey = new HStoreKey();
      midkey.readFields(in);
    }
  }

  /**
   * Hbase customizations of MapFile.
   */
  static class HbaseMapFile extends MapFile {
    static final Class<? extends Writable>  KEY_CLASS = HStoreKey.class;
    static final Class<? extends Writable>  VALUE_CLASS =
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
    static class HbaseReader extends MapFile.Reader {

      private final boolean blockCacheEnabled;

      /**
       * @param fs
       * @param dirName
       * @param conf
       * @throws IOException
       */
      public HbaseReader(FileSystem fs, String dirName, Configuration conf)
      throws IOException {
        this(fs, dirName, conf, false);
      }
      
      /**
       * @param fs
       * @param dirName
       * @param conf
       * @param blockCacheEnabled
       * @throws IOException
       */
      public HbaseReader(FileSystem fs, String dirName, Configuration conf,
          boolean blockCacheEnabled)
      throws IOException {
        super(fs, dirName, null, conf, false); // defer opening streams
        this.blockCacheEnabled = blockCacheEnabled;
        open(fs, dirName, null, conf);
        
        // Force reading of the mapfile index by calling midKey.
        // Reading the index will bring the index into memory over
        // here on the client and then close the index file freeing
        // up socket connection and resources in the datanode. 
        // Usually, the first access on a MapFile.Reader will load the
        // index force the issue in HStoreFile MapFiles because an
        // access may not happen for some time; meantime we're
        // using up datanode resources.  See HADOOP-2341.
        midKey();
      }

      @Override
      protected org.apache.hadoop.io.SequenceFile.Reader createDataFileReader(
          FileSystem fs, Path dataFile, Configuration conf)
      throws IOException {
        if (!blockCacheEnabled) {
          return super.createDataFileReader(fs, dataFile, conf);
        }
        LOG.info("Block Cache enabled");
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
    }
    
    static class HbaseWriter extends MapFile.Writer {
      /**
       * @param conf
       * @param fs
       * @param dirName
       * @param compression
       * @throws IOException
       */
      public HbaseWriter(Configuration conf, FileSystem fs, String dirName,
          SequenceFile.CompressionType compression)
      throws IOException {
        super(conf, fs, dirName, KEY_CLASS, VALUE_CLASS, compression);
        // Default for mapfiles is 128.  Makes random reads faster if we
        // have more keys indexed and we're not 'next'-ing around in the
        // mapfile.
        setIndexInterval(conf.getInt("hbase.io.index.interval", 128));
      }
    }
  }
  
  /**
   * On write, all keys are added to a bloom filter.  On read, all keys are
   * tested first against bloom filter. Keys are HStoreKey.  If passed bloom
   * filter is null, just passes invocation to parent.
   */
  static class BloomFilterMapFile extends HbaseMapFile {
    protected static final String BLOOMFILTER_FILE_NAME = "filter";

    static class Reader extends HbaseReader {
      private final BloomFilter bloomFilter;

      /**
       * @param fs
       * @param dirName
       * @param conf
       * @param filter
       * @param blockCacheEnabled
       * @throws IOException
       */
      public Reader(FileSystem fs, String dirName, Configuration conf,
          final boolean filter, final boolean blockCacheEnabled)
      throws IOException {
        super(fs, dirName, conf, blockCacheEnabled);
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
          throw new FileNotFoundException("Could not find bloom filter: " +
              filterFile);
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
      
      /** {@inheritDoc} */
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

      /** {@inheritDoc} */
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
      
      /* @return size of the bloom filter */
      int getBloomFilterSize() {
        return bloomFilter == null ? 0 : bloomFilter.getVectorSize();
      }
    }
    
    static class Writer extends HbaseWriter {
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
       * @throws IOException
       */
      @SuppressWarnings("unchecked")
      public Writer(Configuration conf, FileSystem fs, String dirName,
        SequenceFile.CompressionType compression, final boolean filter,
        int nrows)
      throws IOException {
        super(conf, fs, dirName, compression);
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
          this.bloomFilter = new BloomFilter(
              (int) Math.ceil(
                  (DEFAULT_NUMBER_OF_HASH_FUNCTIONS * (1.0 * nrows)) /
                  Math.log(2.0)),
              (int) DEFAULT_NUMBER_OF_HASH_FUNCTIONS
          );
        } else {
          this.bloomFilter = null;
        }
      }

      /** {@inheritDoc} */
      @Override
      public void append(WritableComparable key, Writable val)
      throws IOException {
        if (bloomFilter != null) {
          bloomFilter.add(getBloomFilterKey(key));
        }
        super.append(key, val);
      }

      /** {@inheritDoc} */
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
  }
  
  /**
   * A facade for a {@link MapFile.Reader} that serves up either the top or
   * bottom half of a MapFile (where 'bottom' is the first half of the file
   * containing the keys that sort lowest and 'top' is the second half of the
   * file with keys that sort greater than those of the bottom half).
   * Subclasses BloomFilterMapFile.Reader in case 
   * 
   * <p>This file is not splitable.  Calls to {@link #midKey()} return null.
   */
  static class HalfMapFileReader extends BloomFilterMapFile.Reader {
    private final boolean top;
    private final WritableComparable midkey;
    private boolean firstNextCall = true;
    
    HalfMapFileReader(final FileSystem fs, final String dirName, 
        final Configuration conf, final Range r,
        final WritableComparable midKey)
    throws IOException {
      this(fs, dirName, conf, r, midKey, false, false);
    }
    
    HalfMapFileReader(final FileSystem fs, final String dirName, 
        final Configuration conf, final Range r,
        final WritableComparable midKey, final boolean filter,
        final boolean blockCacheEnabled)
    throws IOException {
      super(fs, dirName, conf, filter, blockCacheEnabled);
      top = isTopFileRegion(r);
      midkey = midKey;
    }
    
    @SuppressWarnings("unchecked")
    private void checkKey(final WritableComparable key)
    throws IOException {
      if (top) {
        if (key.compareTo(midkey) < 0) {
          throw new IOException("Illegal Access: Key is less than midKey of " +
          "backing mapfile");
        }
      } else if (key.compareTo(midkey) >= 0) {
        throw new IOException("Illegal Access: Key is greater than or equal " +
        "to midKey of backing mapfile");
      }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void finalKey(WritableComparable key)
    throws IOException {
      if (top) {
        super.finalKey(key); 
      } else {
        reset();
        Writable value = new ImmutableBytesWritable();
        WritableComparable k = super.getClosest(midkey, value, true);
        ByteArrayOutputStream byteout = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteout);
        k.write(out);
        ByteArrayInputStream bytein =
          new ByteArrayInputStream(byteout.toByteArray());
        DataInputStream in = new DataInputStream(bytein);
        key.readFields(in);
      }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Writable get(WritableComparable key, Writable val)
        throws IOException {
      checkKey(key);
      return super.get(key, val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized WritableComparable getClosest(WritableComparable key,
      Writable val)
    throws IOException {
      WritableComparable closest = null;
      if (top) {
        // If top, the lowest possible key is midkey.  Do not have to check
        // what comes back from super getClosest.  Will return exact match or
        // greater.
        closest = (key.compareTo(this.midkey) < 0)?
          this.midkey: super.getClosest(key, val);
      } else {
        // We're serving bottom of the file.
        if (key.compareTo(this.midkey) < 0) {
          // Check key is within range for bottom.
          closest = super.getClosest(key, val);
          // midkey was made against largest store file at time of split. Smaller
          // store files could have anything in them.  Check return value is
          // not beyond the midkey (getClosest returns exact match or next
          // after).
          if (closest != null && closest.compareTo(this.midkey) >= 0) {
            // Don't let this value out.
            closest = null;
          }
        }
        // Else, key is > midkey so let out closest = null.
      }
      return closest;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unused")
    @Override
    public synchronized WritableComparable midKey() throws IOException {
      // Returns null to indicate file is not splitable.
      return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized boolean next(WritableComparable key, Writable val)
    throws IOException {
      if (firstNextCall) {
        firstNextCall = false;
        if (this.top) {
          // Seek to midkey.  Midkey may not exist in this file.  That should be
          // fine.  Then we'll either be positioned at end or start of file.
          WritableComparable nearest = getClosest(midkey, val);
          // Now copy the mid key into the passed key.
          if (nearest != null) {
            Writables.copyWritable(nearest, key);
            return true;
          }
          return false;
        }
      }
      boolean result = super.next(key, val);
      if (!top && key.compareTo(midkey) >= 0) {
        result = false;
      }
      return result;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void reset() throws IOException {
      if (top) {
        firstNextCall = true;
        seek(midkey);
        return;
      }
      super.reset();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized boolean seek(WritableComparable key)
    throws IOException {
      checkKey(key);
      return super.seek(key);
    }
  }
}