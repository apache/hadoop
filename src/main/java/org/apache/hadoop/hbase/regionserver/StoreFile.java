/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.ByteBloomFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Store data file.  Stores usually have one or more of these files.  They
 * are produced by flushing the memstore to disk.  To
 * create, call {@link #createWriter(FileSystem, Path, int)} and append data.  Be
 * sure to add any metadata before calling close on the Writer
 * (Use the appendMetadata convenience methods). On close, a StoreFile is
 * sitting in the Filesystem.  To refer to it, create a StoreFile instance
 * passing filesystem and path.  To read, call {@link #createReader()}.
 * <p>StoreFiles may also reference store files in another Store.
 */
public class StoreFile implements HConstants {
  static final Log LOG = LogFactory.getLog(StoreFile.class.getName());

  private static final String HFILE_CACHE_SIZE_KEY = "hfile.block.cache.size";

  private static BlockCache hfileBlockCache = null;

  // Make default block size for StoreFiles 8k while testing.  TODO: FIX!
  // Need to make it 8k for testing.
  public static final int DEFAULT_BLOCKSIZE_SMALL = 8 * 1024;

  private final FileSystem fs;
  // This file's path.
  private final Path path;
  // If this storefile references another, this is the reference instance.
  private Reference reference;
  // If this StoreFile references another, this is the other files path.
  private Path referencePath;
  // Should the block cache be used or not.
  private boolean blockcache;
  // Is this from an in-memory store
  private boolean inMemory;

  // Keys for metadata stored in backing HFile.
  /** Constant for the max sequence ID meta */
  public static final byte [] MAX_SEQ_ID_KEY = Bytes.toBytes("MAX_SEQ_ID_KEY");
  // Set when we obtain a Reader.
  private long sequenceid = -1;

  /** Constant for major compaction meta */
  public static final byte [] MAJOR_COMPACTION_KEY =
    Bytes.toBytes("MAJOR_COMPACTION_KEY");
  
  // If true, this file was product of a major compaction.  Its then set
  // whenever you get a Reader.
  private AtomicBoolean majorCompaction = null;

  /** Meta key set when store file is a result of a bulk load */
  public static final byte[] BULKLOAD_TASK_KEY =
    Bytes.toBytes("BULKLOAD_SOURCE_TASK");
  public static final byte[] BULKLOAD_TIME_KEY =
    Bytes.toBytes("BULKLOAD_TIMESTAMP");

  
  static final String BLOOM_FILTER_META_KEY = "BLOOM_FILTER_META";
  static final String BLOOM_FILTER_DATA_KEY = "BLOOM_FILTER_DATA";
  static final byte[] BLOOM_FILTER_TYPE_KEY = 
    Bytes.toBytes("BLOOM_FILTER_TYPE");

  /**
   * Map of the metadata entries in the corresponding HFile
   */
  private Map<byte[], byte[]> metadataMap;

  /*
   * Regex that will work for straight filenames and for reference names.
   * If reference, then the regex has more than just one group.  Group 1 is
   * this files id.  Group 2 the referenced region name, etc.
   */
  private static final Pattern REF_NAME_PARSER =
    Pattern.compile("^(\\d+)(?:\\.(.+))?$");

  private volatile StoreFile.Reader reader;

  // Used making file ids.
  private final static Random rand = new Random();
  private final Configuration conf;
  private final BloomType bloomType;


  /**
   * Constructor, loads a reader and it's indices, etc. May allocate a
   * substantial amount of ram depending on the underlying files (10-20MB?).
   *
   * @param fs  The current file system to use.
   * @param p  The path of the file.
   * @param blockcache  <code>true</code> if the block cache is enabled.
   * @param conf  The current configuration.
   * @param bt The bloom type to use for this store file
   * @throws IOException When opening the reader fails.
   */
  StoreFile(final FileSystem fs, final Path p, final boolean blockcache,
      final Configuration conf, final BloomType bt, final boolean inMemory) 
  throws IOException {
    this.conf = conf;
    this.fs = fs;
    this.path = p;
    this.blockcache = blockcache;
    this.inMemory = inMemory;
    if (isReference(p)) {
      this.reference = Reference.read(fs, p);
      this.referencePath = getReferredToFile(this.path);
    }
    // ignore if the column family config says "no bloom filter" 
    // even if there is one in the hfile.
    if (conf.getBoolean("io.hfile.bloom.enabled", true)) {
      this.bloomType = bt;
    } else {
      this.bloomType = BloomType.NONE;
      LOG.info("Ignoring bloom filter check for file (disabled in config)");
    }
  }

  /**
   * @return Path or null if this StoreFile was made with a Stream.
   */
  Path getPath() {
    return this.path;
  }

  /**
   * @return The Store/ColumnFamily this file belongs to.
   */
  byte [] getFamily() {
    return Bytes.toBytes(this.path.getParent().getName());
  }

  /**
   * @return True if this is a StoreFile Reference; call after {@link #open()}
   * else may get wrong answer.
   */
  boolean isReference() {
    return this.reference != null;
  }

  /**
   * @param p Path to check.
   * @return True if the path has format of a HStoreFile reference.
   */
  public static boolean isReference(final Path p) {
    return !p.getName().startsWith("_") &&
      isReference(p, REF_NAME_PARSER.matcher(p.getName()));
  }

  /**
   * @param p Path to check.
   * @param m Matcher to use.
   * @return True if the path has format of a HStoreFile reference.
   */
  public static boolean isReference(final Path p, final Matcher m) {
    if (m == null || !m.matches()) {
      LOG.warn("Failed match of store file name " + p.toString());
      throw new RuntimeException("Failed match of store file name " +
          p.toString());
    }
    return m.groupCount() > 1 && m.group(2) != null;
  }

  /*
   * Return path to the file referred to by a Reference.  Presumes a directory
   * hierarchy of <code>${hbase.rootdir}/tablename/regionname/familyname</code>.
   * @param p Path to a Reference file.
   * @return Calculated path to parent region file.
   * @throws IOException
   */
  static Path getReferredToFile(final Path p) {
    Matcher m = REF_NAME_PARSER.matcher(p.getName());
    if (m == null || !m.matches()) {
      LOG.warn("Failed match of store file name " + p.toString());
      throw new RuntimeException("Failed match of store file name " +
          p.toString());
    }
    // Other region name is suffix on the passed Reference file name
    String otherRegion = m.group(2);
    // Tabledir is up two directories from where Reference was written.
    Path tableDir = p.getParent().getParent().getParent();
    String nameStrippedOfSuffix = m.group(1);
    // Build up new path with the referenced region in place of our current
    // region in the reference path.  Also strip regionname suffix from name.
    return new Path(new Path(new Path(tableDir, otherRegion),
      p.getParent().getName()), nameStrippedOfSuffix);
  }

  /**
   * @return True if this file was made by a major compaction.
   */
  boolean isMajorCompaction() {
    if (this.majorCompaction == null) {
      throw new NullPointerException("This has not been set yet");
    }
    return this.majorCompaction.get();
  }

  /**
   * @return This files maximum edit sequence id.
   */
  public long getMaxSequenceId() {
    if (this.sequenceid == -1) {
      throw new IllegalAccessError("Has not been initialized");
    }
    return this.sequenceid;
  }
  
  /**
   * Return the highest sequence ID found across all storefiles in
   * the given list. Store files that were created by a mapreduce
   * bulk load are ignored, as they do not correspond to any edit
   * log items.
   * @return 0 if no non-bulk-load files are provided
   */
  public static long getMaxSequenceIdInList(List<StoreFile> sfs) {
    long max = 0;
    for (StoreFile sf : sfs) {
      if (!sf.isBulkLoadResult()) {
        max = Math.max(max, sf.getMaxSequenceId());
      }
    }
    return max;
  }

  /**
   * @return true if this storefile was created by HFileOutputFormat
   * for a bulk load.
   */
  boolean isBulkLoadResult() {
    return metadataMap.containsKey(BULKLOAD_TIME_KEY);
  }

  /**
   * Return the timestamp at which this bulk load file was generated.
   */
  public long getBulkLoadTimestamp() {
    return Bytes.toLong(metadataMap.get(BULKLOAD_TIME_KEY));
  }

  /**
   * Returns the block cache or <code>null</code> in case none should be used.
   *
   * @param conf  The current configuration.
   * @return The block cache or <code>null</code>.
   */
  public static synchronized BlockCache getBlockCache(Configuration conf) {
    if (hfileBlockCache != null) return hfileBlockCache;

    float cachePercentage = conf.getFloat(HFILE_CACHE_SIZE_KEY, 0.0f);
    // There should be a better way to optimize this. But oh well.
    if (cachePercentage == 0L) return null;
    if (cachePercentage > 1.0) {
      throw new IllegalArgumentException(HFILE_CACHE_SIZE_KEY +
        " must be between 0.0 and 1.0, not > 1.0");
    }

    // Calculate the amount of heap to give the heap.
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    long cacheSize = (long)(mu.getMax() * cachePercentage);
    LOG.info("Allocating LruBlockCache with maximum size " +
      StringUtils.humanReadableInt(cacheSize));
    hfileBlockCache = new LruBlockCache(cacheSize, DEFAULT_BLOCKSIZE_SMALL);
    return hfileBlockCache;
  }

  /**
   * @return the blockcache
   */
  public BlockCache getBlockCache() {
    return blockcache ? getBlockCache(conf) : null;
  }

  /**
   * Opens reader on this store file.  Called by Constructor.
   * @return Reader for the store file.
   * @throws IOException
   * @see #closeReader()
   */
  private StoreFile.Reader open()
  throws IOException {
    if (this.reader != null) {
      throw new IllegalAccessError("Already open");
    }
    if (isReference()) {
      this.reader = new HalfStoreFileReader(this.fs, this.referencePath,
          getBlockCache(), this.reference);
    } else {
      this.reader = new StoreFile.Reader(this.fs, this.path, getBlockCache(),
          this.inMemory);
    }
    // Load up indices and fileinfo.
    metadataMap = Collections.unmodifiableMap(this.reader.loadFileInfo());
    // Read in our metadata.
    byte [] b = metadataMap.get(MAX_SEQ_ID_KEY);
    if (b != null) {
      // By convention, if halfhfile, top half has a sequence number > bottom
      // half. Thats why we add one in below. Its done for case the two halves
      // are ever merged back together --rare.  Without it, on open of store,
      // since store files are distingushed by sequence id, the one half would
      // subsume the other.
      this.sequenceid = Bytes.toLong(b);
      if (isReference()) {
        if (Reference.isTopFileRegion(this.reference.getFileRegion())) {
          this.sequenceid += 1;
        }
      }

    }
    b = metadataMap.get(MAJOR_COMPACTION_KEY);
    if (b != null) {
      boolean mc = Bytes.toBoolean(b);
      if (this.majorCompaction == null) {
        this.majorCompaction = new AtomicBoolean(mc);
      } else {
        this.majorCompaction.set(mc);
      }
    }
    
    if (this.bloomType != BloomType.NONE) {
      this.reader.loadBloomfilter();
    }

    return this.reader;
  }
  
  /**
   * @return Reader for StoreFile. creates if necessary
   * @throws IOException
   */
  public StoreFile.Reader createReader() throws IOException {
    if (this.reader == null) {
      this.reader = open();
    }
    return this.reader;
  }

  /**
   * @return Current reader.  Must call createReader first else returns null.
   * @throws IOException 
   * @see {@link #createReader()}
   */
  public StoreFile.Reader getReader() {
    return this.reader;
  }

  /**
   * @throws IOException
   */
  public synchronized void closeReader() throws IOException {
    if (this.reader != null) {
      this.reader.close();
      this.reader = null;
    }
  }

  /**
   * Delete this file
   * @throws IOException
   */
  public void deleteReader() throws IOException {
    closeReader();
    this.fs.delete(getPath(), true);
  }

  @Override
  public String toString() {    
    return this.path.toString() +
      (isReference()? "-" + this.referencePath + "-" + reference.toString(): "");
  }
  
  /**
   * @return a length description of this StoreFile, suitable for debug output
   */
  public String toStringDetailed() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.path.toString());
    sb.append(", isReference=").append(isReference());
    sb.append(", isBulkLoadResult=").append(isBulkLoadResult());
    if (isBulkLoadResult()) {
      sb.append(", bulkLoadTS=").append(getBulkLoadTimestamp());
    } else {
      sb.append(", seqid=").append(getMaxSequenceId());
    }
    sb.append(", majorCompaction=").append(isMajorCompaction());

    return sb.toString();
  }

  /**
   * Utility to help with rename.
   * @param fs
   * @param src
   * @param tgt
   * @return True if succeeded.
   * @throws IOException
   */
  public static Path rename(final FileSystem fs, final Path src,
      final Path tgt)
  throws IOException {
    if (!fs.exists(src)) {
      throw new FileNotFoundException(src.toString());
    }
    if (!fs.rename(src, tgt)) {
      throw new IOException("Failed rename of " + src + " to " + tgt);
    }
    return tgt;
  }

  /**
   * Get a store file writer. Client is responsible for closing file when done.
   * If metadata, add BEFORE closing using
   * {@link #appendMetadata(org.apache.hadoop.hbase.io.hfile.HFile.Writer, long)}.
   * @param fs
   * @param dir Path to family directory.  Makes the directory if doesn't exist.
   * Creates a file with a unique name in this directory.
   * @param blocksize size per filesystem block
   * @return HFile.Writer
   * @throws IOException
   */
  public static StoreFile.Writer createWriter(final FileSystem fs, final Path dir, 
      final int blocksize) 
  throws IOException {
    return createWriter(fs,dir,blocksize,null,null,null,BloomType.NONE,0);
  }

  /**
   * Create a store file writer. Client is responsible for closing file when done.
   * If metadata, add BEFORE closing using appendMetadata()
   * @param fs
   * @param dir Path to family directory.  Makes the directory if doesn't exist.
   * Creates a file with a unique name in this directory.
   * @param blocksize
   * @param algorithm Pass null to get default.
   * @param conf HBase system configuration. used with bloom filters
   * @param bloomType column family setting for bloom filters
   * @param c Pass null to get default.
   * @param maxKeySize peak theoretical entry size (maintains error rate)
   * @return HFile.Writer
   * @throws IOException
   */
  public static StoreFile.Writer createWriter(final FileSystem fs, final Path dir, 
      final int blocksize, final Compression.Algorithm algorithm, 
      final KeyValue.KVComparator c, final Configuration conf, 
      BloomType bloomType, int maxKeySize) 
  throws IOException {
    if (!fs.exists(dir)) {
      fs.mkdirs(dir);
    }
    Path path = getUniqueFile(fs, dir);
    if(conf == null || !conf.getBoolean("io.hfile.bloom.enabled", true)) {
      bloomType = BloomType.NONE;
    }

    return new StoreFile.Writer(fs, path, blocksize,
        algorithm == null? HFile.DEFAULT_COMPRESSION_ALGORITHM: algorithm,
        conf, c == null? KeyValue.COMPARATOR: c, bloomType, maxKeySize);
  }

  /**
   * @param fs
   * @param dir Directory to create file in.
   * @return random filename inside passed <code>dir</code>
   */
  public static Path getUniqueFile(final FileSystem fs, final Path dir)
  throws IOException {
    if (!fs.getFileStatus(dir).isDir()) {
      throw new IOException("Expecting " + dir.toString() +
        " to be a directory");
    }
    return fs.getFileStatus(dir).isDir()? getRandomFilename(fs, dir): dir;
  }

  /**
   *
   * @param fs
   * @param dir
   * @return Path to a file that doesn't exist at time of this invocation.
   * @throws IOException
   */
  static Path getRandomFilename(final FileSystem fs, final Path dir)
  throws IOException {
    return getRandomFilename(fs, dir, null);
  }

  /**
   *
   * @param fs
   * @param dir
   * @param suffix
   * @return Path to a file that doesn't exist at time of this invocation.
   * @throws IOException
   */
  static Path getRandomFilename(final FileSystem fs, final Path dir,
      final String suffix)
  throws IOException {
    long id = -1;
    Path p = null;
    do {
      id = Math.abs(rand.nextLong());
      p = new Path(dir, Long.toString(id) +
        ((suffix == null || suffix.length() <= 0)? "": suffix));
    } while(fs.exists(p));
    return p;
  }

  /*
   * Write out a split reference.
   * @param fs
   * @param splitDir Presumes path format is actually
   * <code>SOME_DIRECTORY/REGIONNAME/FAMILY</code>.
   * @param f File to split.
   * @param splitRow
   * @param range
   * @return Path to created reference.
   * @throws IOException
   */
  static Path split(final FileSystem fs, final Path splitDir,
    final StoreFile f, final byte [] splitRow, final Reference.Range range)
  throws IOException {
    // A reference to the bottom half of the hsf store file.
    Reference r = new Reference(splitRow, range);
    // Add the referred-to regions name as a dot separated suffix.
    // See REF_NAME_PARSER regex above.  The referred-to regions name is
    // up in the path of the passed in <code>f</code> -- parentdir is family,
    // then the directory above is the region name.
    String parentRegionName = f.getPath().getParent().getParent().getName();
    // Write reference with same file id only with the other region name as
    // suffix and into the new region location (under same family).
    Path p = new Path(splitDir, f.getPath().getName() + "." + parentRegionName);
    return r.write(fs, p);
  }

  public static enum BloomType {
    /**
     * Bloomfilters disabled
     */
    NONE,
    /**
     * Bloom enabled with Table row as Key
     */
    ROW,
    /**
     * Bloom enabled with Table row & column (family+qualifier) as Key
     */
    ROWCOL
  }

  /**
   *
   */
  public static class Reader extends HFile.Reader {
    /** Bloom Filter class.  Caches only meta, pass in data */
    protected BloomFilter bloomFilter = null;
    /** Type of bloom filter (e.g. ROW vs ROWCOL) */
    protected BloomType bloomFilterType;

    public Reader(FileSystem fs, Path path, BlockCache cache, 
        boolean inMemory)
    throws IOException {
      super(fs, path, cache, inMemory);
    }

    public Reader(final FSDataInputStream fsdis, final long size,
        final BlockCache cache, final boolean inMemory) {
      super(fsdis,size,cache,inMemory);
      bloomFilterType = BloomType.NONE;
    }

    @Override
    public Map<byte [], byte []> loadFileInfo() 
    throws IOException {
      Map<byte [], byte []> fi = super.loadFileInfo();

      byte[] b = fi.get(BLOOM_FILTER_TYPE_KEY);
      if (b != null) {
        bloomFilterType = BloomType.valueOf(Bytes.toString(b));
      }
      
      return fi;
    }
    
    /**
     * Load the bloom filter for this HFile into memory.  
     * Assumes the HFile has already been loaded
     */
    public void loadBloomfilter() {
      if (this.bloomFilter != null) {
        return; // already loaded
      }
      
      // see if bloom filter information is in the metadata
      try {
        ByteBuffer b = getMetaBlock(BLOOM_FILTER_META_KEY, false);
        if (b != null) {
          if (bloomFilterType == BloomType.NONE) {
            throw new IOException("valid bloom filter type not found in FileInfo");
          }
          this.bloomFilter = new ByteBloomFilter(b);
          LOG.info("Loaded " + (bloomFilterType==BloomType.ROW? "row":"col") 
                 + " bloom filter metadata for " + name);
        }
      } catch (IOException e) {
        LOG.error("Error reading bloom filter meta -- proceeding without", e);
        this.bloomFilter = null;
      } catch (IllegalArgumentException e) {
        LOG.error("Bad bloom filter meta -- proceeding without", e);
        this.bloomFilter = null;
      }
    }
    
    BloomFilter getBloomFilter() {
      return this.bloomFilter;
    }
    
    /**
     * @return bloom type information associated with this store file
     */
    public BloomType getBloomFilterType() {
      return this.bloomFilterType;
    }

    @Override
    public int getFilterEntries() {
      return (this.bloomFilter != null) ? this.bloomFilter.getKeyCount() 
          : super.getFilterEntries();
    }

    @Override
    public HFileScanner getScanner(boolean cacheBlocks, final boolean pread) {
      return new Scanner(this, cacheBlocks, pread);
    }

    protected class Scanner extends HFile.Reader.Scanner {
      public Scanner(Reader r, boolean cacheBlocks, final boolean pread) {
        super(r, cacheBlocks, pread);
      }

      @Override
      public boolean shouldSeek(final byte[] row, 
          final SortedSet<byte[]> columns) {
        if (bloomFilter == null) {
          return true;
        }
        
        byte[] key;
        switch(bloomFilterType) {
          case ROW:
            key = row;
            break;
          case ROWCOL:
            if (columns.size() == 1) {
              byte[] col = columns.first();
              key = Bytes.add(row, col);
              break;
            }
            //$FALL-THROUGH$
          default:
            return true;
        }
        
        try {
          ByteBuffer bloom = getMetaBlock(BLOOM_FILTER_DATA_KEY, true);
          if (bloom != null) {
            return bloomFilter.contains(key, bloom);
          }
        } catch (IOException e) {
          LOG.error("Error reading bloom filter data -- proceeding without", 
              e);
          bloomFilter = null;
        } catch (IllegalArgumentException e) {
          LOG.error("Bad bloom filter data -- proceeding without", e);
          bloomFilter = null;
        }
          
        return true;
      }
      
    }
  }
  
  /**
   *
   */
  public static class Writer extends HFile.Writer {
    private final BloomFilter bloomFilter;
    private final BloomType bloomType;
    private KVComparator kvComparator;
    private KeyValue lastKv = null;
    private byte[] lastByteArray = null;

    /**
     * Creates an HFile.Writer that also write helpful meta data.
     * @param fs file system to write to
     * @param path file name to create
     * @param blocksize HDFS block size
     * @param compress HDFS block compression
     * @param conf user configuration
     * @param comparator key comparator
     * @param bloomType bloom filter setting
     * @param maxKeys maximum amount of keys to add (for blooms)
     * @throws IOException problem writing to FS
     */
    public Writer(FileSystem fs, Path path, int blocksize,
        Compression.Algorithm compress, final Configuration conf,
        final KVComparator comparator, BloomType bloomType, int maxKeys)
      throws IOException {
      super(fs, path, blocksize, compress, comparator.getRawComparator());

      this.kvComparator = comparator;

      if (bloomType != BloomType.NONE && conf != null) {
        float err = conf.getFloat("io.hfile.bloom.error.rate", (float)0.01);      
        int maxFold = conf.getInt("io.hfile.bloom.max.fold", 7);
        
        this.bloomFilter = new ByteBloomFilter(maxKeys, err, 
            Hash.getHashType(conf), maxFold);
        this.bloomFilter.allocBloom();
        this.bloomType = bloomType;
      } else {
        this.bloomFilter = null;
        this.bloomType = BloomType.NONE;
      }
    }

    /**
     * Writes meta data.
     * Call before {@link #close()} since its written as meta data to this file.
     * @param maxSequenceId Maximum sequence id.
     * @param majorCompaction True if this file is product of a major compaction
     * @throws IOException problem writing to FS
     */
    public void appendMetadata(final long maxSequenceId,
      final boolean majorCompaction)
    throws IOException {
      appendFileInfo(MAX_SEQ_ID_KEY, Bytes.toBytes(maxSequenceId));
      appendFileInfo(MAJOR_COMPACTION_KEY, Bytes.toBytes(majorCompaction));
    }

    @Override
    public void append(final KeyValue kv)
    throws IOException {
      if (this.bloomFilter != null) {
        // only add to the bloom filter on a new, unique key
        boolean newKey = true;
        if (this.lastKv != null) {
          switch(bloomType) {
          case ROW:   
            newKey = ! kvComparator.matchingRows(kv, lastKv); 
            break;
          case ROWCOL: 
            newKey = ! kvComparator.matchingRowColumn(kv, lastKv); 
            break;
          case NONE:
            newKey = false;
          }
        }
        if (newKey) {
          /*
           * http://2.bp.blogspot.com/_Cib_A77V54U/StZMrzaKufI/AAAAAAAAADo/ZhK7bGoJdMQ/s400/KeyValue.png
           * Key = RowLen + Row + FamilyLen + Column [Family + Qualifier] + TimeStamp
           *
           * 2 Types of Filtering:
           *  1. Row = Row 
           *  2. RowCol = Row + Qualifier
           */
          switch (bloomType) {
          case ROW:
            this.bloomFilter.add(kv.getBuffer(), kv.getRowOffset(), 
                kv.getRowLength());
            break;
          case ROWCOL:
            // merge(row, qualifier)
            int ro = kv.getRowOffset();
            int rl = kv.getRowLength();
            int qo = kv.getQualifierOffset();
            int ql = kv.getQualifierLength();
            byte [] result = new byte[rl + ql];
            System.arraycopy(kv.getBuffer(), ro, result, 0,  rl);
            System.arraycopy(kv.getBuffer(), qo, result, rl, ql);

            this.bloomFilter.add(result);
            break;
          default:
          }
          this.lastKv = kv;
        }
      }
      super.append(kv);
    }

    @Override
    public void append(final byte [] key, final byte [] value)
    throws IOException {
      if (this.bloomFilter != null) {
        // only add to the bloom filter on a new row
        if(this.lastByteArray == null || !Arrays.equals(key, lastByteArray)) {
          this.bloomFilter.add(key);
          this.lastByteArray = key;
        }
      }
      super.append(key, value);
    }
    
    @Override
    public void close() 
    throws IOException {
      // make sure we wrote something to the bloom before adding it
      if (this.bloomFilter != null && this.bloomFilter.getKeyCount() > 0) {
        bloomFilter.finalize();
        if (this.bloomFilter.getMaxKeys() > 0) {
          int b = this.bloomFilter.getByteSize();
          int k = this.bloomFilter.getKeyCount();
          int m = this.bloomFilter.getMaxKeys();
          StoreFile.LOG.info("Bloom added to HFile.  " + b + "B, " + 
              k + "/" + m + " (" + NumberFormat.getPercentInstance().format(
                ((double)k) / ((double)m)) + ")");
        }
        appendMetaBlock(BLOOM_FILTER_META_KEY, bloomFilter.getMetaWriter());
        appendMetaBlock(BLOOM_FILTER_DATA_KEY, bloomFilter.getDataWriter());
        appendFileInfo(BLOOM_FILTER_TYPE_KEY, Bytes.toBytes(bloomType.toString()));
      }
      super.close();
    }
    
  }
  
  /**
   * Useful comparators for comparing StoreFiles.
   */
  abstract static class Comparators {
    /**
     * Comparator that compares based on the flush time of
     * the StoreFiles. All bulk loads are placed before all non-
     * bulk loads, and then all files are sorted by sequence ID.
     * If there are ties, the path name is used as a tie-breaker.
     */
    static final Comparator<StoreFile> FLUSH_TIME =
      Ordering.compound(ImmutableList.of(
          Ordering.natural().onResultOf(new GetBulkTime()),
          Ordering.natural().onResultOf(new GetSeqId()),
          Ordering.natural().onResultOf(new GetPathName())
      ));

    private static class GetBulkTime implements Function<StoreFile, Long> {
      @Override
      public Long apply(StoreFile sf) {
        if (!sf.isBulkLoadResult()) return Long.MAX_VALUE;
        return sf.getBulkLoadTimestamp();
      }
    }
    private static class GetSeqId implements Function<StoreFile, Long> {
      @Override
      public Long apply(StoreFile sf) {
        if (sf.isBulkLoadResult()) return -1L;
        return sf.getMaxSequenceId();
      }
    }
    private static class GetPathName implements Function<StoreFile, String> {
      @Override
      public String apply(StoreFile sf) {
        return sf.getPath().getName();
      }
    }

  }
}
