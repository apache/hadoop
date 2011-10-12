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

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.HFileWriterV1;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

/**
 * A Store data file.  Stores usually have one or more of these files.  They
 * are produced by flushing the memstore to disk.  To
 * create, call {@link #createWriter(FileSystem, Path, int, Configuration)}
 * and append data. Be sure to add any metadata before calling close on the
 * Writer (Use the appendMetadata convenience methods). On close, a StoreFile
 * is sitting in the Filesystem.  To refer to it, create a StoreFile instance
 * passing filesystem and path.  To read, call {@link #createReader()}.
 * <p>StoreFiles may also reference store files in another Store.
 *
 * The reason for this weird pattern where you use a different instance for the
 * writer and a reader is that we write once but read a lot more.
 */
public class StoreFile {
  static final Log LOG = LogFactory.getLog(StoreFile.class.getName());

  static final String HFILE_BLOCK_CACHE_SIZE_KEY = "hfile.block.cache.size";

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

  // Keys for fileinfo values in HFile

  /** Max Sequence ID in FileInfo */
  public static final byte [] MAX_SEQ_ID_KEY = Bytes.toBytes("MAX_SEQ_ID_KEY");

  /** Major compaction flag in FileInfo */
  public static final byte[] MAJOR_COMPACTION_KEY =
      Bytes.toBytes("MAJOR_COMPACTION_KEY");

  /** Bloom filter Type in FileInfo */
  static final byte[] BLOOM_FILTER_TYPE_KEY =
      Bytes.toBytes("BLOOM_FILTER_TYPE");

  /** Last Bloom filter key in FileInfo */
  private static final byte[] LAST_BLOOM_KEY = Bytes.toBytes("LAST_BLOOM_KEY");

  /** Key for Timerange information in metadata*/
  public static final byte[] TIMERANGE_KEY = Bytes.toBytes("TIMERANGE");

  // Make default block size for StoreFiles 8k while testing.  TODO: FIX!
  // Need to make it 8k for testing.
  public static final int DEFAULT_BLOCKSIZE_SMALL = 8 * 1024;


  private static BlockCache hfileBlockCache = null;

  private final FileSystem fs;

  // This file's path.
  private final Path path;

  // If this storefile references another, this is the reference instance.
  private Reference reference;

  // If this StoreFile references another, this is the other files path.
  private Path referencePath;

  // Block cache configuration and reference.
  private final CacheConfig cacheConf;

  // HDFS blocks distribuion information
  private HDFSBlocksDistribution hdfsBlocksDistribution;

  // Keys for metadata stored in backing HFile.
  // Set when we obtain a Reader.
  private long sequenceid = -1;

  // If true, this file was product of a major compaction.  Its then set
  // whenever you get a Reader.
  private AtomicBoolean majorCompaction = null;

  /** Meta key set when store file is a result of a bulk load */
  public static final byte[] BULKLOAD_TASK_KEY =
    Bytes.toBytes("BULKLOAD_SOURCE_TASK");
  public static final byte[] BULKLOAD_TIME_KEY =
    Bytes.toBytes("BULKLOAD_TIMESTAMP");

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

  // StoreFile.Reader
  private volatile Reader reader;

  // Used making file ids.
  private final static Random rand = new Random();
  private final Configuration conf;

  /**
   * Bloom filter type specified in column family configuration. Does not
   * necessarily correspond to the Bloom filter type present in the HFile.
   */
  private final BloomType cfBloomType;

  // the last modification time stamp
  private long modificationTimeStamp = 0L;

  /**
   * Constructor, loads a reader and it's indices, etc. May allocate a
   * substantial amount of ram depending on the underlying files (10-20MB?).
   *
   * @param fs  The current file system to use.
   * @param p  The path of the file.
   * @param blockcache  <code>true</code> if the block cache is enabled.
   * @param conf  The current configuration.
   * @param cacheConf  The cache configuration and block cache reference.
   * @param cfBloomType The bloom type to use for this store file as specified
   *          by column family configuration. This may or may not be the same
   *          as the Bloom filter type actually present in the HFile, because
   *          column family configuration might change. If this is
   *          {@link BloomType#NONE}, the existing Bloom filter is ignored.
   * @throws IOException When opening the reader fails.
   */
  StoreFile(final FileSystem fs,
            final Path p,
            final Configuration conf,
            final CacheConfig cacheConf,
            final BloomType cfBloomType)
      throws IOException {
    this.conf = conf;
    this.fs = fs;
    this.path = p;
    this.cacheConf = cacheConf;
    if (isReference(p)) {
      this.reference = Reference.read(fs, p);
      this.referencePath = getReferredToFile(this.path);
    }

    if (BloomFilterFactory.isBloomEnabled(conf)) {
      this.cfBloomType = cfBloomType;
    } else {
      LOG.info("Ignoring bloom filter check for file " + path + ": " +
          "cfBloomType=" + cfBloomType + " (disabled in config)");
      this.cfBloomType = BloomType.NONE;
    }

    // cache the modification time stamp of this store file
    FileStatus[] stats = fs.listStatus(p);
    if (stats != null && stats.length == 1) {
      this.modificationTimeStamp = stats[0].getModificationTime();
    } else {
      this.modificationTimeStamp = 0;
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
    return this.sequenceid;
  }

  public long getModificationTimeStamp() {
    return modificationTimeStamp;
  }

  /**
   * Return the highest sequence ID found across all storefiles in
   * the given list. Store files that were created by a mapreduce
   * bulk load are ignored, as they do not correspond to any edit
   * log items.
   * @return 0 if no non-bulk-load files are provided or, this is Store that
   * does not yet have any store files.
   */
  public static long getMaxSequenceIdInList(Collection<StoreFile> sfs) {
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
   * @return the cached value of HDFS blocks distribution. The cached value is
   * calculated when store file is opened.
   */
  public HDFSBlocksDistribution getHDFSBlockDistribution() {
    return this.hdfsBlocksDistribution;
  }

  /**
   * helper function to compute HDFS blocks distribution of a given reference
   * file.For reference file, we don't compute the exact value. We use some
   * estimate instead given it might be good enough. we assume bottom part
   * takes the first half of reference file, top part takes the second half
   * of the reference file. This is just estimate, given
   * midkey ofregion != midkey of HFile, also the number and size of keys vary.
   * If this estimate isn't good enough, we can improve it later.
   * @param fs  The FileSystem
   * @param reference  The reference
   * @param reference  The referencePath
   * @return HDFS blocks distribution
   */
  static private HDFSBlocksDistribution computeRefFileHDFSBlockDistribution(
    FileSystem fs, Reference reference, Path referencePath) throws IOException {
    if ( referencePath == null) {
      return null;
    }

    FileStatus status = fs.getFileStatus(referencePath);
    long start = 0;
    long length = 0;

    if (Reference.isTopFileRegion(reference.getFileRegion())) {
      start = status.getLen()/2;
      length = status.getLen() - status.getLen()/2;
    } else {
      start = 0;
      length = status.getLen()/2;
    }
    return FSUtils.computeHDFSBlocksDistribution(fs, status, start, length);
  }

  /**
   * helper function to compute HDFS blocks distribution of a given file.
   * For reference file, it is an estimate
   * @param fs  The FileSystem
   * @param o  The path of the file
   * @return HDFS blocks distribution
   */
  static public HDFSBlocksDistribution computeHDFSBlockDistribution(
    FileSystem fs, Path p) throws IOException {
    if (isReference(p)) {
      Reference reference = Reference.read(fs, p);
      Path referencePath = getReferredToFile(p);
      return computeRefFileHDFSBlockDistribution(fs, reference, referencePath);
    } else {
      FileStatus status = fs.getFileStatus(p);
      long length = status.getLen();
      return FSUtils.computeHDFSBlocksDistribution(fs, status, 0, length);
    }
  }


  /**
   * compute HDFS block distribution, for reference file, it is an estimate
   */
  private void computeHDFSBlockDistribution() throws IOException {
    if (isReference()) {
      this.hdfsBlocksDistribution = computeRefFileHDFSBlockDistribution(
        this.fs, this.reference, this.referencePath);
    } else {
      FileStatus status = this.fs.getFileStatus(this.path);
      long length = status.getLen();
      this.hdfsBlocksDistribution = FSUtils.computeHDFSBlocksDistribution(
        this.fs, status, 0, length);
    }
  }

  /**
   * Opens reader on this store file.  Called by Constructor.
   * @return Reader for the store file.
   * @throws IOException
   * @see #closeReader()
   */
  private Reader open() throws IOException {
    if (this.reader != null) {
      throw new IllegalAccessError("Already open");
    }
    if (isReference()) {
      this.reader = new HalfStoreFileReader(this.fs, this.referencePath,
          this.cacheConf, this.reference);
    } else {
      this.reader = new Reader(this.fs, this.path, this.cacheConf);
    }

    computeHDFSBlockDistribution();

    // Load up indices and fileinfo. This also loads Bloom filter type.
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
    this.reader.setSequenceID(this.sequenceid);

    b = metadataMap.get(MAJOR_COMPACTION_KEY);
    if (b != null) {
      boolean mc = Bytes.toBoolean(b);
      if (this.majorCompaction == null) {
        this.majorCompaction = new AtomicBoolean(mc);
      } else {
        this.majorCompaction.set(mc);
      }
    } else {
      // Presume it is not major compacted if it doesn't explicity say so
      // HFileOutputFormat explicitly sets the major compacted key.
      this.majorCompaction = new AtomicBoolean(false);
    }

    BloomType hfileBloomType = reader.getBloomFilterType();
    if (cfBloomType != BloomType.NONE) {
      reader.loadBloomfilter();
      if (hfileBloomType != cfBloomType) {
        LOG.info("HFile Bloom filter type for "
            + reader.getHFileReader().getName() + ": " + hfileBloomType
            + ", but " + cfBloomType + " specified in column family "
            + "configuration");
      }
    } else if (hfileBloomType != BloomType.NONE) {
      LOG.info("Bloom filter turned off by CF config for "
          + reader.getHFileReader().getName());
    }

    try {
      byte [] timerangeBytes = metadataMap.get(TIMERANGE_KEY);
      if (timerangeBytes != null) {
        this.reader.timeRangeTracker = new TimeRangeTracker();
        Writables.copyWritable(timerangeBytes, this.reader.timeRangeTracker);
      }
    } catch (IllegalArgumentException e) {
      LOG.error("Error reading timestamp range data from meta -- " +
          "proceeding without", e);
      this.reader.timeRangeTracker = null;
    }
    return this.reader;
  }

  /**
   * @return Reader for StoreFile. creates if necessary
   * @throws IOException
   */
  public Reader createReader() throws IOException {
    if (this.reader == null) {
      this.reader = open();
    }
    return this.reader;
  }

  /**
   * @return Current reader.  Must call createReader first else returns null.
   * @throws IOException
   * @see #createReader()
   */
  public Reader getReader() {
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
  public static Path rename(final FileSystem fs,
                            final Path src,
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
   *
   * @param fs
   * @param dir Path to family directory.  Makes the directory if doesn't exist.
   * Creates a file with a unique name in this directory.
   * @param blocksize size per filesystem block
   * @return StoreFile.Writer
   * @throws IOException
   */
  public static Writer createWriter(final FileSystem fs, final Path dir,
      final int blocksize, Configuration conf, CacheConfig cacheConf)
  throws IOException {
    return createWriter(fs, dir, blocksize, null, null, conf, cacheConf,
        BloomType.NONE, 0);
  }

  /**
   * Create a store file writer. Client is responsible for closing file when done.
   * If metadata, add BEFORE closing using appendMetadata()
   * @param fs
   * @param dir Path to family directory.  Makes the directory if doesn't exist.
   * Creates a file with a unique name in this directory.
   * @param blocksize
   * @param algorithm Pass null to get default.
   * @param c Pass null to get default.
   * @param conf HBase system configuration. used with bloom filters
   * @param cacheConf Cache configuration and reference.
   * @param bloomType column family setting for bloom filters
   * @param maxKeyCount estimated maximum number of keys we expect to add
   * @return HFile.Writer
   * @throws IOException
   */
  public static StoreFile.Writer createWriter(final FileSystem fs,
                                              final Path dir,
                                              final int blocksize,
                                              final Compression.Algorithm algorithm,
                                              final KeyValue.KVComparator c,
                                              final Configuration conf,
                                              final CacheConfig cacheConf,
                                              BloomType bloomType,
                                              long maxKeyCount)
      throws IOException {

    if (!fs.exists(dir)) {
      fs.mkdirs(dir);
    }
    Path path = getUniqueFile(fs, dir);
    if (!BloomFilterFactory.isBloomEnabled(conf)) {
      bloomType = BloomType.NONE;
    }

    return new Writer(fs, path, blocksize,
        algorithm == null? HFile.DEFAULT_COMPRESSION_ALGORITHM: algorithm,
        conf, cacheConf, c == null ? KeyValue.COMPARATOR: c, bloomType,
        maxKeyCount);
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
  static Path getRandomFilename(final FileSystem fs,
                                final Path dir,
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

  /**
   * Write out a split reference.
   *
   * Package local so it doesnt leak out of regionserver.
   *
   * @param fs
   * @param splitDir Presumes path format is actually
   * <code>SOME_DIRECTORY/REGIONNAME/FAMILY</code>.
   * @param f File to split.
   * @param splitRow
   * @param range
   * @return Path to created reference.
   * @throws IOException
   */
  static Path split(final FileSystem fs,
                    final Path splitDir,
                    final StoreFile f,
                    final byte [] splitRow,
                    final Reference.Range range)
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


  /**
   * A StoreFile writer.  Use this to read/write HBase Store Files. It is package
   * local because it is an implementation detail of the HBase regionserver.
   */
  public static class Writer {
    private final BloomFilterWriter bloomFilterWriter;
    private final BloomType bloomType;
    private byte[] lastBloomKey;
    private int lastBloomKeyOffset, lastBloomKeyLen;
    private KVComparator kvComparator;
    private KeyValue lastKv = null;

    TimeRangeTracker timeRangeTracker = new TimeRangeTracker();
    /* isTimeRangeTrackerSet keeps track if the timeRange has already been set
     * When flushing a memstore, we set TimeRange and use this variable to
     * indicate that it doesn't need to be calculated again while
     * appending KeyValues.
     * It is not set in cases of compactions when it is recalculated using only
     * the appended KeyValues*/
    boolean isTimeRangeTrackerSet = false;

    protected HFile.Writer writer;
    /**
     * Creates an HFile.Writer that also write helpful meta data.
     * @param fs file system to write to
     * @param path file name to create
     * @param blocksize HDFS block size
     * @param compress HDFS block compression
     * @param conf user configuration
     * @param comparator key comparator
     * @param bloomType bloom filter setting
     * @param maxKeys the expected maximum number of keys to be added. Was used
     *        for Bloom filter size in {@link HFile} format version 1.
     * @throws IOException problem writing to FS
     */
    public Writer(FileSystem fs, Path path, int blocksize,
        Compression.Algorithm compress, final Configuration conf,
        CacheConfig cacheConf,
        final KVComparator comparator, BloomType bloomType, long maxKeys)
        throws IOException {
      writer = HFile.getWriterFactory(conf).createWriter(
          fs, path, blocksize,
          compress, comparator.getRawComparator());

      this.kvComparator = comparator;

      bloomFilterWriter = BloomFilterFactory.createBloomAtWrite(conf, cacheConf,
          bloomType, (int) Math.min(maxKeys, Integer.MAX_VALUE), writer);
      if (bloomFilterWriter != null) {
        this.bloomType = bloomType;
        LOG.info("Bloom filter type for " + path + ": " + this.bloomType +
            ", "+ bloomFilterWriter.getClass().getSimpleName());
      } else {
        // Not using Bloom filters.
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
    public void appendMetadata(final long maxSequenceId, final boolean majorCompaction)
    throws IOException {
      writer.appendFileInfo(MAX_SEQ_ID_KEY, Bytes.toBytes(maxSequenceId));
      writer.appendFileInfo(MAJOR_COMPACTION_KEY,
          Bytes.toBytes(majorCompaction));
      appendTimeRangeMetadata();
    }

    /**
     * Add TimestampRange to Metadata
     */
    public void appendTimeRangeMetadata() throws IOException {
      appendFileInfo(TIMERANGE_KEY,WritableUtils.toByteArray(timeRangeTracker));
    }

    /**
     * Set TimeRangeTracker
     * @param trt
     */
    public void setTimeRangeTracker(final TimeRangeTracker trt) {
      this.timeRangeTracker = trt;
      isTimeRangeTrackerSet = true;
    }

    /**
     * If the timeRangeTracker is not set,
     * update TimeRangeTracker to include the timestamp of this key
     * @param kv
     * @throws IOException
     */
    public void includeInTimeRangeTracker(final KeyValue kv) {
      if (!isTimeRangeTrackerSet) {
        timeRangeTracker.includeTimestamp(kv);
      }
    }

    /**
     * If the timeRangeTracker is not set,
     * update TimeRangeTracker to include the timestamp of this key
     * @param key
     * @throws IOException
     */
    public void includeInTimeRangeTracker(final byte [] key) {
      if (!isTimeRangeTrackerSet) {
        timeRangeTracker.includeTimestamp(key);
      }
    }

    public void append(final KeyValue kv) throws IOException {
      if (this.bloomFilterWriter != null) {
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
            break;
          default:
            throw new IOException("Invalid Bloom filter type: " + bloomType);
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
          byte[] bloomKey;
          int bloomKeyOffset, bloomKeyLen;

          switch (bloomType) {
          case ROW:
            bloomKey = kv.getBuffer();
            bloomKeyOffset = kv.getRowOffset();
            bloomKeyLen = kv.getRowLength();
            break;
          case ROWCOL:
            // merge(row, qualifier)
            // TODO: could save one buffer copy in case of compound Bloom
            // filters when this involves creating a KeyValue
            bloomKey = bloomFilterWriter.createBloomKey(kv.getBuffer(),
                kv.getRowOffset(), kv.getRowLength(), kv.getBuffer(),
                kv.getQualifierOffset(), kv.getQualifierLength());
            bloomKeyOffset = 0;
            bloomKeyLen = bloomKey.length;
            break;
          default:
            throw new IOException("Invalid Bloom filter type: " + bloomType +
                " (ROW or ROWCOL expected)");
          }
          bloomFilterWriter.add(bloomKey, bloomKeyOffset, bloomKeyLen);
          if (lastBloomKey != null
              && bloomFilterWriter.getComparator().compare(bloomKey,
                  bloomKeyOffset, bloomKeyLen, lastBloomKey,
                  lastBloomKeyOffset, lastBloomKeyLen) <= 0) {
            throw new IOException("Non-increasing Bloom keys: "
                + Bytes.toStringBinary(bloomKey, bloomKeyOffset, bloomKeyLen)
                + " after "
                + Bytes.toStringBinary(lastBloomKey, lastBloomKeyOffset,
                    lastBloomKeyLen));
          }
          lastBloomKey = bloomKey;
          lastBloomKeyOffset = bloomKeyOffset;
          lastBloomKeyLen = bloomKeyLen;
          this.lastKv = kv;
        }
      }
      writer.append(kv);
      includeInTimeRangeTracker(kv);
    }

    public Path getPath() {
      return this.writer.getPath();
    }

    boolean hasBloom() {
      return this.bloomFilterWriter != null;
    }

    /**
     * For unit testing only.
     * @return the Bloom filter used by this writer.
     */
    BloomFilterWriter getBloomWriter() {
      return bloomFilterWriter;
    }

    public void close() throws IOException {
      // Make sure we wrote something to the Bloom filter before adding it.
      boolean haveBloom = bloomFilterWriter != null &&
          bloomFilterWriter.getKeyCount() > 0;
      if (haveBloom) {
        bloomFilterWriter.compactBloom();
        writer.addBloomFilter(bloomFilterWriter);
        writer.appendFileInfo(BLOOM_FILTER_TYPE_KEY,
            Bytes.toBytes(bloomType.toString()));
        if (lastBloomKey != null) {
          writer.appendFileInfo(LAST_BLOOM_KEY, Arrays.copyOfRange(
              lastBloomKey, lastBloomKeyOffset, lastBloomKeyOffset
                  + lastBloomKeyLen));
        }
      }
      writer.close();

      // Log final Bloom filter statistics. This needs to be done after close()
      // because compound Bloom filters might be finalized as part of closing.
      if (haveBloom && bloomFilterWriter.getMaxKeys() > 0) {
        StoreFile.LOG.info("Bloom added to HFile ("
            + getPath() + "): " +
            bloomFilterWriter.toString().replace("\n", "; "));
      }
    }

    public void appendFileInfo(byte[] key, byte[] value) throws IOException {
      writer.appendFileInfo(key, value);
    }
  }

  /**
   * Reader for a StoreFile.
   */
  public static class Reader {
    static final Log LOG = LogFactory.getLog(Reader.class.getName());

    protected BloomFilter bloomFilter = null;
    protected BloomType bloomFilterType;
    private final HFile.Reader reader;
    protected TimeRangeTracker timeRangeTracker = null;
    protected long sequenceID = -1;
    private byte[] lastBloomKey;

    public Reader(FileSystem fs, Path path, CacheConfig cacheConf)
        throws IOException {
      reader = HFile.createReader(fs, path, cacheConf);
      bloomFilterType = BloomType.NONE;
    }

    /**
     * ONLY USE DEFAULT CONSTRUCTOR FOR UNIT TESTS
     */
    Reader() {
      this.reader = null;
    }

    public RawComparator<byte []> getComparator() {
      return reader.getComparator();
    }

    /**
     * Get a scanner to scan over this StoreFile.
     *
     * @param cacheBlocks should this scanner cache blocks?
     * @param pread use pread (for highly concurrent small readers)
     * @return a scanner
     */
    public StoreFileScanner getStoreFileScanner(boolean cacheBlocks, boolean pread) {
      return new StoreFileScanner(this, getScanner(cacheBlocks, pread));
    }

    /**
     * Warning: Do not write further code which depends on this call. Instead
     * use getStoreFileScanner() which uses the StoreFileScanner class/interface
     * which is the preferred way to scan a store with higher level concepts.
     *
     * @param cacheBlocks should we cache the blocks?
     * @param pread use pread (for concurrent small readers)
     * @return the underlying HFileScanner
     */
    @Deprecated
    public HFileScanner getScanner(boolean cacheBlocks, boolean pread) {
      return reader.getScanner(cacheBlocks, pread);
    }

    public void close() throws IOException {
      reader.close();
    }

    public boolean shouldSeek(Scan scan, final SortedSet<byte[]> columns) {
      return (passesTimerangeFilter(scan) && passesBloomFilter(scan, columns));
    }

    /**
     * Check if this storeFile may contain keys within the TimeRange
     * @param scan
     * @return False if it definitely does not exist in this StoreFile
     */
    private boolean passesTimerangeFilter(Scan scan) {
      if (timeRangeTracker == null) {
        return true;
      } else {
        return timeRangeTracker.includesTimeRange(scan.getTimeRange());
      }
    }

    /**
     * Checks whether the given scan passes the Bloom filter (if present). Only
     * checks Bloom filters for single-row or single-row-column scans. Bloom
     * filter checking for multi-gets is implemented as part of the store
     * scanner system (see {@link StoreFileScanner#seekExactly}) and uses
     * the lower-level API {@link #passesBloomFilter(byte[], int, int, byte[],
     * int, int)}.
     *
     * @param scan the scan specification. Used to determine the row, and to
     *          check whether this is a single-row ("get") scan.
     * @param columns the set of columns. Only used for row-column Bloom
     *          filters.
     * @return true if the scan with the given column set passes the Bloom
     *         filter, or if the Bloom filter is not applicable for the scan.
     *         False if the Bloom filter is applicable and the scan fails it.
     */
    private boolean passesBloomFilter(Scan scan,
        final SortedSet<byte[]> columns) {
      // Multi-column non-get scans will use Bloom filters through the
      // lower-level API function that this function calls.
      if (!scan.isGetScan()) {
        return true;
      }

      byte[] row = scan.getStartRow();
      switch (this.bloomFilterType) {
        case ROW:
          return passesBloomFilter(row, 0, row.length, null, 0, 0);

        case ROWCOL:
          if (columns != null && columns.size() == 1) {
            byte[] column = columns.first();
            return passesBloomFilter(row, 0, row.length, column, 0, 
                column.length);
          }

          // For multi-column queries the Bloom filter is checked from the
          // seekExact operation.
          return true;

        default:
          return true;
      }      
    }

    /**
     * A method for checking Bloom filters. Called directly from
     * {@link StoreFileScanner} in case of a multi-column query.
     *
     * @param row
     * @param rowOffset
     * @param rowLen
     * @param col
     * @param colOffset
     * @param colLen
     * @return
     */
    public boolean passesBloomFilter(byte[] row, int rowOffset, int rowLen,
        byte[] col, int colOffset, int colLen) {
      if (bloomFilter == null)
        return true;

      byte[] key;
      switch (bloomFilterType) { 
        case ROW:
          if (col != null) {
            throw new RuntimeException("Row-only Bloom filter called with " +
                "column specified");
          }
          if (rowOffset != 0 || rowLen != row.length) {
              throw new AssertionError("For row-only Bloom filters the row "
                  + "must occupy the whole array");
          }
          key = row;
          break;

        case ROWCOL:
          key = bloomFilter.createBloomKey(row, rowOffset, rowLen, col,
              colOffset, colLen);
          break;

        default:
          return true;
      }

      // Cache Bloom filter as a local variable in case it is set to null by
      // another thread on an IO error.
      BloomFilter bloomFilter = this.bloomFilter;

      if (bloomFilter == null) {
        return true;
      }

      // Empty file?
      if (reader.getTrailer().getEntryCount() == 0)
        return false;

      try {
        boolean shouldCheckBloom;
        ByteBuffer bloom;
        if (bloomFilter.supportsAutoLoading()) {
          bloom = null;
          shouldCheckBloom = true;
        } else {
          bloom = reader.getMetaBlock(HFileWriterV1.BLOOM_FILTER_DATA_KEY,
              true);
          shouldCheckBloom = bloom != null;
        }

        if (shouldCheckBloom) {
          boolean exists;

          // Whether the primary Bloom key is greater than the last Bloom key
          // from the file info. For row-column Bloom filters this is not yet
          // a sufficient condition to return false.
          boolean keyIsAfterLast = lastBloomKey != null
              && bloomFilter.getComparator().compare(key, lastBloomKey) > 0;

          if (bloomFilterType == BloomType.ROWCOL) {
            // Since a Row Delete is essentially a DeleteFamily applied to all
            // columns, a file might be skipped if using row+col Bloom filter.
            // In order to ensure this file is included an additional check is
            // required looking only for a row bloom.
            byte[] rowBloomKey = bloomFilter.createBloomKey(row, 0, row.length,
                null, 0, 0);

            if (keyIsAfterLast
                && bloomFilter.getComparator().compare(rowBloomKey,
                    lastBloomKey) > 0) {
              exists = false;
            } else {
              exists =
                  this.bloomFilter.contains(key, 0, key.length, bloom) ||
                  this.bloomFilter.contains(rowBloomKey, 0, rowBloomKey.length,
                      bloom);
            }
          } else {
            exists = !keyIsAfterLast
                && this.bloomFilter.contains(key, 0, key.length, bloom);
          }

          return exists;
        }
      } catch (IOException e) {
        LOG.error("Error reading bloom filter data -- proceeding without",
            e);
        setBloomFilterFaulty();
      } catch (IllegalArgumentException e) {
        LOG.error("Bad bloom filter data -- proceeding without", e);
        setBloomFilterFaulty();
      }

      return true;
    }

    public Map<byte[], byte[]> loadFileInfo() throws IOException {
      Map<byte [], byte []> fi = reader.loadFileInfo();

      byte[] b = fi.get(BLOOM_FILTER_TYPE_KEY);
      if (b != null) {
        bloomFilterType = BloomType.valueOf(Bytes.toString(b));
      }

      lastBloomKey = fi.get(LAST_BLOOM_KEY);

      return fi;
    }

    public void loadBloomfilter() {
      if (this.bloomFilter != null) {
        return; // already loaded
      }

      try {
        DataInput bloomMeta = reader.getBloomFilterMetadata();
        if (bloomMeta != null) {
          if (bloomFilterType == BloomType.NONE) {
            throw new IOException(
                "valid bloom filter type not found in FileInfo");
          }

          bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);
          LOG.info("Loaded " + bloomFilterType + " " +
              bloomFilter.getClass().getSimpleName() + " metadata for " +
              reader.getName());
        }
      } catch (IOException e) {
        LOG.error("Error reading bloom filter meta -- proceeding without", e);
        this.bloomFilter = null;
      } catch (IllegalArgumentException e) {
        LOG.error("Bad bloom filter meta -- proceeding without", e);
        this.bloomFilter = null;
      }
    }

    /**
     * The number of Bloom filter entries in this store file, or an estimate
     * thereof, if the Bloom filter is not loaded. This always returns an upper
     * bound of the number of Bloom filter entries.
     *
     * @return an estimate of the number of Bloom filter entries in this file
     */
    public long getFilterEntries() {
      return bloomFilter != null ? bloomFilter.getKeyCount()
          : reader.getEntries();
    }

    public void setBloomFilterFaulty() {
      bloomFilter = null;
    }

    public byte[] getLastKey() {
      return reader.getLastKey();
    }

    public byte[] midkey() throws IOException {
      return reader.midkey();
    }

    public long length() {
      return reader.length();
    }

    public long getTotalUncompressedBytes() {
      return reader.getTrailer().getTotalUncompressedBytes();
    }

    public long getEntries() {
      return reader.getEntries();
    }

    public byte[] getFirstKey() {
      return reader.getFirstKey();
    }

    public long indexSize() {
      return reader.indexSize();
    }

    public BloomType getBloomFilterType() {
      return this.bloomFilterType;
    }

    public long getSequenceID() {
      return sequenceID;
    }

    public void setSequenceID(long sequenceID) {
      this.sequenceID = sequenceID;
    }

    BloomFilter getBloomFilter() {
      return bloomFilter;
    }

    long getUncompressedDataIndexSize() {
      return reader.getTrailer().getUncompressedDataIndexSize();
    }

    public long getTotalBloomSize() {
      if (bloomFilter == null)
        return 0;
      return bloomFilter.getByteSize();
    }

    public int getHFileVersion() {
      return reader.getTrailer().getVersion();
    }

    HFile.Reader getHFileReader() {
      return reader;
    }

    void disableBloomFilterForTesting() {
      bloomFilter = null;
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

    /**
     * FILE_SIZE = descending sort StoreFiles (largest --> smallest in size)
     */
    static final Comparator<StoreFile> FILE_SIZE =
      Ordering.natural().reverse().onResultOf(new Function<StoreFile, Long>() {
        @Override
        public Long apply(StoreFile sf) {
          return sf.getReader().length();
        }
      });
  }
}
