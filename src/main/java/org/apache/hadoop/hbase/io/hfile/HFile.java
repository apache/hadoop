/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KeyComparator;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

/**
 * File format for hbase.
 * A file of sorted key/value pairs. Both keys and values are byte arrays.
 * <p>
 * The memory footprint of a HFile includes the following (below is taken from the
 * <a
 * href=https://issues.apache.org/jira/browse/HADOOP-3315>TFile</a> documentation
 * but applies also to HFile):
 * <ul>
 * <li>Some constant overhead of reading or writing a compressed block.
 * <ul>
 * <li>Each compressed block requires one compression/decompression codec for
 * I/O.
 * <li>Temporary space to buffer the key.
 * <li>Temporary space to buffer the value.
 * </ul>
 * <li>HFile index, which is proportional to the total number of Data Blocks.
 * The total amount of memory needed to hold the index can be estimated as
 * (56+AvgKeySize)*NumBlocks.
 * </ul>
 * Suggestions on performance optimization.
 * <ul>
 * <li>Minimum block size. We recommend a setting of minimum block size between
 * 8KB to 1MB for general usage. Larger block size is preferred if files are
 * primarily for sequential access. However, it would lead to inefficient random
 * access (because there are more data to decompress). Smaller blocks are good
 * for random access, but require more memory to hold the block index, and may
 * be slower to create (because we must flush the compressor stream at the
 * conclusion of each data block, which leads to an FS I/O flush). Further, due
 * to the internal caching in Compression codec, the smallest possible block
 * size would be around 20KB-30KB.
 * <li>The current implementation does not offer true multi-threading for
 * reading. The implementation uses FSDataInputStream seek()+read(), which is
 * shown to be much faster than positioned-read call in single thread mode.
 * However, it also means that if multiple threads attempt to access the same
 * HFile (using multiple scanners) simultaneously, the actual I/O is carried out
 * sequentially even if they access different DFS blocks (Reexamine! pread seems
 * to be 10% faster than seek+read in my testing -- stack).
 * <li>Compression codec. Use "none" if the data is not very compressable (by
 * compressable, I mean a compression ratio at least 2:1). Generally, use "lzo"
 * as the starting point for experimenting. "gz" overs slightly better
 * compression ratio over "lzo" but requires 4x CPU to compress and 2x CPU to
 * decompress, comparing to "lzo".
 * </ul>
 *
 * For more on the background behind HFile, see <a
 * href=https://issues.apache.org/jira/browse/HBASE-61>HBASE-61</a>.
 * <p>
 * File is made of data blocks followed by meta data blocks (if any), a fileinfo
 * block, data block index, meta data block index, and a fixed size trailer
 * which records the offsets at which file changes content type.
 * <pre>&lt;data blocks>&lt;meta blocks>&lt;fileinfo>&lt;data index>&lt;meta index>&lt;trailer></pre>
 * Each block has a bit of magic at its start.  Block are comprised of
 * key/values.  In data blocks, they are both byte arrays.  Metadata blocks are
 * a String key and a byte array value.  An empty file looks like this:
 * <pre>&lt;fileinfo>&lt;trailer></pre>.  That is, there are not data nor meta
 * blocks present.
 * <p>
 * TODO: Do scanners need to be able to take a start and end row?
 * TODO: Should BlockIndex know the name of its file?  Should it have a Path
 * that points at its file say for the case where an index lives apart from
 * an HFile instance?
 */
public class HFile {
  static final Log LOG = LogFactory.getLog(HFile.class);

  /**
   * Maximum length of key in HFile.
   */
  public final static int MAXIMUM_KEY_LENGTH = Integer.MAX_VALUE;

  /**
   * Default block size for an HFile.
   */
  public final static int DEFAULT_BLOCKSIZE = 64 * 1024;

  /**
   * Default compression: none.
   */
  public final static Compression.Algorithm DEFAULT_COMPRESSION_ALGORITHM =
    Compression.Algorithm.NONE;

  /** Minimum supported HFile format version */
  public static final int MIN_FORMAT_VERSION = 1;

  /** Maximum supported HFile format version */
  public static final int MAX_FORMAT_VERSION = 2;

  /** Default compression name: none. */
  public final static String DEFAULT_COMPRESSION =
    DEFAULT_COMPRESSION_ALGORITHM.getName();

  /** Separator between HFile name and offset in block cache key */
  static final char CACHE_KEY_SEPARATOR = '_';

  // For measuring latency of "typical" reads and writes
  static volatile AtomicLong readOps = new AtomicLong();
  static volatile AtomicLong readTimeNano = new AtomicLong();
  static volatile AtomicLong writeOps = new AtomicLong();
  static volatile AtomicLong writeTimeNano = new AtomicLong();

  public static final long getReadOps() {
    return readOps.getAndSet(0);
  }

  public static final long getReadTimeMs() {
    return readTimeNano.getAndSet(0) / 1000000;
  }

  public static final long getWriteOps() {
    return writeOps.getAndSet(0);
  }

  public static final long getWriteTimeMs() {
    return writeTimeNano.getAndSet(0) / 1000000;
  }

  /** API required to write an {@link HFile} */
  public interface Writer extends Closeable {

    /** Add an element to the file info map. */
    void appendFileInfo(byte[] key, byte[] value) throws IOException;

    void append(KeyValue kv) throws IOException;

    void append(byte[] key, byte[] value) throws IOException;

    /** @return the path to this {@link HFile} */
    Path getPath();

    void appendMetaBlock(String bloomFilterMetaKey, Writable metaWriter);

    /**
     * Adds an inline block writer such as a multi-level block index writer or
     * a compound Bloom filter writer.
     */
    void addInlineBlockWriter(InlineBlockWriter bloomWriter);

    /**
     * Store Bloom filter in the file. This does not deal with Bloom filter
     * internals but is necessary, since Bloom filters are stored differently
     * in HFile version 1 and version 2.
     */
    void addBloomFilter(BloomFilterWriter bfw);
  }

  /**
   * This variety of ways to construct writers is used throughout the code, and
   * we want to be able to swap writer implementations.
   */
  public static abstract class WriterFactory {
    protected Configuration conf;

    WriterFactory(Configuration conf) { this.conf = conf; }

    public abstract Writer createWriter(FileSystem fs, Path path)
        throws IOException;

    public abstract Writer createWriter(FileSystem fs, Path path,
        int blockSize, Compression.Algorithm compress,
        final KeyComparator comparator) throws IOException;

    public abstract Writer createWriter(FileSystem fs, Path path,
        int blockSize, String compress,
        final KeyComparator comparator) throws IOException;

    public abstract Writer createWriter(final FSDataOutputStream ostream,
        final int blockSize, final String compress,
        final KeyComparator comparator) throws IOException;

    public abstract Writer createWriter(final FSDataOutputStream ostream,
        final int blockSize, final Compression.Algorithm compress,
        final KeyComparator c) throws IOException;
  }

  /** The configuration key for HFile version to use for new files */
  public static final String FORMAT_VERSION_KEY = "hfile.format.version";

  public static int getFormatVersion(Configuration conf) {
    int version = conf.getInt(FORMAT_VERSION_KEY, MAX_FORMAT_VERSION);
    checkFormatVersion(version);
    return version;
  }

  /**
   * Returns the factory to be used to create {@link HFile} writers. Should
   * always be {@link HFileWriterV2#WRITER_FACTORY_V2} in production, but
   * can also be {@link HFileWriterV1#WRITER_FACTORY_V1} in testing.
   */
  public static final WriterFactory getWriterFactory(Configuration conf) {
    int version = getFormatVersion(conf);
    LOG.debug("Using HFile format version " + version);
    switch (version) {
    case 1:
      return new HFileWriterV1.WriterFactoryV1(conf);
    case 2:
      return new HFileWriterV2.WriterFactoryV2(conf);
    default:
      throw new IllegalArgumentException("Cannot create writer for HFile " +
          "format version " + version);
    }
  }

  /**
   * Configuration key to evict all blocks of a given file from the block cache
   * when the file is closed.
   */
  public static final String EVICT_BLOCKS_ON_CLOSE_KEY =
      "hbase.rs.evictblocksonclose";

  /**
   * Configuration key to cache data blocks on write. There are separate
   * switches for Bloom blocks and non-root index blocks.
   */
  public static final String CACHE_BLOCKS_ON_WRITE_KEY =
      "hbase.rs.cacheblocksonwrite";

  /** An interface used by clients to open and iterate an {@link HFile}. */
  public interface Reader extends Closeable {

    /**
     * Returns this reader's "name". Usually the last component of the path.
     * Needs to be constant as the file is being moved to support caching on
     * write.
     */
     String getName();

     String getColumnFamilyName();

     RawComparator<byte []> getComparator();

     HFileScanner getScanner(boolean cacheBlocks,
        final boolean pread, final boolean isCompaction);

     ByteBuffer getMetaBlock(String metaBlockName,
        boolean cacheBlock) throws IOException;

     HFileBlock readBlock(long offset, int onDiskBlockSize,
         boolean cacheBlock, final boolean pread, final boolean isCompaction)
         throws IOException;

     Map<byte[], byte[]> loadFileInfo() throws IOException;

     byte[] getLastKey();

     byte[] midkey() throws IOException;

     long length();

     long getEntries();

     byte[] getFirstKey();

     long indexSize();

     byte[] getFirstRowKey();

     byte[] getLastRowKey();

     FixedFileTrailer getTrailer();

     HFileBlockIndex.BlockIndexReader getDataBlockIndexReader();

     HFileScanner getScanner(boolean cacheBlocks, boolean pread);

     Compression.Algorithm getCompressionAlgorithm();

    /**
     * Retrieves Bloom filter metadata as appropriate for each {@link HFile}
     * version. Knows nothing about how that metadata is structured.
     */
     DataInput getBloomFilterMetadata() throws IOException;

     Path getPath();
  }

  private static Reader pickReaderVersion(Path path, FSDataInputStream fsdis,
      long size, boolean closeIStream, BlockCache blockCache,
      boolean inMemory, boolean evictOnClose) throws IOException {
    FixedFileTrailer trailer = FixedFileTrailer.readFromStream(fsdis, size);
    switch (trailer.getVersion()) {
    case 1:
      return new HFileReaderV1(path, trailer, fsdis, size, closeIStream,
          blockCache, inMemory, evictOnClose);
    case 2:
      return new HFileReaderV2(path, trailer, fsdis, size, closeIStream,
          blockCache, inMemory, evictOnClose);
    default:
      throw new IOException("Cannot instantiate reader for HFile version " +
          trailer.getVersion());
    }
  }

  public static Reader createReader(
      FileSystem fs, Path path, BlockCache blockCache, boolean inMemory,
      boolean evictOnClose) throws IOException {
    return pickReaderVersion(path, fs.open(path),
        fs.getFileStatus(path).getLen(), true, blockCache, inMemory,
        evictOnClose);
  }

  public static Reader createReader(Path path, FSDataInputStream fsdis,
      long size, BlockCache blockache, boolean inMemory, boolean evictOnClose)
      throws IOException {
    return pickReaderVersion(path, fsdis, size, false, blockache, inMemory,
        evictOnClose);
  }

  /*
   * Metadata for this file.  Conjured by the writer.  Read in by the reader.
   */
  static class FileInfo extends HbaseMapWritable<byte [], byte []> {
    static final String RESERVED_PREFIX = "hfile.";
    static final byte[] RESERVED_PREFIX_BYTES = Bytes.toBytes(RESERVED_PREFIX);
    static final byte [] LASTKEY = Bytes.toBytes(RESERVED_PREFIX + "LASTKEY");
    static final byte [] AVG_KEY_LEN =
      Bytes.toBytes(RESERVED_PREFIX + "AVG_KEY_LEN");
    static final byte [] AVG_VALUE_LEN =
      Bytes.toBytes(RESERVED_PREFIX + "AVG_VALUE_LEN");
    static final byte [] COMPARATOR =
      Bytes.toBytes(RESERVED_PREFIX + "COMPARATOR");

    /**
     * Append the given key/value pair to the file info, optionally checking the
     * key prefix.
     *
     * @param k key to add
     * @param v value to add
     * @param checkPrefix whether to check that the provided key does not start
     *          with the reserved prefix
     * @return this file info object
     * @throws IOException if the key or value is invalid
     */
    public FileInfo append(final byte[] k, final byte[] v,
        final boolean checkPrefix) throws IOException {
      if (k == null || v == null) {
        throw new NullPointerException("Key nor value may be null");
      }
      if (checkPrefix && isReservedFileInfoKey(k)) {
        throw new IOException("Keys with a " + FileInfo.RESERVED_PREFIX
            + " are reserved");
      }
      put(k, v);
      return this;
    }

  }

  /** Return true if the given file info key is reserved for internal use. */
  public static boolean isReservedFileInfoKey(byte[] key) {
    return Bytes.startsWith(key, FileInfo.RESERVED_PREFIX_BYTES);
  }

  /**
   * Get names of supported compression algorithms. The names are acceptable by
   * HFile.Writer.
   *
   * @return Array of strings, each represents a supported compression
   *         algorithm. Currently, the following compression algorithms are
   *         supported.
   *         <ul>
   *         <li>"none" - No compression.
   *         <li>"gz" - GZIP compression.
   *         </ul>
   */
  public static String[] getSupportedCompressionAlgorithms() {
    return Compression.getSupportedAlgorithms();
  }

  // Utility methods.
  /*
   * @param l Long to convert to an int.
   * @return <code>l</code> cast as an int.
   */
  static int longToInt(final long l) {
    // Expecting the size() of a block not exceeding 4GB. Assuming the
    // size() will wrap to negative integer if it exceeds 2GB (From tfile).
    return (int)(l & 0x00000000ffffffffL);
  }

  /**
   * Returns all files belonging to the given region directory. Could return an
   * empty list.
   *
   * @param fs  The file system reference.
   * @param regionDir  The region directory to scan.
   * @return The list of files found.
   * @throws IOException When scanning the files fails.
   */
  static List<Path> getStoreFiles(FileSystem fs, Path regionDir)
      throws IOException {
    List<Path> res = new ArrayList<Path>();
    PathFilter dirFilter = new FSUtils.DirFilter(fs);
    FileStatus[] familyDirs = fs.listStatus(regionDir, dirFilter);
    for(FileStatus dir : familyDirs) {
      FileStatus[] files = fs.listStatus(dir.getPath());
      for (FileStatus file : files) {
        if (!file.isDir()) {
          res.add(file.getPath());
        }
      }
    }
    return res;
  }

  public static void main(String[] args) throws IOException {
    HFilePrettyPrinter prettyPrinter = new HFilePrettyPrinter();
    System.exit(prettyPrinter.run(args));
  }

  public static String getBlockCacheKey(String hfileName, long offset) {
    return hfileName + CACHE_KEY_SEPARATOR + offset;
  }

  /**
   * Checks the given {@link HFile} format version, and throws an exception if
   * invalid. Note that if the version number comes from an input file and has
   * not been verified, the caller needs to re-throw an {@link IOException} to
   * indicate that this is not a software error, but corrupted input.
   *
   * @param version an HFile version
   * @throws IllegalArgumentException if the version is invalid
   */
  public static void checkFormatVersion(int version)
      throws IllegalArgumentException {
    if (version < MIN_FORMAT_VERSION || version > MAX_FORMAT_VERSION) {
      throw new IllegalArgumentException("Invalid HFile version: " + version
          + " (expected to be " + "between " + MIN_FORMAT_VERSION + " and "
          + MAX_FORMAT_VERSION + ")");
    }
  }

}
