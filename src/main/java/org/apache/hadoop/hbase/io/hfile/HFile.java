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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.KeyValue.KeyComparator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

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

  /* These values are more or less arbitrary, and they are used as a
   * form of check to make sure the file isn't completely corrupt.
   */
  final static byte [] DATABLOCKMAGIC =
    {'D', 'A', 'T', 'A', 'B', 'L', 'K', 42 };
  final static byte [] INDEXBLOCKMAGIC =
    { 'I', 'D', 'X', 'B', 'L', 'K', 41, 43 };
  final static byte [] METABLOCKMAGIC =
    { 'M', 'E', 'T', 'A', 'B', 'L', 'K', 99 };
  final static byte [] TRAILERBLOCKMAGIC =
    { 'T', 'R', 'A', 'B', 'L', 'K', 34, 36 };

  /**
   * Maximum length of key in HFile.
   */
  public final static int MAXIMUM_KEY_LENGTH = Integer.MAX_VALUE;

  /**
   * Default blocksize for hfile.
   */
  public final static int DEFAULT_BLOCKSIZE = 64 * 1024;

  /**
   * Default compression: none.
   */
  public final static Compression.Algorithm DEFAULT_COMPRESSION_ALGORITHM =
    Compression.Algorithm.NONE;
  /** Default compression name: none. */
  public final static String DEFAULT_COMPRESSION =
    DEFAULT_COMPRESSION_ALGORITHM.getName();

  // For measuring latency of "typical" reads and writes
  private static volatile long readOps;
  private static volatile long readTime;
  private static volatile long writeOps;
  private static volatile long writeTime;

  public static final long getReadOps() {
    long ret = readOps;
    readOps = 0;
    return ret;
  }

  public static final long getReadTime() {
    long ret = readTime;
    readTime = 0;
    return ret;
  }

  public static final long getWriteOps() {
    long ret = writeOps;
    writeOps = 0;
    return ret;
  }

  public static final long getWriteTime() {
    long ret = writeTime;
    writeTime = 0;
    return ret;
  }

  /**
   * HFile Writer.
   */
  public static class Writer implements Closeable {
    // FileSystem stream to write on.
    private FSDataOutputStream outputStream;
    // True if we opened the <code>outputStream</code> (and so will close it).
    private boolean closeOutputStream;

    // Name for this object used when logging or in toString.  Is either
    // the result of a toString on stream or else toString of passed file Path.
    protected String name;

    // Total uncompressed bytes, maybe calculate a compression ratio later.
    private long totalBytes = 0;

    // Total # of key/value entries, ie: how many times add() was called.
    private int entryCount = 0;

    // Used calculating average key and value lengths.
    private long keylength = 0;
    private long valuelength = 0;

    // Used to ensure we write in order.
    private final RawComparator<byte []> rawComparator;

    // A stream made per block written.
    private DataOutputStream out;

    // Number of uncompressed bytes per block.  Reinitialized when we start
    // new block.
    private int blocksize;

    // Offset where the current block began.
    private long blockBegin;

    // First key in a block (Not first key in file).
    private byte [] firstKey = null;

    // Key previously appended.  Becomes the last key in the file.
    private byte [] lastKeyBuffer = null;
    private int lastKeyOffset = -1;
    private int lastKeyLength = -1;

    // See {@link BlockIndex}. Below four fields are used to write the block
    // index.
    ArrayList<byte[]> blockKeys = new ArrayList<byte[]>();
    // Block offset in backing stream.
    ArrayList<Long> blockOffsets = new ArrayList<Long>();
    // Raw (decompressed) data size.
    ArrayList<Integer> blockDataSizes = new ArrayList<Integer>();

    // Meta block system.
    private ArrayList<byte []> metaNames = new ArrayList<byte []>();
    private ArrayList<Writable> metaData = new ArrayList<Writable>();

    // Used compression.  Used even if no compression -- 'none'.
    private final Compression.Algorithm compressAlgo;
    private Compressor compressor;

    // Special datastructure to hold fileinfo.
    private FileInfo fileinfo = new FileInfo();

    // May be null if we were passed a stream.
    private Path path = null;

    /**
     * Constructor that uses all defaults for compression and block size.
     * @param fs
     * @param path
     * @throws IOException
     */
    public Writer(FileSystem fs, Path path)
    throws IOException {
      this(fs, path, DEFAULT_BLOCKSIZE, (Compression.Algorithm) null, null);
    }

    /**
     * Constructor that takes a Path.
     * @param fs
     * @param path
     * @param blocksize
     * @param compress
     * @param comparator
     * @throws IOException
     * @throws IOException
     */
    public Writer(FileSystem fs, Path path, int blocksize,
      String compress, final KeyComparator comparator)
    throws IOException {
      this(fs, path, blocksize,
        compress == null? DEFAULT_COMPRESSION_ALGORITHM:
          Compression.getCompressionAlgorithmByName(compress),
        comparator);
    }

    /**
     * Constructor that takes a Path.
     * @param fs
     * @param path
     * @param blocksize
     * @param compress
     * @param comparator
     * @throws IOException
     */
    public Writer(FileSystem fs, Path path, int blocksize,
      Compression.Algorithm compress,
      final KeyComparator comparator)
    throws IOException {
      this(fs.create(path), blocksize, compress, comparator);
      this.closeOutputStream = true;
      this.name = path.toString();
      this.path = path;
    }

    /**
     * Constructor that takes a stream.
     * @param ostream Stream to use.
     * @param blocksize
     * @param compress
     * @param c RawComparator to use.
     * @throws IOException
     */
    public Writer(final FSDataOutputStream ostream, final int blocksize,
      final String  compress, final KeyComparator c)
    throws IOException {
      this(ostream, blocksize,
        Compression.getCompressionAlgorithmByName(compress), c);
    }

    /**
     * Constructor that takes a stream.
     * @param ostream Stream to use.
     * @param blocksize
     * @param compress
     * @param c
     * @throws IOException
     */
    public Writer(final FSDataOutputStream ostream, final int blocksize,
      final Compression.Algorithm  compress, final KeyComparator c)
    throws IOException {
      this.outputStream = ostream;
      this.closeOutputStream = false;
      this.blocksize = blocksize;
      this.rawComparator = c == null? Bytes.BYTES_RAWCOMPARATOR: c;
      this.name = this.outputStream.toString();
      this.compressAlgo = compress == null?
        DEFAULT_COMPRESSION_ALGORITHM: compress;
    }

    /*
     * If at block boundary, opens new block.
     * @throws IOException
     */
    private void checkBlockBoundary() throws IOException {
      if (this.out != null && this.out.size() < blocksize) return;
      finishBlock();
      newBlock();
    }

    /*
     * Do the cleanup if a current block.
     * @throws IOException
     */
    private void finishBlock() throws IOException {
      if (this.out == null) return;
      long now = System.currentTimeMillis();

      int size = releaseCompressingStream(this.out);
      this.out = null;
      blockKeys.add(firstKey);
      blockOffsets.add(Long.valueOf(blockBegin));
      blockDataSizes.add(Integer.valueOf(size));
      this.totalBytes += size;

      writeTime += System.currentTimeMillis() - now;
      writeOps++;
    }

    /*
     * Ready a new block for writing.
     * @throws IOException
     */
    private void newBlock() throws IOException {
      // This is where the next block begins.
      blockBegin = outputStream.getPos();
      this.out = getCompressingStream();
      this.out.write(DATABLOCKMAGIC);
      firstKey = null;
    }

    /*
     * Sets up a compressor and creates a compression stream on top of
     * this.outputStream.  Get one per block written.
     * @return A compressing stream; if 'none' compression, returned stream
     * does not compress.
     * @throws IOException
     * @see {@link #releaseCompressingStream(DataOutputStream)}
     */
    private DataOutputStream getCompressingStream() throws IOException {
      this.compressor = compressAlgo.getCompressor();
      // Get new DOS compression stream.  In tfile, the DOS, is not closed,
      // just finished, and that seems to be fine over there.  TODO: Check
      // no memory retention of the DOS.  Should I disable the 'flush' on the
      // DOS as the BCFile over in tfile does?  It wants to make it so flushes
      // don't go through to the underlying compressed stream.  Flush on the
      // compressed downstream should be only when done.  I was going to but
      // looks like when we call flush in here, its legitimate flush that
      // should go through to the compressor.
      OutputStream os =
        this.compressAlgo.createCompressionStream(this.outputStream,
        this.compressor, 0);
      return new DataOutputStream(os);
    }

    /*
     * Let go of block compressor and compressing stream gotten in call
     * {@link #getCompressingStream}.
     * @param dos
     * @return How much was written on this stream since it was taken out.
     * @see #getCompressingStream()
     * @throws IOException
     */
    private int releaseCompressingStream(final DataOutputStream dos)
    throws IOException {
      dos.flush();
      this.compressAlgo.returnCompressor(this.compressor);
      this.compressor = null;
      return dos.size();
    }

    /**
     * Add a meta block to the end of the file. Call before close().
     * Metadata blocks are expensive.  Fill one with a bunch of serialized data
     * rather than do a metadata block per metadata instance.  If metadata is
     * small, consider adding to file info using
     * {@link #appendFileInfo(byte[], byte[])}
     * @param metaBlockName name of the block
     * @param content will call readFields to get data later (DO NOT REUSE)
     */
    public void appendMetaBlock(String metaBlockName, Writable content) {
      byte[] key = Bytes.toBytes(metaBlockName);
      int i;
      for (i = 0; i < metaNames.size(); ++i) {
        // stop when the current key is greater than our own
        byte[] cur = metaNames.get(i);
        if (this.rawComparator.compare(cur, 0, cur.length, key, 0, key.length)
            > 0) {
          break;
        }
      }
      metaNames.add(i, key);
      metaData.add(i, content);
    }

    /**
     * Add to the file info.  Added key value can be gotten out of the return
     * from {@link Reader#loadFileInfo()}.
     * @param k Key
     * @param v Value
     * @throws IOException
     */
    public void appendFileInfo(final byte [] k, final byte [] v)
    throws IOException {
      appendFileInfo(this.fileinfo, k, v, true);
    }

    FileInfo appendFileInfo(FileInfo fi, final byte [] k, final byte [] v,
      final boolean checkPrefix)
    throws IOException {
      if (k == null || v == null) {
        throw new NullPointerException("Key nor value may be null");
      }
      if (checkPrefix &&
          Bytes.toString(k).toLowerCase().startsWith(FileInfo.RESERVED_PREFIX)) {
        throw new IOException("Keys with a " + FileInfo.RESERVED_PREFIX +
          " are reserved");
      }
      fi.put(k, v);
      return fi;
    }

    /**
     * @return Path or null if we were passed a stream rather than a Path.
     */
    public Path getPath() {
      return this.path;
    }

    @Override
    public String toString() {
      return "writer=" + this.name + ", compression=" +
        this.compressAlgo.getName();
    }

    /**
     * Add key/value to file.
     * Keys must be added in an order that agrees with the Comparator passed
     * on construction.
     * @param kv KeyValue to add.  Cannot be empty nor null.
     * @throws IOException
     */
    public void append(final KeyValue kv)
    throws IOException {
      append(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength(),
        kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
    }

    /**
     * Add key/value to file.
     * Keys must be added in an order that agrees with the Comparator passed
     * on construction.
     * @param key Key to add.  Cannot be empty nor null.
     * @param value Value to add.  Cannot be empty nor null.
     * @throws IOException
     */
    public void append(final byte [] key, final byte [] value)
    throws IOException {
      append(key, 0, key.length, value, 0, value.length);
    }

    /**
     * Add key/value to file.
     * Keys must be added in an order that agrees with the Comparator passed
     * on construction.
     * @param key
     * @param koffset
     * @param klength
     * @param value
     * @param voffset
     * @param vlength
     * @throws IOException
     */
    private void append(final byte [] key, final int koffset, final int klength,
        final byte [] value, final int voffset, final int vlength)
    throws IOException {
      boolean dupKey = checkKey(key, koffset, klength);
      checkValue(value, voffset, vlength);
      if (!dupKey) {
        checkBlockBoundary();
      }
      // Write length of key and value and then actual key and value bytes.
      this.out.writeInt(klength);
      this.keylength += klength;
      this.out.writeInt(vlength);
      this.valuelength += vlength;
      this.out.write(key, koffset, klength);
      this.out.write(value, voffset, vlength);
      // Are we the first key in this block?
      if (this.firstKey == null) {
        // Copy the key.
        this.firstKey = new byte [klength];
        System.arraycopy(key, koffset, this.firstKey, 0, klength);
      }
      this.lastKeyBuffer = key;
      this.lastKeyOffset = koffset;
      this.lastKeyLength = klength;
      this.entryCount ++;
    }

    /*
     * @param key Key to check.
     * @return the flag of duplicate Key or not
     * @throws IOException
     */
    private boolean checkKey(final byte [] key, final int offset, final int length)
    throws IOException {
      boolean dupKey = false;

      if (key == null || length <= 0) {
        throw new IOException("Key cannot be null or empty");
      }
      if (length > MAXIMUM_KEY_LENGTH) {
        throw new IOException("Key length " + length + " > " +
          MAXIMUM_KEY_LENGTH);
      }
      if (this.lastKeyBuffer != null) {
        int keyComp = this.rawComparator.compare(this.lastKeyBuffer, this.lastKeyOffset,
            this.lastKeyLength, key, offset, length);
        if (keyComp > 0) {
          throw new IOException("Added a key not lexically larger than" +
            " previous key=" + Bytes.toStringBinary(key, offset, length) +
            ", lastkey=" + Bytes.toStringBinary(this.lastKeyBuffer, this.lastKeyOffset,
                this.lastKeyLength));
        } else if (keyComp == 0) {
          dupKey = true;
        }
      }
      return dupKey;
    }

    private void checkValue(final byte [] value, final int offset,
        final int length) throws IOException {
      if (value == null) {
        throw new IOException("Value cannot be null");
      }
    }

    public long getTotalBytes() {
      return this.totalBytes;
    }

    public void close() throws IOException {
      if (this.outputStream == null) {
        return;
      }
      // Write out the end of the data blocks, then write meta data blocks.
      // followed by fileinfo, data block index and meta block index.

      finishBlock();

      FixedFileTrailer trailer = new FixedFileTrailer();

      // Write out the metadata blocks if any.
      ArrayList<Long> metaOffsets = null;
      ArrayList<Integer> metaDataSizes = null;
      if (metaNames.size() > 0) {
        metaOffsets = new ArrayList<Long>(metaNames.size());
        metaDataSizes = new ArrayList<Integer>(metaNames.size());
        for (int i = 0 ; i < metaNames.size() ; ++ i ) {
          // store the beginning offset
          long curPos = outputStream.getPos();
          metaOffsets.add(curPos);
          // write the metadata content
          DataOutputStream dos = getCompressingStream();
          dos.write(METABLOCKMAGIC);
          metaData.get(i).write(dos);
          int size = releaseCompressingStream(dos);
          // store the metadata size
          metaDataSizes.add(size);
        }
      }

      // Write fileinfo.
      trailer.fileinfoOffset = writeFileInfo(this.outputStream);

      // Write the data block index.
      trailer.dataIndexOffset = BlockIndex.writeIndex(this.outputStream,
        this.blockKeys, this.blockOffsets, this.blockDataSizes);

      // Meta block index.
      if (metaNames.size() > 0) {
        trailer.metaIndexOffset = BlockIndex.writeIndex(this.outputStream,
          this.metaNames, metaOffsets, metaDataSizes);
      }

      // Now finish off the trailer.
      trailer.dataIndexCount = blockKeys.size();
      trailer.metaIndexCount = metaNames.size();

      trailer.totalUncompressedBytes = totalBytes;
      trailer.entryCount = entryCount;

      trailer.compressionCodec = this.compressAlgo.ordinal();

      trailer.serialize(outputStream);

      if (this.closeOutputStream) {
        this.outputStream.close();
        this.outputStream = null;
      }
    }

    /*
     * Add last bits of metadata to fileinfo and then write it out.
     * Reader will be expecting to find all below.
     * @param o Stream to write on.
     * @return Position at which we started writing.
     * @throws IOException
     */
    private long writeFileInfo(FSDataOutputStream o) throws IOException {
      if (this.lastKeyBuffer != null) {
        // Make a copy.  The copy is stuffed into HMapWritable.  Needs a clean
        // byte buffer.  Won't take a tuple.
        byte [] b = new byte[this.lastKeyLength];
        System.arraycopy(this.lastKeyBuffer, this.lastKeyOffset, b, 0,
          this.lastKeyLength);
        appendFileInfo(this.fileinfo, FileInfo.LASTKEY, b, false);
      }
      int avgKeyLen = this.entryCount == 0? 0:
        (int)(this.keylength/this.entryCount);
      appendFileInfo(this.fileinfo, FileInfo.AVG_KEY_LEN,
        Bytes.toBytes(avgKeyLen), false);
      int avgValueLen = this.entryCount == 0? 0:
        (int)(this.valuelength/this.entryCount);
      appendFileInfo(this.fileinfo, FileInfo.AVG_VALUE_LEN,
        Bytes.toBytes(avgValueLen), false);
      appendFileInfo(this.fileinfo, FileInfo.COMPARATOR,
        Bytes.toBytes(this.rawComparator.getClass().getName()), false);
      long pos = o.getPos();
      this.fileinfo.write(o);
      return pos;
    }
  }

  /**
   * HFile Reader.
   */
  public static class Reader implements Closeable {
    // Stream to read from.
    private FSDataInputStream istream;
    // True if we should close istream when done.  We don't close it if we
    // didn't open it.
    private boolean closeIStream;

    // These are read in when the file info is loaded.
    HFile.BlockIndex blockIndex;
    private BlockIndex metaIndex;
    FixedFileTrailer trailer;
    private volatile boolean fileInfoLoaded = false;

    // Filled when we read in the trailer.
    private Compression.Algorithm compressAlgo;

    // Last key in the file.  Filled in when we read in the file info
    private byte [] lastkey = null;
    // Stats read in when we load file info.
    private int avgKeyLen = -1;
    private int avgValueLen = -1;

    // Used to ensure we seek correctly.
    RawComparator<byte []> comparator;

    // Size of this file.
    private final long fileSize;

    // Block cache to use.
    private final BlockCache cache;
    public int cacheHits = 0;
    public int blockLoads = 0;
    public int metaLoads = 0;

    // Whether file is from in-memory store
    private boolean inMemory = false;

    // Name for this object used when logging or in toString.  Is either
    // the result of a toString on the stream or else is toString of passed
    // file Path plus metadata key/value pairs.
    protected String name;

    /**
     * Opens a HFile.  You must load the file info before you can
     * use it by calling {@link #loadFileInfo()}.
     *
     * @param fs filesystem to load from
     * @param path path within said filesystem
     * @param cache block cache. Pass null if none.
     * @throws IOException
     */
    public Reader(FileSystem fs, Path path, BlockCache cache, boolean inMemory)
    throws IOException {
      this(fs.open(path), fs.getFileStatus(path).getLen(), cache, inMemory);
      this.closeIStream = true;
      this.name = path.toString();
    }

    /**
     * Opens a HFile.  You must load the index before you can
     * use it by calling {@link #loadFileInfo()}.
     *
     * @param fsdis input stream.  Caller is responsible for closing the passed
     * stream.
     * @param size Length of the stream.
     * @param cache block cache. Pass null if none.
     * @throws IOException
     */
    public Reader(final FSDataInputStream fsdis, final long size,
        final BlockCache cache, final boolean inMemory) {
      this.cache = cache;
      this.fileSize = size;
      this.istream = fsdis;
      this.closeIStream = false;
      this.name = this.istream == null? "": this.istream.toString();
      this.inMemory = inMemory;
    }

    @Override
    public String toString() {
      return "reader=" + this.name +
          (!isFileInfoLoaded()? "":
            ", compression=" + this.compressAlgo.getName() +
            ", inMemory=" + this.inMemory +
            ", firstKey=" + toStringFirstKey() +
            ", lastKey=" + toStringLastKey()) +
            ", avgKeyLen=" + this.avgKeyLen +
            ", avgValueLen=" + this.avgValueLen +
            ", entries=" + this.trailer.entryCount +
            ", length=" + this.fileSize;
    }

    protected String toStringFirstKey() {
      return KeyValue.keyToString(getFirstKey());
    }

    protected String toStringLastKey() {
      return KeyValue.keyToString(getLastKey());
    }

    public long length() {
      return this.fileSize;
    }

    public boolean inMemory() {
      return this.inMemory;
    }

    /**
     * Read in the index and file info.
     * @return A map of fileinfo data.
     * See {@link Writer#appendFileInfo(byte[], byte[])}.
     * @throws IOException
     */
    public Map<byte [], byte []> loadFileInfo()
    throws IOException {
      this.trailer = readTrailer();

      // Read in the fileinfo and get what we need from it.
      this.istream.seek(this.trailer.fileinfoOffset);
      FileInfo fi = new FileInfo();
      fi.readFields(this.istream);
      this.lastkey = fi.get(FileInfo.LASTKEY);
      this.avgKeyLen = Bytes.toInt(fi.get(FileInfo.AVG_KEY_LEN));
      this.avgValueLen = Bytes.toInt(fi.get(FileInfo.AVG_VALUE_LEN));
      String clazzName = Bytes.toString(fi.get(FileInfo.COMPARATOR));
      this.comparator = getComparator(clazzName);

      // Read in the data index.
      this.blockIndex = BlockIndex.readIndex(this.comparator, this.istream,
        this.trailer.dataIndexOffset, this.trailer.dataIndexCount);

      // Read in the metadata index.
      if (trailer.metaIndexCount > 0) {
        this.metaIndex = BlockIndex.readIndex(Bytes.BYTES_RAWCOMPARATOR,
          this.istream, this.trailer.metaIndexOffset, trailer.metaIndexCount);
      }
      this.fileInfoLoaded = true;
      return fi;
    }

    boolean isFileInfoLoaded() {
      return this.fileInfoLoaded;
    }

    @SuppressWarnings("unchecked")
    private RawComparator<byte []> getComparator(final String clazzName)
    throws IOException {
      if (clazzName == null || clazzName.length() == 0) {
        return null;
      }
      try {
        return (RawComparator<byte []>)Class.forName(clazzName).newInstance();
      } catch (InstantiationException e) {
        throw new IOException(e);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }

    /* Read the trailer off the input stream.  As side effect, sets the
     * compression algorithm.
     * @return Populated FixedFileTrailer.
     * @throws IOException
     */
    private FixedFileTrailer readTrailer() throws IOException {
      FixedFileTrailer fft = new FixedFileTrailer();
      long seekPoint = this.fileSize - FixedFileTrailer.trailerSize();
      this.istream.seek(seekPoint);
      fft.deserialize(this.istream);
      // Set up the codec.
      this.compressAlgo =
        Compression.Algorithm.values()[fft.compressionCodec];
      return fft;
    }

    /**
     * Create a Scanner on this file.  No seeks or reads are done on creation.
     * Call {@link HFileScanner#seekTo(byte[])} to position an start the read.
     * There is nothing to clean up in a Scanner. Letting go of your references
     * to the scanner is sufficient.
     * @param pread Use positional read rather than seek+read if true (pread is
     * better for random reads, seek+read is better scanning).
     * @param cacheBlocks True if we should cache blocks read in by this scanner.
     * @return Scanner on this file.
     */
    public HFileScanner getScanner(boolean cacheBlocks, final boolean pread) {
      return new Scanner(this, cacheBlocks, pread);
    }

    /**
     * @param key Key to search.
     * @return Block number of the block containing the key or -1 if not in this
     * file.
     */
    protected int blockContainingKey(final byte [] key, int offset, int length) {
      if (blockIndex == null) {
        throw new RuntimeException("Block index not loaded");
      }
      return blockIndex.blockContainingKey(key, offset, length);
    }
    /**
     * @param metaBlockName
     * @param cacheBlock Add block to cache, if found
     * @return Block wrapped in a ByteBuffer
     * @throws IOException
     */
    public ByteBuffer getMetaBlock(String metaBlockName, boolean cacheBlock)
    throws IOException {
      if (trailer.metaIndexCount == 0) {
        return null; // there are no meta blocks
      }
      if (metaIndex == null) {
        throw new IOException("Meta index not loaded");
      }

      byte [] mbname = Bytes.toBytes(metaBlockName);
      int block = metaIndex.blockContainingKey(mbname, 0, mbname.length);
      if (block == -1)
        return null;
      long blockSize;
      if (block == metaIndex.count - 1) {
        blockSize = trailer.fileinfoOffset - metaIndex.blockOffsets[block];
      } else {
        blockSize = metaIndex.blockOffsets[block+1] - metaIndex.blockOffsets[block];
      }

      long now = System.currentTimeMillis();

      // Per meta key from any given file, synchronize reads for said block
      synchronized (metaIndex.blockKeys[block]) {
        metaLoads++;
        // Check cache for block.  If found return.
        if (cache != null) {
          ByteBuffer cachedBuf = cache.getBlock(name + "meta" + block);
          if (cachedBuf != null) {
            // Return a distinct 'shallow copy' of the block,
            // so pos doesnt get messed by the scanner
            cacheHits++;
            return cachedBuf.duplicate();
          }
          // Cache Miss, please load.
        }

        ByteBuffer buf = decompress(metaIndex.blockOffsets[block],
          longToInt(blockSize), metaIndex.blockDataSizes[block], true);
        byte [] magic = new byte[METABLOCKMAGIC.length];
        buf.get(magic, 0, magic.length);

        if (! Arrays.equals(magic, METABLOCKMAGIC)) {
          throw new IOException("Meta magic is bad in block " + block);
        }

        // Create a new ByteBuffer 'shallow copy' to hide the magic header
        buf = buf.slice();

        readTime += System.currentTimeMillis() - now;
        readOps++;

        // Cache the block
        if(cacheBlock && cache != null) {
          cache.cacheBlock(name + "meta" + block, buf.duplicate(), inMemory);
        }

        return buf;
      }
    }

    /**
     * Read in a file block.
     * @param block Index of block to read.
     * @param pread Use positional read instead of seek+read (positional is
     * better doing random reads whereas seek+read is better scanning).
     * @return Block wrapped in a ByteBuffer.
     * @throws IOException
     */
    ByteBuffer readBlock(int block, boolean cacheBlock, final boolean pread)
    throws IOException {
      if (blockIndex == null) {
        throw new IOException("Block index not loaded");
      }
      if (block < 0 || block >= blockIndex.count) {
        throw new IOException("Requested block is out of range: " + block +
          ", max: " + blockIndex.count);
      }
      // For any given block from any given file, synchronize reads for said
      // block.
      // Without a cache, this synchronizing is needless overhead, but really
      // the other choice is to duplicate work (which the cache would prevent you from doing).
      synchronized (blockIndex.blockKeys[block]) {
        blockLoads++;
        // Check cache for block.  If found return.
        if (cache != null) {
          ByteBuffer cachedBuf = cache.getBlock(name + block);
          if (cachedBuf != null) {
            // Return a distinct 'shallow copy' of the block,
            // so pos doesnt get messed by the scanner
            cacheHits++;
            return cachedBuf.duplicate();
          }
          // Carry on, please load.
        }

        // Load block from filesystem.
        long now = System.currentTimeMillis();
        long onDiskBlockSize;
        if (block == blockIndex.count - 1) {
          // last block!  The end of data block is first meta block if there is
          // one or if there isn't, the fileinfo offset.
          long offset = this.metaIndex != null?
            this.metaIndex.blockOffsets[0]: this.trailer.fileinfoOffset;
          onDiskBlockSize = offset - blockIndex.blockOffsets[block];
        } else {
          onDiskBlockSize = blockIndex.blockOffsets[block+1] -
          blockIndex.blockOffsets[block];
        }
        ByteBuffer buf = decompress(blockIndex.blockOffsets[block],
          longToInt(onDiskBlockSize), this.blockIndex.blockDataSizes[block],
          pread);

        byte [] magic = new byte[DATABLOCKMAGIC.length];
        buf.get(magic, 0, magic.length);
        if (!Arrays.equals(magic, DATABLOCKMAGIC)) {
          throw new IOException("Data magic is bad in block " + block);
        }

        // 'shallow copy' to hide the header
        // NOTE: you WILL GET BIT if you call buf.array() but don't start
        //       reading at buf.arrayOffset()
        buf = buf.slice();

        readTime += System.currentTimeMillis() - now;
        readOps++;

        // Cache the block
        if(cacheBlock && cache != null) {
          cache.cacheBlock(name + block, buf.duplicate(), inMemory);
        }

        return buf;
      }
    }

    /*
     * Decompress <code>compressedSize</code> bytes off the backing
     * FSDataInputStream.
     * @param offset
     * @param compressedSize
     * @param decompressedSize
     *
     * @return
     * @throws IOException
     */
    private ByteBuffer decompress(final long offset, final int compressedSize,
      final int decompressedSize, final boolean pread)
    throws IOException {
      Decompressor decompressor = null;
      ByteBuffer buf = null;
      try {
        decompressor = this.compressAlgo.getDecompressor();
        // My guess is that the bounded range fis is needed to stop the
        // decompressor reading into next block -- IIRC, it just grabs a
        // bunch of data w/o regard to whether decompressor is coming to end of a
        // decompression.
        InputStream is = this.compressAlgo.createDecompressionStream(
          new BoundedRangeFileInputStream(this.istream, offset, compressedSize,
            pread),
          decompressor, 0);
        buf = ByteBuffer.allocate(decompressedSize);
        IOUtils.readFully(is, buf.array(), 0, buf.capacity());
        is.close();
      } finally {
        if (null != decompressor) {
          this.compressAlgo.returnDecompressor(decompressor);
        }
      }
      return buf;
    }

    /**
     * @return First key in the file.  May be null if file has no entries.
     */
    public byte [] getFirstKey() {
      if (blockIndex == null) {
        throw new RuntimeException("Block index not loaded");
      }
      return this.blockIndex.isEmpty()? null: this.blockIndex.blockKeys[0];
    }

    /**
     * @return number of KV entries in this HFile
     */
    public int getEntries() {
      if (!this.isFileInfoLoaded()) {
        throw new RuntimeException("File info not loaded");
      }
      return this.trailer.entryCount;
    }

    /**
     * @return Last key in the file.  May be null if file has no entries.
     */
    public byte [] getLastKey() {
      if (!isFileInfoLoaded()) {
        throw new RuntimeException("Load file info first");
      }
      return this.blockIndex.isEmpty()? null: this.lastkey;
    }

    /**
     * @return number of K entries in this HFile's filter.  Returns KV count if no filter.
     */
    public int getFilterEntries() {
      return getEntries();
    }

    /**
     * @return Comparator.
     */
    public RawComparator<byte []> getComparator() {
      return this.comparator;
    }

    /**
     * @return index size
     */
    public long indexSize() {
      return (this.blockIndex != null? this.blockIndex.heapSize(): 0) +
        ((this.metaIndex != null)? this.metaIndex.heapSize(): 0);
    }

    /**
     * @return Midkey for this file.  We work with block boundaries only so
     * returned midkey is an approximation only.
     * @throws IOException
     */
    public byte [] midkey() throws IOException {
      if (!isFileInfoLoaded() || this.blockIndex.isEmpty()) {
        return null;
      }
      return this.blockIndex.midkey();
    }

    public void close() throws IOException {
      if (this.closeIStream && this.istream != null) {
        this.istream.close();
        this.istream = null;
      }
    }

    /*
     * Implementation of {@link HFileScanner} interface.
     */
    protected static class Scanner implements HFileScanner {
      private final Reader reader;
      private ByteBuffer block;
      private int currBlock;

      private final boolean cacheBlocks;
      private final boolean pread;

      private int currKeyLen = 0;
      private int currValueLen = 0;

      public int blockFetches = 0;

      public Scanner(Reader r, boolean cacheBlocks, final boolean pread) {
        this.reader = r;
        this.cacheBlocks = cacheBlocks;
        this.pread = pread;
      }

      public KeyValue getKeyValue() {
        if(this.block == null) {
          return null;
        }
        return new KeyValue(this.block.array(),
            this.block.arrayOffset() + this.block.position() - 8);
      }

      public ByteBuffer getKey() {
        if (this.block == null || this.currKeyLen == 0) {
          throw new RuntimeException("you need to seekTo() before calling getKey()");
        }
        ByteBuffer keyBuff = this.block.slice();
        keyBuff.limit(this.currKeyLen);
        keyBuff.rewind();
        // Do keyBuff.asReadOnly()?
        return keyBuff;
      }

      public ByteBuffer getValue() {
        if (block == null || currKeyLen == 0) {
          throw new RuntimeException("you need to seekTo() before calling getValue()");
        }
        // TODO: Could this be done with one ByteBuffer rather than create two?
        ByteBuffer valueBuff = this.block.slice();
        valueBuff.position(this.currKeyLen);
        valueBuff = valueBuff.slice();
        valueBuff.limit(currValueLen);
        valueBuff.rewind();
        return valueBuff;
      }

      public boolean next() throws IOException {
        // LOG.deug("rem:" + block.remaining() + " p:" + block.position() +
        // " kl: " + currKeyLen + " kv: " + currValueLen);
        if (block == null) {
          throw new IOException("Next called on non-seeked scanner");
        }
        block.position(block.position() + currKeyLen + currValueLen);
        if (block.remaining() <= 0) {
          // LOG.debug("Fetch next block");
          currBlock++;
          if (currBlock >= reader.blockIndex.count) {
            // damn we are at the end
            currBlock = 0;
            block = null;
            return false;
          }
          block = reader.readBlock(this.currBlock, this.cacheBlocks, this.pread);
          currKeyLen = block.getInt();
          currValueLen = block.getInt();
          blockFetches++;
          return true;
        }
        // LOG.debug("rem:" + block.remaining() + " p:" + block.position() +
        // " kl: " + currKeyLen + " kv: " + currValueLen);

        currKeyLen = block.getInt();
        currValueLen = block.getInt();
        return true;
      }

      public boolean shouldSeek(final byte[] row,
          final SortedSet<byte[]> columns) {
        return true;
      }

      public int seekTo(byte [] key) throws IOException {
        return seekTo(key, 0, key.length);
      }


      public int seekTo(byte[] key, int offset, int length) throws IOException {
        int b = reader.blockContainingKey(key, offset, length);
        if (b < 0) return -1; // falls before the beginning of the file! :-(
        // Avoid re-reading the same block (that'd be dumb).
        loadBlock(b);

        return blockSeek(key, offset, length, false);
      }

      /**
       * Within a loaded block, seek looking for the first key
       * that is smaller than (or equal to?) the key we are interested in.
       *
       * A note on the seekBefore - if you have seekBefore = true, AND the
       * first key in the block = key, then you'll get thrown exceptions.
       * @param key to find
       * @param seekBefore find the key before the exact match.
       * @return
       */
      private int blockSeek(byte[] key, int offset, int length, boolean seekBefore) {
        int klen, vlen;
        int lastLen = 0;
        do {
          klen = block.getInt();
          vlen = block.getInt();
          int comp = this.reader.comparator.compare(key, offset, length,
            block.array(), block.arrayOffset() + block.position(), klen);
          if (comp == 0) {
            if (seekBefore) {
              block.position(block.position() - lastLen - 16);
              currKeyLen = block.getInt();
              currValueLen = block.getInt();
              return 1; // non exact match.
            }
            currKeyLen = klen;
            currValueLen = vlen;
            return 0; // indicate exact match
          }
          if (comp < 0) {
            // go back one key:
            block.position(block.position() - lastLen - 16);
            currKeyLen = block.getInt();
            currValueLen = block.getInt();
            return 1;
          }
          block.position(block.position() + klen + vlen);
          lastLen = klen + vlen ;
        } while(block.remaining() > 0);
        // ok we are at the end, so go back a littleeeeee....
        // The 8 in the below is intentionally different to the 16s in the above
        // Do the math you you'll figure it.
        block.position(block.position() - lastLen - 8);
        currKeyLen = block.getInt();
        currValueLen = block.getInt();
        return 1; // didn't exactly find it.
      }

      public boolean seekBefore(byte [] key) throws IOException {
        return seekBefore(key, 0, key.length);
      }

      public boolean seekBefore(byte[] key, int offset, int length)
      throws IOException {
        int b = reader.blockContainingKey(key, offset, length);
        if (b < 0)
          return false; // key is before the start of the file.

        // Question: does this block begin with 'key'?
        if (this.reader.comparator.compare(reader.blockIndex.blockKeys[b],
            0, reader.blockIndex.blockKeys[b].length,
            key, offset, length) == 0) {
          // Ok the key we're interested in is the first of the block, so go back one.
          if (b == 0) {
            // we have a 'problem', the key we want is the first of the file.
            return false;
          }
          b--;
          // TODO shortcut: seek forward in this block to the last key of the block.
        }
        loadBlock(b);
        blockSeek(key, offset, length, true);
        return true;
      }

      public String getKeyString() {
        return Bytes.toStringBinary(block.array(), block.arrayOffset() +
          block.position(), currKeyLen);
      }

      public String getValueString() {
        return Bytes.toString(block.array(), block.arrayOffset() +
          block.position() + currKeyLen, currValueLen);
      }

      public Reader getReader() {
        return this.reader;
      }

      public boolean isSeeked(){
        return this.block != null;
      }

      public boolean seekTo() throws IOException {
        if (this.reader.blockIndex.isEmpty()) {
          return false;
        }
        if (block != null && currBlock == 0) {
          block.rewind();
          currKeyLen = block.getInt();
          currValueLen = block.getInt();
          return true;
        }
        currBlock = 0;
        block = reader.readBlock(this.currBlock, this.cacheBlocks, this.pread);
        currKeyLen = block.getInt();
        currValueLen = block.getInt();
        blockFetches++;
        return true;
      }

      private void loadBlock(int bloc) throws IOException {
        if (block == null) {
          block = reader.readBlock(bloc, this.cacheBlocks, this.pread);
          currBlock = bloc;
          blockFetches++;
        } else {
          if (bloc != currBlock) {
            block = reader.readBlock(bloc, this.cacheBlocks, this.pread);
            currBlock = bloc;
            blockFetches++;
          } else {
            // we are already in the same block, just rewind to seek again.
            block.rewind();
          }
        }
      }

      @Override
      public String toString() {
        return "HFileScanner for reader " + String.valueOf(reader);
      }
    }

    public String getTrailerInfo() {
      return trailer.toString();
    }
  }

  /*
   * The RFile has a fixed trailer which contains offsets to other variable
   * parts of the file.  Also includes basic metadata on this file.
   */
  private static class FixedFileTrailer {
    // Offset to the fileinfo data, a small block of vitals..
    long fileinfoOffset;
    // Offset to the data block index.
    long dataIndexOffset;
    // How many index counts are there (aka: block count)
    int dataIndexCount;
    // Offset to the meta block index.
    long metaIndexOffset;
    // How many meta block index entries (aka: meta block count)
    int metaIndexCount;
    long totalUncompressedBytes;
    int entryCount;
    int compressionCodec;
    int version = 1;

    FixedFileTrailer() {
      super();
    }

    static int trailerSize() {
      // Keep this up to date...
      return
      ( Bytes.SIZEOF_INT * 5 ) +
      ( Bytes.SIZEOF_LONG * 4 ) +
      TRAILERBLOCKMAGIC.length;
    }

    void serialize(DataOutputStream outputStream) throws IOException {
      outputStream.write(TRAILERBLOCKMAGIC);
      outputStream.writeLong(fileinfoOffset);
      outputStream.writeLong(dataIndexOffset);
      outputStream.writeInt(dataIndexCount);
      outputStream.writeLong(metaIndexOffset);
      outputStream.writeInt(metaIndexCount);
      outputStream.writeLong(totalUncompressedBytes);
      outputStream.writeInt(entryCount);
      outputStream.writeInt(compressionCodec);
      outputStream.writeInt(version);
    }

    void deserialize(DataInputStream inputStream) throws IOException {
      byte [] header = new byte[TRAILERBLOCKMAGIC.length];
      inputStream.readFully(header);
      if ( !Arrays.equals(header, TRAILERBLOCKMAGIC)) {
        throw new IOException("Trailer 'header' is wrong; does the trailer " +
          "size match content?");
      }
      fileinfoOffset         = inputStream.readLong();
      dataIndexOffset        = inputStream.readLong();
      dataIndexCount         = inputStream.readInt();

      metaIndexOffset        = inputStream.readLong();
      metaIndexCount         = inputStream.readInt();

      totalUncompressedBytes = inputStream.readLong();
      entryCount             = inputStream.readInt();
      compressionCodec       = inputStream.readInt();
      version                = inputStream.readInt();

      if (version != 1) {
        throw new IOException("Wrong version: " + version);
      }
    }

    @Override
    public String toString() {
      return "fileinfoOffset=" + fileinfoOffset +
      ", dataIndexOffset=" + dataIndexOffset +
      ", dataIndexCount=" + dataIndexCount +
      ", metaIndexOffset=" + metaIndexOffset +
      ", metaIndexCount=" + metaIndexCount +
      ", totalBytes=" + totalUncompressedBytes +
      ", entryCount=" + entryCount +
      ", version=" + version;
    }
  }

  /*
   * The block index for a RFile.
   * Used reading.
   */
  static class BlockIndex implements HeapSize {
    // How many actual items are there? The next insert location too.
    int count = 0;
    byte [][] blockKeys;
    long [] blockOffsets;
    int [] blockDataSizes;
    int size = 0;

    /* Needed doing lookup on blocks.
     */
    final RawComparator<byte []> comparator;

    /*
     * Shutdown default constructor
     */
    @SuppressWarnings("unused")
    private BlockIndex() {
      this(null);
    }


    /**
     * @param c comparator used to compare keys.
     */
    BlockIndex(final RawComparator<byte []>c) {
      this.comparator = c;
      // Guess that cost of three arrays + this object is 4 * 8 bytes.
      this.size += (4 * 8);
    }

    /**
     * @return True if block index is empty.
     */
    boolean isEmpty() {
      return this.blockKeys.length <= 0;
    }

    /**
     * Adds a new entry in the block index.
     *
     * @param key Last key in the block
     * @param offset file offset where the block is stored
     * @param dataSize the uncompressed data size
     */
    void add(final byte[] key, final long offset, final int dataSize) {
      blockOffsets[count] = offset;
      blockKeys[count] = key;
      blockDataSizes[count] = dataSize;
      count++;
      this.size += (Bytes.SIZEOF_INT * 2 + key.length);
    }

    /**
     * @param key Key to find
     * @return Offset of block containing <code>key</code> or -1 if this file
     * does not contain the request.
     */
    int blockContainingKey(final byte[] key, int offset, int length) {
      int pos = Bytes.binarySearch(blockKeys, key, offset, length, this.comparator);
      if (pos < 0) {
        pos ++;
        pos *= -1;
        if (pos == 0) {
          // falls before the beginning of the file.
          return -1;
        }
        // When switched to "first key in block" index, binarySearch now returns
        // the block with a firstKey < key.  This means the value we want is potentially
        // in the next block.
        pos --; // in previous block.

        return pos;
      }
      // wow, a perfect hit, how unlikely?
      return pos;
    }

    /*
     * @return File midkey.  Inexact.  Operates on block boundaries.  Does
     * not go into blocks.
     */
    byte [] midkey() throws IOException {
      int pos = ((this.count - 1)/2);              // middle of the index
      if (pos < 0) {
        throw new IOException("HFile empty");
      }
      return this.blockKeys[pos];
    }

    /*
     * Write out index. Whatever we write here must jibe with what
     * BlockIndex#readIndex is expecting.  Make sure the two ends of the
     * index serialization match.
     * @param o
     * @param keys
     * @param offsets
     * @param sizes
     * @param c
     * @return Position at which we entered the index.
     * @throws IOException
     */
    static long writeIndex(final FSDataOutputStream o,
      final List<byte []> keys, final List<Long> offsets,
      final List<Integer> sizes)
    throws IOException {
      long pos = o.getPos();
      // Don't write an index if nothing in the index.
      if (keys.size() > 0) {
        o.write(INDEXBLOCKMAGIC);
        // Write the index.
        for (int i = 0; i < keys.size(); ++i) {
          o.writeLong(offsets.get(i).longValue());
          o.writeInt(sizes.get(i).intValue());
          byte [] key = keys.get(i);
          Bytes.writeByteArray(o, key);
        }
      }
      return pos;
    }

    /*
     * Read in the index that is at <code>indexOffset</code>
     * Must match what was written by writeIndex in the Writer.close.
     * @param in
     * @param indexOffset
     * @throws IOException
     */
    static BlockIndex readIndex(final RawComparator<byte []> c,
        final FSDataInputStream in, final long indexOffset, final int indexSize)
    throws IOException {
      BlockIndex bi = new BlockIndex(c);
      bi.blockOffsets = new long[indexSize];
      bi.blockKeys = new byte[indexSize][];
      bi.blockDataSizes = new int[indexSize];
      // If index size is zero, no index was written.
      if (indexSize > 0) {
        in.seek(indexOffset);
        byte [] magic = new byte[INDEXBLOCKMAGIC.length];
        IOUtils.readFully(in, magic, 0, magic.length);
        if (!Arrays.equals(magic, INDEXBLOCKMAGIC)) {
          throw new IOException("Index block magic is wrong: " +
            Arrays.toString(magic));
        }
        for (int i = 0; i < indexSize; ++i ) {
          long offset   = in.readLong();
          int dataSize  = in.readInt();
          byte [] key = Bytes.readByteArray(in);
          bi.add(key, offset, dataSize);
        }
      }
      return bi;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("size=" + count);
      for (int i = 0; i < count ; i++) {
        sb.append(", ");
        sb.append("key=").append(Bytes.toStringBinary(blockKeys[i])).
          append(", offset=").append(blockOffsets[i]).
          append(", dataSize=" + blockDataSizes[i]);
      }
      return sb.toString();
    }

    public long heapSize() {
      long heapsize = ClassSize.align(ClassSize.OBJECT +
          2 * Bytes.SIZEOF_INT + (3 + 1) * ClassSize.REFERENCE);
      //Calculating the size of blockKeys
      if(blockKeys != null) {
        //Adding array + references overhead
        heapsize += ClassSize.align(ClassSize.ARRAY +
            blockKeys.length * ClassSize.REFERENCE);
        //Adding bytes
        for(byte [] bs : blockKeys) {
          heapsize += ClassSize.align(ClassSize.ARRAY + bs.length);
        }
      }
      if(blockOffsets != null) {
        heapsize += ClassSize.align(ClassSize.ARRAY +
            blockOffsets.length * Bytes.SIZEOF_LONG);
      }
      if(blockDataSizes != null) {
        heapsize += ClassSize.align(ClassSize.ARRAY +
            blockDataSizes.length * Bytes.SIZEOF_INT);
      }

      return ClassSize.align(heapsize);
    }

  }

  /*
   * Metadata for this file.  Conjured by the writer.  Read in by the reader.
   */
  static class FileInfo extends HbaseMapWritable<byte [], byte []> {
    static final String RESERVED_PREFIX = "hfile.";
    static final byte [] LASTKEY = Bytes.toBytes(RESERVED_PREFIX + "LASTKEY");
    static final byte [] AVG_KEY_LEN =
      Bytes.toBytes(RESERVED_PREFIX + "AVG_KEY_LEN");
    static final byte [] AVG_VALUE_LEN =
      Bytes.toBytes(RESERVED_PREFIX + "AVG_VALUE_LEN");
    static final byte [] COMPARATOR =
      Bytes.toBytes(RESERVED_PREFIX + "COMPARATOR");

    /*
     * Constructor.
     */
    FileInfo() {
      super();
    }
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

  public static void main(String []args) throws IOException {
    try {
      // create options
      Options options = new Options();
      options.addOption("v", "verbose", false, "Verbose output; emits file and meta data delimiters");
      options.addOption("p", "printkv", false, "Print key/value pairs");
      options.addOption("m", "printmeta", false, "Print meta data of file");
      options.addOption("k", "checkrow", false,
        "Enable row order check; looks for out-of-order keys");
      options.addOption("a", "checkfamily", false, "Enable family check");
      options.addOption("f", "file", true,
        "File to scan. Pass full-path; e.g. hdfs://a:9000/hbase/.META./12/34");
      options.addOption("r", "region", true,
        "Region to scan. Pass region name; e.g. '.META.,,1'");
      if (args.length == 0) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("HFile ", options, true);
        System.exit(-1);
      }
      CommandLineParser parser = new PosixParser();
      CommandLine cmd = parser.parse(options, args);
      boolean verbose = cmd.hasOption("v");
      boolean printKeyValue = cmd.hasOption("p");
      boolean printMeta = cmd.hasOption("m");
      boolean checkRow = cmd.hasOption("k");
      boolean checkFamily = cmd.hasOption("a");
      // get configuration, file system and get list of files
      Configuration conf = HBaseConfiguration.create();
      conf.set("fs.defaultFS",
        conf.get(org.apache.hadoop.hbase.HConstants.HBASE_DIR));
      FileSystem fs = FileSystem.get(conf);
      ArrayList<Path> files = new ArrayList<Path>();
      if (cmd.hasOption("f")) {
        files.add(new Path(cmd.getOptionValue("f")));
      }
      if (cmd.hasOption("r")) {
        String regionName = cmd.getOptionValue("r");
        byte[] rn = Bytes.toBytes(regionName);
        byte[][] hri = HRegionInfo.parseRegionName(rn);
        Path rootDir = FSUtils.getRootDir(conf);
        Path tableDir = new Path(rootDir, Bytes.toString(hri[0]));
        String enc = HRegionInfo.encodeRegionName(rn);
        Path regionDir = new Path(tableDir, enc);
        if (verbose) System.out.println("region dir -> " + regionDir);
        List<Path> regionFiles = getStoreFiles(fs, regionDir);
        if (verbose) System.out.println("Number of region files found -> " +
          regionFiles.size());
        if (verbose) {
          int i = 1;
          for (Path p : regionFiles) {
            if (verbose) System.out.println("Found file[" + i++ + "] -> " + p);
          }
        }
        files.addAll(regionFiles);
      }
      // iterate over all files found
      for (Path file : files) {
        if (verbose) System.out.println("Scanning -> " + file);
        if (!fs.exists(file)) {
          System.err.println("ERROR, file doesnt exist: " + file);
          continue;
        }
        // create reader and load file info
        HFile.Reader reader = new HFile.Reader(fs, file, null, false);
        Map<byte[],byte[]> fileInfo = reader.loadFileInfo();
        // scan over file and read key/value's and check if requested
        HFileScanner scanner = reader.getScanner(false, false);
        scanner.seekTo();
        KeyValue pkv = null;
        int count = 0;
        do {
          KeyValue kv = scanner.getKeyValue();
          // dump key value
          if (printKeyValue) {
            System.out.println("K: " + kv +
              " V: " + Bytes.toStringBinary(kv.getValue()));
          }
          // check if rows are in order
          if (checkRow && pkv != null) {
            if (Bytes.compareTo(pkv.getRow(), kv.getRow()) > 0) {
              System.err.println("WARNING, previous row is greater then" +
                " current row\n\tfilename -> " + file +
                "\n\tprevious -> " + Bytes.toStringBinary(pkv.getKey()) +
                "\n\tcurrent  -> " + Bytes.toStringBinary(kv.getKey()));
            }
          }
          // check if families are consistent
          if (checkFamily) {
            String fam = Bytes.toString(kv.getFamily());
            if (!file.toString().contains(fam)) {
              System.err.println("WARNING, filename does not match kv family," +
                "\n\tfilename -> " + file +
                "\n\tkeyvalue -> " + Bytes.toStringBinary(kv.getKey()));
            }
            if (pkv != null && Bytes.compareTo(pkv.getFamily(), kv.getFamily()) != 0) {
              System.err.println("WARNING, previous kv has different family" +
                " compared to current key\n\tfilename -> " + file +
                "\n\tprevious -> " +  Bytes.toStringBinary(pkv.getKey()) +
                "\n\tcurrent  -> " + Bytes.toStringBinary(kv.getKey()));
            }
          }
          pkv = kv;
          count++;
        } while (scanner.next());
        if (verbose || printKeyValue) {
          System.out.println("Scanned kv count -> " + count);
        }
        // print meta data
        if (printMeta) {
          System.out.println("Block index size as per heapsize: " + reader.indexSize());
          System.out.println(reader.toString());
          System.out.println(reader.getTrailerInfo());
          System.out.println("Fileinfo:");
          for (Map.Entry<byte[], byte[]> e : fileInfo.entrySet()) {
            System.out.print(Bytes.toString(e.getKey()) + " = " );
            if (Bytes.compareTo(e.getKey(), Bytes.toBytes("MAX_SEQ_ID_KEY"))==0) {
              long seqid = Bytes.toLong(e.getValue());
              System.out.println(seqid);
            } else {
              System.out.println(Bytes.toStringBinary(e.getValue()));
            }
          }
        }
        reader.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
