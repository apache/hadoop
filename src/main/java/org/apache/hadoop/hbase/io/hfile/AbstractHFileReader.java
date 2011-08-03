/*
 * Copyright 2011 The Apache Software Foundation
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.io.RawComparator;

/**
 * Common functionality needed by all versions of {@link HFile} readers.
 */
public abstract class AbstractHFileReader implements HFile.Reader {

  private static final Log LOG = LogFactory.getLog(AbstractHFileReader.class);

  /** Filesystem-level block reader for this HFile format version. */
  protected HFileBlock.FSReader fsBlockReader;

  /** Stream to read from. */
  protected FSDataInputStream istream;

  /**
   * True if we should close the input stream when done. We don't close it if we
   * didn't open it.
   */
  protected final boolean closeIStream;

  /** Data block index reader keeping the root data index in memory */
  protected HFileBlockIndex.BlockIndexReader dataBlockIndexReader;

  /** Meta block index reader -- always single level */
  protected HFileBlockIndex.BlockIndexReader metaBlockIndexReader;

  protected final FixedFileTrailer trailer;

  /** Filled when we read in the trailer. */
  protected final Compression.Algorithm compressAlgo;

  /** Last key in the file. Filled in when we read in the file info */
  protected byte [] lastKey = null;

  /** Average key length read from file info */
  protected int avgKeyLen = -1;

  /** Average value length read from file info */
  protected int avgValueLen = -1;

  /** Key comparator */
  protected RawComparator<byte []> comparator;

  /** Size of this file. */
  protected final long fileSize;

  /** Block cache to use. */
  protected final BlockCache blockCache;

  protected AtomicLong cacheHits = new AtomicLong();
  protected AtomicLong blockLoads = new AtomicLong();
  protected AtomicLong metaLoads = new AtomicLong();

  /**
   * Whether file is from in-memory store (comes from column family
   * configuration).
   */
  protected boolean inMemory = false;

  /**
   * Whether blocks of file should be evicted from the block cache when the
   * file is being closed
   */
  protected final boolean evictOnClose;

  /** Path of file */
  protected final Path path;

  /** File name to be used for block names */
  protected final String name;

  protected FileInfo fileInfo;

  /** Prefix of the form cf.<column_family_name> for statistics counters. */
  private final String cfStatsPrefix;

  protected AbstractHFileReader(Path path, FixedFileTrailer trailer,
      final FSDataInputStream fsdis, final long fileSize,
      final boolean closeIStream,
      final BlockCache blockCache, final boolean inMemory,
      final boolean evictOnClose) {
    this.trailer = trailer;
    this.compressAlgo = trailer.getCompressionCodec();
    this.blockCache = blockCache;
    this.fileSize = fileSize;
    this.istream = fsdis;
    this.closeIStream = closeIStream;
    this.inMemory = inMemory;
    this.evictOnClose = evictOnClose;
    this.path = path;
    this.name = path.getName();
    cfStatsPrefix = "cf." + parseCfNameFromPath(path.toString());
  }

  @SuppressWarnings("serial")
  public static class BlockIndexNotLoadedException
      extends IllegalStateException {
    public BlockIndexNotLoadedException() {
      // Add a message in case anyone relies on it as opposed to class name.
      super("Block index not loaded");
    }
  }

  protected String toStringFirstKey() {
    return KeyValue.keyToString(getFirstKey());
  }

  protected String toStringLastKey() {
    return KeyValue.keyToString(getLastKey());
  }

  /**
   * Parse the HFile path to figure out which table and column family
   * it belongs to. This is used to maintain read statistics on a
   * per-column-family basis.
   *
   * @param path HFile path name
   */
  public static String parseCfNameFromPath(String path) {
    String splits[] = path.split("/");
    if (splits.length < 2) {
      LOG.warn("Could not determine the table and column family of the " +
          "HFile path " + path);
      return "unknown";
    }

    return splits[splits.length - 2];
  }

  public abstract boolean isFileInfoLoaded();

  @Override
  public String toString() {
    return "reader=" + path.toString() +
        (!isFileInfoLoaded()? "":
          ", compression=" + compressAlgo.getName() +
          ", inMemory=" + inMemory +
          ", firstKey=" + toStringFirstKey() +
          ", lastKey=" + toStringLastKey()) +
          ", avgKeyLen=" + avgKeyLen +
          ", avgValueLen=" + avgValueLen +
          ", entries=" + trailer.getEntryCount() +
          ", length=" + fileSize;
  }

  @Override
  public long length() {
    return fileSize;
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(byte[])} to position an start the read. There is
   * nothing to clean up in a Scanner. Letting go of your references to the
   * scanner is sufficient. NOTE: Do not use this overload of getScanner for
   * compactions.
   *
   * @param cacheBlocks True if we should cache blocks read in by this scanner.
   * @param pread Use positional read rather than seek+read if true (pread is
   *          better for random reads, seek+read is better scanning).
   * @return Scanner on this file.
   */
  @Override
  public HFileScanner getScanner(boolean cacheBlocks, final boolean pread) {
    return getScanner(cacheBlocks, pread, false);
  }

  /**
   * @return the first key in the file. May be null if file has no entries. Note
   *         that this is not the first row key, but rather the byte form of the
   *         first KeyValue.
   */
  @Override
  public byte [] getFirstKey() {
    if (dataBlockIndexReader == null) {
      throw new BlockIndexNotLoadedException();
    }
    return dataBlockIndexReader.isEmpty() ? null
        : dataBlockIndexReader.getRootBlockKey(0);
  }

  /**
   * TODO left from {@HFile} version 1: move this to StoreFile after Ryan's
   * patch goes in to eliminate {@link KeyValue} here.
   *
   * @return the first row key, or null if the file is empty.
   */
  @Override
  public byte[] getFirstRowKey() {
    byte[] firstKey = getFirstKey();
    if (firstKey == null)
      return null;
    return KeyValue.createKeyValueFromKey(firstKey).getRow();
  }

  /**
   * TODO left from {@HFile} version 1: move this to StoreFile after
   * Ryan's patch goes in to eliminate {@link KeyValue} here.
   *
   * @return the last row key, or null if the file is empty.
   */
  @Override
  public byte[] getLastRowKey() {
    byte[] lastKey = getLastKey();
    if (lastKey == null)
      return null;
    return KeyValue.createKeyValueFromKey(lastKey).getRow();
  }

  /** @return number of KV entries in this HFile */
  @Override
  public long getEntries() {
    return trailer.getEntryCount();
  }

  /** @return comparator */
  @Override
  public RawComparator<byte []> getComparator() {
    return comparator;
  }

  /** @return compression algorithm */
  @Override
  public Compression.Algorithm getCompressionAlgorithm() {
    return compressAlgo;
  }

  /**
   * @return the total heap size of data and meta block indexes in bytes. Does
   *         not take into account non-root blocks of a multilevel data index.
   */
  public long indexSize() {
    return (dataBlockIndexReader != null ? dataBlockIndexReader.heapSize() : 0)
        + ((metaBlockIndexReader != null) ? metaBlockIndexReader.heapSize()
            : 0);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public HFileBlockIndex.BlockIndexReader getDataBlockIndexReader() {
    return dataBlockIndexReader;
  }

  @Override
  public String getColumnFamilyName() {
    return cfStatsPrefix;
  }

  @Override
  public FixedFileTrailer getTrailer() {
    return trailer;
  }

  @Override
  public FileInfo loadFileInfo() throws IOException {
    return fileInfo;
  }

  /**
   * An exception thrown when an operation requiring a scanner to be seeked
   * is invoked on a scanner that is not seeked.
   */
  @SuppressWarnings("serial")
  public static class NotSeekedException extends IllegalStateException {
    public NotSeekedException() {
      super("Not seeked to a key/value");
    }
  }

  protected static abstract class Scanner implements HFileScanner {
    protected HFile.Reader reader;
    protected ByteBuffer blockBuffer;

    protected boolean cacheBlocks;
    protected final boolean pread;
    protected final boolean isCompaction;

    protected int currKeyLen;
    protected int currValueLen;

    protected int blockFetches;

    public Scanner(final HFile.Reader reader, final boolean cacheBlocks,
        final boolean pread, final boolean isCompaction) {
      this.reader = reader;
      this.cacheBlocks = cacheBlocks;
      this.pread = pread;
      this.isCompaction = isCompaction;
    }

    @Override
    public Reader getReader() {
      return reader;
    }

    @Override
    public boolean isSeeked(){
      return blockBuffer != null;
    }

    @Override
    public String toString() {
      return "HFileScanner for reader " + String.valueOf(reader);
    }

    protected void assertSeeked() {
      if (!isSeeked())
        throw new NotSeekedException();
    }
  }

  /** For testing */
  HFileBlock.FSReader getUncachedBlockReader() {
    return fsBlockReader;
  }

}
