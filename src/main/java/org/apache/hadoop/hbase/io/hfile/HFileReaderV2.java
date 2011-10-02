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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.IdLock;

/**
 * {@link HFile} reader for version 2.
 */
public class HFileReaderV2 extends AbstractHFileReader {

  private static final Log LOG = LogFactory.getLog(HFileReaderV2.class);

  /**
   * The size of a (key length, value length) tuple that prefixes each entry in
   * a data block.
   */
  private static final int KEY_VALUE_LEN_SIZE = 2 * Bytes.SIZEOF_INT;

  /**
   * A "sparse lock" implementation allowing to lock on a particular block
   * identified by offset. The purpose of this is to avoid two clients loading
   * the same block, and have all but one client wait to get the block from the
   * cache.
   */
  private IdLock offsetLock = new IdLock();

  /**
   * Blocks read from the load-on-open section, excluding data root index, meta
   * index, and file info.
   */
  private List<HFileBlock> loadOnOpenBlocks = new ArrayList<HFileBlock>();

  /**
   * Opens a HFile. You must load the index before you can use it by calling
   * {@link #loadFileInfo()}.
   *
   * @param fsdis input stream. Caller is responsible for closing the passed
   *          stream.
   * @param size Length of the stream.
   * @param blockCache block cache. Pass null if none.
   * @param inMemory whether blocks should be marked as in-memory in cache
   * @param evictOnClose whether blocks in cache should be evicted on close
   * @throws IOException
   */
  public HFileReaderV2(Path path, FixedFileTrailer trailer,
      final FSDataInputStream fsdis, final long size,
      final boolean closeIStream, final BlockCache blockCache,
      final boolean inMemory, final boolean evictOnClose) throws IOException {
    super(path, trailer, fsdis, size, closeIStream, blockCache, inMemory,
        evictOnClose);

    trailer.expectVersion(2);
    fsBlockReader = new HFileBlock.FSReaderV2(fsdis, compressAlgo,
        fileSize);

    // Comparator class name is stored in the trailer in version 2.
    comparator = trailer.createComparator();
    dataBlockIndexReader = new HFileBlockIndex.BlockIndexReader(comparator,
        trailer.getNumDataIndexLevels(), this);
    metaBlockIndexReader = new HFileBlockIndex.BlockIndexReader(
        Bytes.BYTES_RAWCOMPARATOR, 1);

    // Parse load-on-open data.

    HFileBlock.BlockIterator blockIter = fsBlockReader.blockRange(
        trailer.getLoadOnOpenDataOffset(),
        fileSize - trailer.getTrailerSize());

    // Data index. We also read statistics about the block index written after
    // the root level.
    dataBlockIndexReader.readMultiLevelIndexRoot(
        blockIter.nextBlockAsStream(BlockType.ROOT_INDEX),
        trailer.getDataIndexCount());

    // Meta index.
    metaBlockIndexReader.readRootIndex(
        blockIter.nextBlockAsStream(BlockType.ROOT_INDEX),
        trailer.getMetaIndexCount());

    // File info
    fileInfo = new FileInfo();
    fileInfo.readFields(blockIter.nextBlockAsStream(BlockType.FILE_INFO));
    lastKey = fileInfo.get(FileInfo.LASTKEY);
    avgKeyLen = Bytes.toInt(fileInfo.get(FileInfo.AVG_KEY_LEN));
    avgValueLen = Bytes.toInt(fileInfo.get(FileInfo.AVG_VALUE_LEN));

    // Store all other load-on-open blocks for further consumption.
    HFileBlock b;
    while ((b = blockIter.nextBlock()) != null) {
      loadOnOpenBlocks.add(b);
    }
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(byte[])} to position an start the read. There is
   * nothing to clean up in a Scanner. Letting go of your references to the
   * scanner is sufficient.
   *
   * @param cacheBlocks True if we should cache blocks read in by this scanner.
   * @param pread Use positional read rather than seek+read if true (pread is
   *          better for random reads, seek+read is better scanning).
   * @param isCompaction is scanner being used for a compaction?
   * @return Scanner on this file.
   */
  @Override
  public HFileScanner getScanner(boolean cacheBlocks, final boolean pread,
      final boolean isCompaction) {
    return new ScannerV2(this, cacheBlocks, pread, isCompaction);
  }

  /**
   * @param metaBlockName
   * @param cacheBlock Add block to cache, if found
   * @return block wrapped in a ByteBuffer, with header skipped
   * @throws IOException
   */
  @Override
  public ByteBuffer getMetaBlock(String metaBlockName, boolean cacheBlock)
      throws IOException {
    if (trailer.getMetaIndexCount() == 0) {
      return null; // there are no meta blocks
    }
    if (metaBlockIndexReader == null) {
      throw new IOException("Meta index not loaded");
    }

    byte[] mbname = Bytes.toBytes(metaBlockName);
    int block = metaBlockIndexReader.rootBlockContainingKey(mbname, 0,
        mbname.length);
    if (block == -1)
      return null;
    long blockSize = metaBlockIndexReader.getRootBlockDataSize(block);
    long startTimeNs = System.nanoTime();

    // Per meta key from any given file, synchronize reads for said block. This
    // is OK to do for meta blocks because the meta block index is always
    // single-level.
    synchronized (metaBlockIndexReader.getRootBlockKey(block)) {
      metaLoads.incrementAndGet();

      // Check cache for block. If found return.
      long metaBlockOffset = metaBlockIndexReader.getRootBlockOffset(block);
      String cacheKey = HFile.getBlockCacheKey(name, metaBlockOffset);

      if (blockCache != null) {
        HFileBlock cachedBlock = (HFileBlock) blockCache.getBlock(cacheKey,
            true);
        if (cachedBlock != null) {
          // Return a distinct 'shallow copy' of the block,
          // so pos does not get messed by the scanner
          cacheHits.incrementAndGet();
          return cachedBlock.getBufferWithoutHeader();
        }
        // Cache Miss, please load.
      }

      HFileBlock metaBlock = fsBlockReader.readBlockData(metaBlockOffset,
          blockSize, -1, true);

      HFile.readTimeNano.addAndGet(System.nanoTime() - startTimeNs);
      HFile.readOps.incrementAndGet();

      // Cache the block
      if (cacheBlock && blockCache != null) {
        blockCache.cacheBlock(cacheKey, metaBlock, inMemory);
      }

      return metaBlock.getBufferWithoutHeader();
    }
  }

  /**
   * Read in a file block.
   *
   * @param dataBlockOffset offset to read.
   * @param onDiskSize size of the block
   * @param pread Use positional read instead of seek+read (positional is better
   *          doing random reads whereas seek+read is better scanning).
   * @param isCompaction is this block being read as part of a compaction
   * @return Block wrapped in a ByteBuffer.
   * @throws IOException
   */
  @Override
  public HFileBlock readBlock(long dataBlockOffset, long onDiskBlockSize,
      boolean cacheBlock, boolean pread, final boolean isCompaction)
      throws IOException {
    if (dataBlockIndexReader == null) {
      throw new IOException("Block index not loaded");
    }
    if (dataBlockOffset < 0
        || dataBlockOffset >= trailer.getLoadOnOpenDataOffset()) {
      throw new IOException("Requested block is out of range: "
          + dataBlockOffset + ", lastDataBlockOffset: "
          + trailer.getLastDataBlockOffset());
    }
    // For any given block from any given file, synchronize reads for said
    // block.
    // Without a cache, this synchronizing is needless overhead, but really
    // the other choice is to duplicate work (which the cache would prevent you
    // from doing).

    String cacheKey = HFile.getBlockCacheKey(name, dataBlockOffset);
    IdLock.Entry lockEntry = offsetLock.getLockEntry(dataBlockOffset);
    try {
      blockLoads.incrementAndGet();

      // Check cache for block. If found return.
      if (blockCache != null) {
        HFileBlock cachedBlock = (HFileBlock) blockCache.getBlock(cacheKey,
            true);
        if (cachedBlock != null) {
          cacheHits.incrementAndGet();

          return cachedBlock;
        }
        // Carry on, please load.
      }

      // Load block from filesystem.
      long startTimeNs = System.nanoTime();
      HFileBlock dataBlock = fsBlockReader.readBlockData(dataBlockOffset,
          onDiskBlockSize, -1, pread);

      HFile.readTimeNano.addAndGet(System.nanoTime() - startTimeNs);
      HFile.readOps.incrementAndGet();

      // Cache the block
      if (cacheBlock && blockCache != null) {
        blockCache.cacheBlock(cacheKey, dataBlock, inMemory);
      }

      return dataBlock;
    } finally {
      offsetLock.releaseLockEntry(lockEntry);
    }
  }

  /**
   * @return Last key in the file. May be null if file has no entries. Note that
   *         this is not the last row key, but rather the byte form of the last
   *         KeyValue.
   */
  @Override
  public byte[] getLastKey() {
    return dataBlockIndexReader.isEmpty() ? null : lastKey;
  }

  /**
   * @return Midkey for this file. We work with block boundaries only so
   *         returned midkey is an approximation only.
   * @throws IOException
   */
  @Override
  public byte[] midkey() throws IOException {
    return dataBlockIndexReader.midkey();
  }

  @Override
  public void close() throws IOException {
    if (evictOnClose && blockCache != null) {
      int numEvicted = blockCache.evictBlocksByPrefix(name
          + HFile.CACHE_KEY_SEPARATOR);
      LOG.debug("On close of file " + name + " evicted " + numEvicted
          + " block(s)");
    }
    if (closeIStream && istream != null) {
      istream.close();
      istream = null;
    }
  }

  /**
   * Implementation of {@link HFileScanner} interface.
   */
  protected static class ScannerV2 extends AbstractHFileReader.Scanner {
    private HFileBlock block;

    public ScannerV2(HFileReaderV2 r, boolean cacheBlocks,
        final boolean pread, final boolean isCompaction) {
      super(r, cacheBlocks, pread, isCompaction);
    }

    @Override
    public KeyValue getKeyValue() {
      if (!isSeeked())
        return null;

      return new KeyValue(blockBuffer.array(), blockBuffer.arrayOffset()
          + blockBuffer.position());
    }

    @Override
    public ByteBuffer getKey() {
      assertSeeked();
      return ByteBuffer.wrap(
          blockBuffer.array(),
          blockBuffer.arrayOffset() + blockBuffer.position()
              + KEY_VALUE_LEN_SIZE, currKeyLen).slice();
    }

    @Override
    public ByteBuffer getValue() {
      assertSeeked();
      return ByteBuffer.wrap(
          blockBuffer.array(),
          blockBuffer.arrayOffset() + blockBuffer.position()
              + KEY_VALUE_LEN_SIZE + currKeyLen, currValueLen).slice();
    }

    private void setNonSeekedState() {
      block = null;
      blockBuffer = null;
      currKeyLen = 0;
      currValueLen = 0;
    }

    /**
     * Go to the next key/value in the block section. Loads the next block if
     * necessary. If successful, {@link #getKey()} and {@link #getValue()} can
     * be called.
     *
     * @return true if successfully navigated to the next key/value
     */
    @Override
    public boolean next() throws IOException {
      assertSeeked();

      try {
        blockBuffer.position(blockBuffer.position() + KEY_VALUE_LEN_SIZE
            + currKeyLen + currValueLen);
      } catch (IllegalArgumentException e) {
        LOG.error("Current pos = " + blockBuffer.position()
            + "; currKeyLen = " + currKeyLen + "; currValLen = "
            + currValueLen + "; block limit = " + blockBuffer.limit()
            + "; HFile name = " + reader.getName()
            + "; currBlock currBlockOffset = " + block.getOffset());
        throw e;
      }

      if (blockBuffer.remaining() <= 0) {
        long lastDataBlockOffset =
            reader.getTrailer().getLastDataBlockOffset();

        if (block.getOffset() >= lastDataBlockOffset) {
          setNonSeekedState();
          return false;
        }

        // read the next block
        HFileBlock nextBlock = readNextDataBlock();
        if (nextBlock == null) {
          setNonSeekedState();
          return false;
        }

        updateCurrBlock(nextBlock);
        return true;
      }

      // We are still in the same block.
      readKeyValueLen();
      return true;
    }

    /**
     * Scans blocks in the "scanned" section of the {@link HFile} until the next
     * data block is found.
     *
     * @return the next block, or null if there are no more data blocks
     * @throws IOException
     */
    private HFileBlock readNextDataBlock() throws IOException {
      long lastDataBlockOffset = reader.getTrailer().getLastDataBlockOffset();
      if (block == null)
        return null;

      HFileBlock curBlock = block;

      do {
        if (curBlock.getOffset() >= lastDataBlockOffset)
          return null;

        if (curBlock.getOffset() < 0) {
          throw new IOException("Invalid block file offset: " + block);
        }
        curBlock = reader.readBlock(curBlock.getOffset()
            + curBlock.getOnDiskSizeWithHeader(),
            curBlock.getNextBlockOnDiskSizeWithHeader(), cacheBlocks, pread,
            isCompaction);
      } while (!curBlock.getBlockType().equals(BlockType.DATA));

      return curBlock;
    }

    /**
     * Positions this scanner at the start of the file.
     *
     * @return false if empty file; i.e. a call to next would return false and
     *         the current key and value are undefined.
     * @throws IOException
     */
    @Override
    public boolean seekTo() throws IOException {
      if (reader == null) {
        return false;
      }

      if (reader.getTrailer().getEntryCount() == 0) {
        // No data blocks.
        return false;
      }

      long firstDataBlockOffset =
          reader.getTrailer().getFirstDataBlockOffset();
      if (block != null && block.getOffset() == firstDataBlockOffset) {
        blockBuffer.rewind();
        readKeyValueLen();
        return true;
      }

      block = reader.readBlock(firstDataBlockOffset, -1, cacheBlocks, pread,
          isCompaction);
      if (block.getOffset() < 0) {
        throw new IOException("Invalid block offset: " + block.getOffset());
      }
      updateCurrBlock(block);
      return true;
    }

    @Override
    public int seekTo(byte[] key) throws IOException {
      return seekTo(key, 0, key.length);
    }

    /**
     * An internal API function. Seek to the given key, optionally rewinding to
     * the first key of the block before doing the seek.
     *
     * @param key key byte array
     * @param offset key offset in the key byte array
     * @param length key length
     * @param rewind whether to rewind to the first key of the block before
     *        doing the seek. If this is false, we are assuming we never go
     *        back, otherwise the result is undefined.
     * @return -1 if the key is earlier than the first key of the file,
     *         0 if we are at the given key, and 1 if we are past the given key
     * @throws IOException
     */
    private int seekTo(byte[] key, int offset, int length, boolean rewind)
        throws IOException {
      HFileBlockIndex.BlockIndexReader indexReader =
          reader.getDataBlockIndexReader();
      HFileBlock seekToBlock = indexReader.seekToDataBlock(key, offset, length,
          block, cacheBlocks, pread, isCompaction);

      if (seekToBlock == null) {
        // This happens if the key e.g. falls before the beginning of the file.
        return -1;
      }
      return loadBlockAndSeekToKey(seekToBlock, rewind, key, offset, length,
          false);
    }

    @Override
    public int seekTo(byte[] key, int offset, int length) throws IOException {
      // Always rewind to the first key of the block, because the given key
      // might be before or after the current key.
      return seekTo(key, offset, length, true);
    }

    @Override
    public int reseekTo(byte[] key) throws IOException {
      return reseekTo(key, 0, key.length);
    }

    @Override
    public int reseekTo(byte[] key, int offset, int length) throws IOException {
      if (isSeeked()) {
        ByteBuffer bb = getKey();
        int compared = reader.getComparator().compare(key, offset,
            length, bb.array(), bb.arrayOffset(), bb.limit());
        if (compared < 1) {
          // If the required key is less than or equal to current key, then
          // don't do anything.
          return compared;
        }
      }

      // Don't rewind on a reseek operation, because reseek implies that we are
      // always going forward in the file.
      return seekTo(key, offset, length, false);
    }

    private int loadBlockAndSeekToKey(HFileBlock seekToBlock, boolean rewind,
        byte[] key, int offset, int length, boolean seekBefore)
        throws IOException {
      if (block == null || block.getOffset() != seekToBlock.getOffset()) {
        updateCurrBlock(seekToBlock);
      } else if (rewind) {
        blockBuffer.rewind();
      }
      return blockSeek(key, offset, length, seekBefore);
    }

    /**
     * Updates the current block to be the given {@link HFileBlock}. Seeks to
     * the the first key/value pair.
     *
     * @param newBlock the block to make current
     */
    private void updateCurrBlock(HFileBlock newBlock) {
      block = newBlock;
      blockBuffer = block.getBufferWithoutHeader();
      readKeyValueLen();
      blockFetches++;
    }

    private final void readKeyValueLen() {
      blockBuffer.mark();
      currKeyLen = blockBuffer.getInt();
      currValueLen = blockBuffer.getInt();
      blockBuffer.reset();

      if (currKeyLen < 0 || currValueLen < 0
          || currKeyLen > blockBuffer.limit()
          || currValueLen > blockBuffer.limit()) {
        throw new IllegalStateException("Invalid currKeyLen " + currKeyLen
            + " or currValueLen " + currValueLen + ". Block offset: "
            + block.getOffset() + ", block length: " + blockBuffer.limit()
            + ", position: " + blockBuffer.position() + " (without header).");
      }
    }

    /**
     * Within a loaded block, seek looking for the first key that is smaller
     * than (or equal to?) the key we are interested in.
     *
     * A note on the seekBefore: if you have seekBefore = true, AND the first
     * key in the block = key, then you'll get thrown exceptions. The caller has
     * to check for that case and load the previous block as appropriate.
     *
     * @param key the key to find
     * @param seekBefore find the key before the given key in case of exact
     *          match.
     * @return 0 in case of an exact key match, 1 in case of an inexact match
     */
    private int blockSeek(byte[] key, int offset, int length,
        boolean seekBefore) {
      int klen, vlen;
      int lastKeyValueSize = -1;
      do {
        blockBuffer.mark();
        klen = blockBuffer.getInt();
        vlen = blockBuffer.getInt();
        blockBuffer.reset();

        int keyOffset = blockBuffer.arrayOffset() + blockBuffer.position()
            + KEY_VALUE_LEN_SIZE;
        int comp = reader.getComparator().compare(key, offset, length,
            blockBuffer.array(), keyOffset, klen);

        if (comp == 0) {
          if (seekBefore) {
            if (lastKeyValueSize < 0) {
              throw new IllegalStateException("blockSeek with seekBefore "
                  + "at the first key of the block: key="
                  + Bytes.toStringBinary(key) + ", blockOffset="
                  + block.getOffset() + ", onDiskSize="
                  + block.getOnDiskSizeWithHeader());
            }
            blockBuffer.position(blockBuffer.position() - lastKeyValueSize);
            readKeyValueLen();
            return 1; // non exact match.
          }
          currKeyLen = klen;
          currValueLen = vlen;
          return 0; // indicate exact match
        }

        if (comp < 0) {
          if (lastKeyValueSize > 0)
            blockBuffer.position(blockBuffer.position() - lastKeyValueSize);
          readKeyValueLen();
          return 1;
        }

        // The size of this key/value tuple, including key/value length fields.
        lastKeyValueSize = klen + vlen + KEY_VALUE_LEN_SIZE;
        blockBuffer.position(blockBuffer.position() + lastKeyValueSize);
      } while (blockBuffer.remaining() > 0);

      // Seek to the last key we successfully read. This will happen if this is
      // the last key/value pair in the file, in which case the following call
      // to next() has to return false.
      blockBuffer.position(blockBuffer.position() - lastKeyValueSize);
      readKeyValueLen();
      return 1; // didn't exactly find it.
    }

    @Override
    public boolean seekBefore(byte[] key) throws IOException {
      return seekBefore(key, 0, key.length);
    }

    private ByteBuffer getFirstKeyInBlock(HFileBlock curBlock) {
      ByteBuffer buffer = curBlock.getBufferWithoutHeader();
      // It is safe to manipulate this buffer because we own the buffer object.
      buffer.rewind();
      int klen = buffer.getInt();
      buffer.getInt();
      ByteBuffer keyBuff = buffer.slice();
      keyBuff.limit(klen);
      keyBuff.rewind();
      return keyBuff;
    }

    @Override
    public boolean seekBefore(byte[] key, int offset, int length)
        throws IOException {
      HFileBlock seekToBlock =
          reader.getDataBlockIndexReader().seekToDataBlock(key, offset,
              length, block, cacheBlocks, pread, isCompaction);
      if (seekToBlock == null) {
        return false;
      }
      ByteBuffer firstKey = getFirstKeyInBlock(seekToBlock);
      if (reader.getComparator().compare(firstKey.array(),
          firstKey.arrayOffset(), firstKey.limit(), key, offset, length) == 0)
      {
        long previousBlockOffset = seekToBlock.getPrevBlockOffset();
        // The key we are interested in
        if (previousBlockOffset == -1) {
          // we have a 'problem', the key we want is the first of the file.
          return false;
        }

        // It is important that we compute and pass onDiskSize to the block
        // reader so that it does not have to read the header separately to
        // figure out the size.
        seekToBlock = reader.readBlock(previousBlockOffset,
            seekToBlock.getOffset() - previousBlockOffset, cacheBlocks,
            pread, isCompaction);

        // TODO shortcut: seek forward in this block to the last key of the
        // block.
      }
      loadBlockAndSeekToKey(seekToBlock, true, key, offset, length, true);
      return true;
    }

    @Override
    public String getKeyString() {
      return Bytes.toStringBinary(blockBuffer.array(),
          blockBuffer.arrayOffset() + blockBuffer.position()
              + KEY_VALUE_LEN_SIZE, currKeyLen);
    }

    @Override
    public String getValueString() {
      return Bytes.toString(blockBuffer.array(), blockBuffer.arrayOffset()
          + blockBuffer.position() + KEY_VALUE_LEN_SIZE + currKeyLen,
          currValueLen);
    }

  }

  /**
   * Returns a buffer with the Bloom filter metadata. The caller takes
   * ownership of the buffer.
   */
  @Override
  public DataInput getBloomFilterMetadata() throws IOException {
    for (HFileBlock b : loadOnOpenBlocks)
      if (b.getBlockType() == BlockType.BLOOM_META)
        return b.getByteStream();
    return null;
  }

  @Override
  public boolean isFileInfoLoaded() {
    return true; // We load file info in constructor in version 2.
  }

}
