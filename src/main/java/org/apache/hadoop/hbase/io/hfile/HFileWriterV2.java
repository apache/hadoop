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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KeyComparator;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.io.hfile.HFileBlock.BlockWritable;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * Writes HFile format version 2.
 */
public class HFileWriterV2 extends AbstractHFileWriter {

  /** Inline block writers for multi-level block index and compound Blooms. */
  private List<InlineBlockWriter> inlineBlockWriters =
      new ArrayList<InlineBlockWriter>();

  /** Unified version 2 block writer */
  private HFileBlock.Writer fsBlockWriter;

  private HFileBlockIndex.BlockIndexWriter dataBlockIndexWriter;
  private HFileBlockIndex.BlockIndexWriter metaBlockIndexWriter;

  /** The offset of the first data block or -1 if the file is empty. */
  private long firstDataBlockOffset = -1;

  /** The offset of the last data block or 0 if the file is empty. */
  private long lastDataBlockOffset;

  /** Additional data items to be written to the "load-on-open" section. */
  private List<BlockWritable> additionalLoadOnOpenData =
    new ArrayList<BlockWritable>();

  static class WriterFactoryV2 extends HFile.WriterFactory {

    WriterFactoryV2(Configuration conf) { super(conf); }

    @Override
    public Writer createWriter(FileSystem fs, Path path)
        throws IOException {
      return new HFileWriterV2(conf, fs, path);
    }

    @Override
    public Writer createWriter(FileSystem fs, Path path, int blockSize,
        Compression.Algorithm compress,
        final KeyComparator comparator) throws IOException {
      return new HFileWriterV2(conf, fs, path, blockSize,
          compress, comparator);
    }

    @Override
    public Writer createWriter(FileSystem fs, Path path, int blockSize,
        String compress, final KeyComparator comparator)
        throws IOException {
      return new HFileWriterV2(conf, fs, path, blockSize,
          compress, comparator);
    }

    @Override
    public Writer createWriter(final FSDataOutputStream ostream,
        final int blockSize, final String compress,
        final KeyComparator comparator) throws IOException {
      return new HFileWriterV2(conf, ostream, blockSize, compress, comparator);
    }

    @Override
    public Writer createWriter(final FSDataOutputStream ostream,
        final int blockSize, final Compression.Algorithm compress,
        final KeyComparator c) throws IOException {
      return new HFileWriterV2(conf, ostream, blockSize, compress, c);
    }
  }

  /** Constructor that uses all defaults for compression and block size. */
  public HFileWriterV2(Configuration conf, FileSystem fs, Path path)
      throws IOException {
    this(conf, fs, path, HFile.DEFAULT_BLOCKSIZE,
        HFile.DEFAULT_COMPRESSION_ALGORITHM, null);
  }

  /**
   * Constructor that takes a path, creates and closes the output stream. Takes
   * compression algorithm name as string.
   */
  public HFileWriterV2(Configuration conf, FileSystem fs, Path path,
      int blockSize, String compressAlgoName,
      final KeyComparator comparator) throws IOException {
    this(conf, fs, path, blockSize,
        compressionByName(compressAlgoName), comparator);
  }

  /** Constructor that takes a path, creates and closes the output stream. */
  public HFileWriterV2(Configuration conf, FileSystem fs, Path path,
      int blockSize, Compression.Algorithm compressAlgo,
      final KeyComparator comparator) throws IOException {
    super(conf, createOutputStream(conf, fs, path), path,
        blockSize, compressAlgo, comparator);
    finishInit(conf);
  }

  /** Constructor that takes a stream. */
  public HFileWriterV2(final Configuration conf,
      final FSDataOutputStream outputStream, final int blockSize,
      final String compressAlgoName, final KeyComparator comparator)
      throws IOException {
    this(conf, outputStream, blockSize,
        Compression.getCompressionAlgorithmByName(compressAlgoName),
        comparator);
  }

  /** Constructor that takes a stream. */
  public HFileWriterV2(final Configuration conf,
      final FSDataOutputStream outputStream, final int blockSize,
      final Compression.Algorithm compress, final KeyComparator comparator)
      throws IOException {
    super(conf, outputStream, null, blockSize, compress, comparator);
    finishInit(conf);
  }

  /** Additional initialization steps */
  private void finishInit(final Configuration conf) {
    if (fsBlockWriter != null)
      throw new IllegalStateException("finishInit called twice");

    // HFile filesystem-level (non-caching) block writer
    fsBlockWriter = new HFileBlock.Writer(compressAlgo);

    // Data block index writer
    dataBlockIndexWriter = new HFileBlockIndex.BlockIndexWriter(fsBlockWriter,
        cacheIndexBlocksOnWrite ? blockCache : null,
        cacheIndexBlocksOnWrite ? name : null);
    dataBlockIndexWriter.setMaxChunkSize(
        HFileBlockIndex.getMaxChunkSize(conf));
    inlineBlockWriters.add(dataBlockIndexWriter);

    // Meta data block index writer
    metaBlockIndexWriter = new HFileBlockIndex.BlockIndexWriter();
  }

  /**
   * At a block boundary, write all the inline blocks and opens new block.
   *
   * @throws IOException
   */
  private void checkBlockBoundary() throws IOException {
    if (fsBlockWriter.blockSizeWritten() < blockSize)
      return;

    finishBlock();
    writeInlineBlocks(false);
    newBlock();
  }

  /** Clean up the current block */
  private void finishBlock() throws IOException {
    if (!fsBlockWriter.isWriting() || fsBlockWriter.blockSizeWritten() == 0)
      return;

    long startTimeNs = System.nanoTime();

    // Update the first data block offset for scanning.
    if (firstDataBlockOffset == -1)
      firstDataBlockOffset = outputStream.getPos();

    // Update the last data block offset
    lastDataBlockOffset = outputStream.getPos();

    fsBlockWriter.writeHeaderAndData(outputStream);

    int onDiskSize = fsBlockWriter.getOnDiskSizeWithHeader();
    dataBlockIndexWriter.addEntry(firstKeyInBlock, lastDataBlockOffset,
        onDiskSize);
    totalUncompressedBytes += fsBlockWriter.getUncompressedSizeWithHeader();

    HFile.writeTimeNano.addAndGet(System.nanoTime() - startTimeNs);
    HFile.writeOps.incrementAndGet();

    if (cacheDataBlocksOnWrite) {
      blockCache.cacheBlock(HFile.getBlockCacheKey(name, lastDataBlockOffset),
          fsBlockWriter.getBlockForCaching());
    }
  }

  /** Gives inline block writers an opportunity to contribute blocks. */
  private void writeInlineBlocks(boolean closing) throws IOException {
    for (InlineBlockWriter ibw : inlineBlockWriters) {
      while (ibw.shouldWriteBlock(closing)) {
        long offset = outputStream.getPos();
        boolean cacheThisBlock = ibw.cacheOnWrite();
        ibw.writeInlineBlock(fsBlockWriter.startWriting(
            ibw.getInlineBlockType(), cacheThisBlock));
        fsBlockWriter.writeHeaderAndData(outputStream);
        ibw.blockWritten(offset, fsBlockWriter.getOnDiskSizeWithHeader(),
            fsBlockWriter.getUncompressedSizeWithoutHeader());

        if (cacheThisBlock) {
          // Cache this block on write.
          blockCache.cacheBlock(HFile.getBlockCacheKey(name, offset),
              fsBlockWriter.getBlockForCaching());
        }
      }
    }
  }

  /**
   * Ready a new block for writing.
   *
   * @throws IOException
   */
  private void newBlock() throws IOException {
    // This is where the next block begins.
    fsBlockWriter.startWriting(BlockType.DATA, cacheDataBlocksOnWrite);
    firstKeyInBlock = null;
  }

  /**
   * Add a meta block to the end of the file. Call before close(). Metadata
   * blocks are expensive. Fill one with a bunch of serialized data rather than
   * do a metadata block per metadata instance. If metadata is small, consider
   * adding to file info using {@link #appendFileInfo(byte[], byte[])}
   *
   * @param metaBlockName
   *          name of the block
   * @param content
   *          will call readFields to get data later (DO NOT REUSE)
   */
  @Override
  public void appendMetaBlock(String metaBlockName, Writable content) {
    byte[] key = Bytes.toBytes(metaBlockName);
    int i;
    for (i = 0; i < metaNames.size(); ++i) {
      // stop when the current key is greater than our own
      byte[] cur = metaNames.get(i);
      if (Bytes.BYTES_RAWCOMPARATOR.compare(cur, 0, cur.length, key, 0,
          key.length) > 0) {
        break;
      }
    }
    metaNames.add(i, key);
    metaData.add(i, content);
  }

  /**
   * Add key/value to file. Keys must be added in an order that agrees with the
   * Comparator passed on construction.
   *
   * @param kv
   *          KeyValue to add. Cannot be empty nor null.
   * @throws IOException
   */
  @Override
  public void append(final KeyValue kv) throws IOException {
    append(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength(),
        kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
  }

  /**
   * Add key/value to file. Keys must be added in an order that agrees with the
   * Comparator passed on construction.
   *
   * @param key
   *          Key to add. Cannot be empty nor null.
   * @param value
   *          Value to add. Cannot be empty nor null.
   * @throws IOException
   */
  @Override
  public void append(final byte[] key, final byte[] value) throws IOException {
    append(key, 0, key.length, value, 0, value.length);
  }

  /**
   * Add key/value to file. Keys must be added in an order that agrees with the
   * Comparator passed on construction.
   *
   * @param key
   * @param koffset
   * @param klength
   * @param value
   * @param voffset
   * @param vlength
   * @throws IOException
   */
  private void append(final byte[] key, final int koffset, final int klength,
      final byte[] value, final int voffset, final int vlength)
      throws IOException {
    boolean dupKey = checkKey(key, koffset, klength);
    checkValue(value, voffset, vlength);
    if (!dupKey) {
      checkBlockBoundary();
    }

    if (!fsBlockWriter.isWriting())
      newBlock();

    // Write length of key and value and then actual key and value bytes.
    {
      DataOutputStream out = fsBlockWriter.getUserDataStream();
      out.writeInt(klength);
      totalKeyLength += klength;
      out.writeInt(vlength);
      totalValueLength += vlength;
      out.write(key, koffset, klength);
      out.write(value, voffset, vlength);
    }

    // Are we the first key in this block?
    if (firstKeyInBlock == null) {
      // Copy the key.
      firstKeyInBlock = new byte[klength];
      System.arraycopy(key, koffset, firstKeyInBlock, 0, klength);
    }

    lastKeyBuffer = key;
    lastKeyOffset = koffset;
    lastKeyLength = klength;
    entryCount++;
  }

  @Override
  public void close() throws IOException {
    if (outputStream == null) {
      return;
    }
    // Write out the end of the data blocks, then write meta data blocks.
    // followed by fileinfo, data block index and meta block index.

    finishBlock();
    writeInlineBlocks(true);

    FixedFileTrailer trailer = new FixedFileTrailer(2);

    // Write out the metadata blocks if any.
    if (!metaNames.isEmpty()) {
      for (int i = 0; i < metaNames.size(); ++i) {
        // store the beginning offset
        long offset = outputStream.getPos();
        // write the metadata content
        DataOutputStream dos = fsBlockWriter.startWriting(BlockType.META,
            cacheDataBlocksOnWrite);
        metaData.get(i).write(dos);

        fsBlockWriter.writeHeaderAndData(outputStream);

        // Add the new meta block to the meta index.
        metaBlockIndexWriter.addEntry(metaNames.get(i), offset,
            fsBlockWriter.getOnDiskSizeWithHeader());
      }
    }

    // Load-on-open section.

    // Data block index.
    //
    // In version 2, this section of the file starts with the root level data
    // block index. We call a function that writes intermediate-level blocks
    // first, then root level, and returns the offset of the root level block
    // index.

    long rootIndexOffset = dataBlockIndexWriter.writeIndexBlocks(outputStream);
    trailer.setLoadOnOpenOffset(rootIndexOffset);

    // Meta block index.
    metaBlockIndexWriter.writeSingleLevelIndex(fsBlockWriter.startWriting(
        BlockType.ROOT_INDEX, false), "meta");
    fsBlockWriter.writeHeaderAndData(outputStream);

    // File info
    writeFileInfo(trailer, fsBlockWriter.startWriting(BlockType.FILE_INFO,
        false));
    fsBlockWriter.writeHeaderAndData(outputStream);

    // Load-on-open data supplied by higher levels, e.g. Bloom filters.
    for (BlockWritable w : additionalLoadOnOpenData)
      fsBlockWriter.writeBlock(w, outputStream);

    // Now finish off the trailer.
    trailer.setNumDataIndexLevels(dataBlockIndexWriter.getNumLevels());
    trailer.setUncompressedDataIndexSize(
        dataBlockIndexWriter.getTotalUncompressedSize());
    trailer.setFirstDataBlockOffset(firstDataBlockOffset);
    trailer.setLastDataBlockOffset(lastDataBlockOffset);
    trailer.setComparatorClass(comparator.getClass());
    trailer.setDataIndexCount(dataBlockIndexWriter.getNumRootEntries());

    finishClose(trailer);

    fsBlockWriter.releaseCompressor();
  }

  @Override
  public void addInlineBlockWriter(InlineBlockWriter ibw) {
    inlineBlockWriters.add(ibw);
    if (blockCache == null && ibw.cacheOnWrite())
      initBlockCache();
  }

  @Override
  public void addBloomFilter(final BloomFilterWriter bfw) {
    if (bfw.getKeyCount() <= 0)
      return;

    additionalLoadOnOpenData.add(new BlockWritable() {
      @Override
      public BlockType getBlockType() {
        return BlockType.BLOOM_META;
      }

      @Override
      public void writeToBlock(DataOutput out) throws IOException {
        bfw.getMetaWriter().write(out);
        Writable dataWriter = bfw.getDataWriter();
        if (dataWriter != null)
          dataWriter.write(out);
      }
    });
  }

}
