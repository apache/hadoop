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
package org.apache.hadoop.hbase.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.io.hfile.InlineBlockWriter;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

/**
 * Adds methods required for writing a compound Bloom filter to the data
 * section of an {@link org.apache.hadoop.hbase.io.hfile.HFile} to the
 * {@link CompoundBloomFilter} class.
 */
public class CompoundBloomFilterWriter extends CompoundBloomFilterBase
    implements BloomFilterWriter, InlineBlockWriter {

  protected static final Log LOG =
    LogFactory.getLog(CompoundBloomFilterWriter.class);

  /** The current chunk being written to */
  private ByteBloomFilter chunk;

  /** Previous chunk, so that we can create another similar chunk */
  private ByteBloomFilter prevChunk;

  /** Maximum fold factor */
  private int maxFold;

  /** The size of individual Bloom filter chunks to create */
  private int chunkByteSize;

  /** A Bloom filter chunk enqueued for writing */
  private static class ReadyChunk {
    int chunkId;
    byte[] firstKey;
    ByteBloomFilter chunk;
  }

  private Queue<ReadyChunk> readyChunks = new LinkedList<ReadyChunk>();

  /** The first key in the current Bloom filter chunk. */
  private byte[] firstKeyInChunk = null;

  private HFileBlockIndex.BlockIndexWriter bloomBlockIndexWriter =
      new HFileBlockIndex.BlockIndexWriter();

  /** Whether to cache-on-write compound Bloom filter chunks */
  private boolean cacheOnWrite;

  /**
   * @param chunkByteSizeHint
   *          each chunk's size in bytes. The real chunk size might be different
   *          as required by the fold factor.
   * @param errorRate
   *          target false positive rate
   * @param hashType
   *          hash function type to use
   * @param maxFold
   *          maximum degree of folding allowed
   */
  public CompoundBloomFilterWriter(int chunkByteSizeHint, float errorRate,
      int hashType, int maxFold, boolean cacheOnWrite,
      RawComparator<byte[]> comparator) {
    chunkByteSize = ByteBloomFilter.computeFoldableByteSize(
        chunkByteSizeHint * 8, maxFold);

    this.errorRate = errorRate;
    this.hashType = hashType;
    this.maxFold = maxFold;
    this.cacheOnWrite = cacheOnWrite;
    this.comparator = comparator;
  }

  @Override
  public boolean shouldWriteBlock(boolean closing) {
    enqueueReadyChunk(closing);
    return !readyChunks.isEmpty();
  }

  /**
   * Enqueue the current chunk if it is ready to be written out.
   *
   * @param closing true if we are closing the file, so we do not expect new
   *        keys to show up
   */
  private void enqueueReadyChunk(boolean closing) {
    if (chunk == null ||
        (chunk.getKeyCount() < chunk.getMaxKeys() && !closing)) {
      return;
    }

    if (firstKeyInChunk == null) {
      throw new NullPointerException("Trying to enqueue a chunk, " +
          "but first key is null: closing=" + closing + ", keyCount=" +
          chunk.getKeyCount() + ", maxKeys=" + chunk.getMaxKeys());
    }

    ReadyChunk readyChunk = new ReadyChunk();
    readyChunk.chunkId = numChunks - 1;
    readyChunk.chunk = chunk;
    readyChunk.firstKey = firstKeyInChunk;
    readyChunks.add(readyChunk);

    long prevMaxKeys = chunk.getMaxKeys();
    long prevByteSize = chunk.getByteSize();

    chunk.compactBloom();

    if (LOG.isDebugEnabled() && prevByteSize != chunk.getByteSize()) {
      LOG.debug("Compacted Bloom chunk #" + readyChunk.chunkId + " from ["
          + prevMaxKeys + " max keys, " + prevByteSize + " bytes] to ["
          + chunk.getMaxKeys() + " max keys, " + chunk.getByteSize()
          + " bytes]");
    }

    totalMaxKeys += chunk.getMaxKeys();
    totalByteSize += chunk.getByteSize();

    firstKeyInChunk = null;
    prevChunk = chunk;
    chunk = null;
  }

  /**
   * Adds a Bloom filter key. This key must be greater than the previous key,
   * as defined by the comparator this compound Bloom filter is configured
   * with. For efficiency, key monotonicity is not checked here. See
   * {@link org.apache.hadoop.hbase.regionserver.StoreFile.Writer#append(
   * org.apache.hadoop.hbase.KeyValue)} for the details of deduplication.
   */
  @Override
  public void add(byte[] bloomKey, int keyOffset, int keyLength) {
    if (bloomKey == null)
      throw new NullPointerException();

    enqueueReadyChunk(false);

    if (chunk == null) {
      if (firstKeyInChunk != null) {
        throw new IllegalStateException("First key in chunk already set: "
            + Bytes.toStringBinary(firstKeyInChunk));
      }
      firstKeyInChunk = Arrays.copyOfRange(bloomKey, keyOffset, keyOffset
          + keyLength);

      if (prevChunk == null) {
        // First chunk
        chunk = ByteBloomFilter.createBySize(chunkByteSize, errorRate,
            hashType, maxFold);
      } else {
        // Use the same parameters as the last chunk, but a new array and
        // a zero key count.
        chunk = prevChunk.createAnother();
      }

      if (chunk.getKeyCount() != 0) {
        throw new IllegalStateException("keyCount=" + chunk.getKeyCount()
            + " > 0");
      }

      chunk.allocBloom();
      ++numChunks;
    }

    chunk.add(bloomKey, keyOffset, keyLength);
    ++totalKeyCount;
  }

  @Override
  public void writeInlineBlock(DataOutput out) throws IOException {
    // We don't remove the chunk from the queue here, because we might need it
    // again for cache-on-write.
    ReadyChunk readyChunk = readyChunks.peek();

    ByteBloomFilter readyChunkBloom = readyChunk.chunk;
    readyChunkBloom.getDataWriter().write(out);
  }

  @Override
  public void blockWritten(long offset, int onDiskSize, int uncompressedSize) {
    ReadyChunk readyChunk = readyChunks.remove();
    bloomBlockIndexWriter.addEntry(readyChunk.firstKey, offset, onDiskSize);
  }

  @Override
  public BlockType getInlineBlockType() {
    return BlockType.BLOOM_CHUNK;
  }

  private class MetaWriter implements Writable {
    protected MetaWriter() {}

    @Override
    public void readFields(DataInput in) throws IOException {
      throw new IOException("Cant read with this class.");
    }

    /**
     * This is modeled after {@link ByteBloomFilter.MetaWriter} for simplicity,
     * although the two metadata formats do not have to be consistent. This
     * does have to be consistent with how {@link
     * CompoundBloomFilter#CompoundBloomFilter(DataInput,
     * org.apache.hadoop.hbase.io.hfile.HFile.Reader)} reads fields.
     */
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(VERSION);

      out.writeLong(getByteSize());
      out.writeInt(prevChunk.getHashCount());
      out.writeInt(prevChunk.getHashType());
      out.writeLong(getKeyCount());
      out.writeLong(getMaxKeys());

      // Fields that don't have equivalents in ByteBloomFilter.
      out.writeInt(numChunks);
      Bytes.writeByteArray(out,
          Bytes.toBytes(comparator.getClass().getName()));

      // Write a single-level index without compression or block header.
      bloomBlockIndexWriter.writeSingleLevelIndex(out, "Bloom filter");
    }
  }

  @Override
  public Writable getMetaWriter() {
    return new MetaWriter();
  }

  @Override
  public void compactBloom() {
  }

  @Override
  public void allocBloom() {
    // Nothing happens here. All allocation happens on demand.
  }

  @Override
  public Writable getDataWriter() {
    return null;
  }

  @Override
  public boolean cacheOnWrite() {
    return cacheOnWrite;
  }

}
