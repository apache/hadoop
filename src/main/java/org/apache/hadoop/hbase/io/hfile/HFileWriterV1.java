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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KeyComparator;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.Compressor;

/**
 * Writes version 1 HFiles. Mainly used for testing backwards-compatibilty.
 */
public class HFileWriterV1 extends AbstractHFileWriter {

  /** Meta data block name for bloom filter parameters. */
  static final String BLOOM_FILTER_META_KEY = "BLOOM_FILTER_META";

  /** Meta data block name for bloom filter bits. */
  public static final String BLOOM_FILTER_DATA_KEY = "BLOOM_FILTER_DATA";

  private static final Log LOG = LogFactory.getLog(HFileWriterV1.class);

  // A stream made per block written.
  private DataOutputStream out;

  // Offset where the current block began.
  private long blockBegin;

  // First keys of every block.
  private ArrayList<byte[]> blockKeys = new ArrayList<byte[]>();

  // Block offset in backing stream.
  private ArrayList<Long> blockOffsets = new ArrayList<Long>();

  // Raw (decompressed) data size.
  private ArrayList<Integer> blockDataSizes = new ArrayList<Integer>();

  private Compressor compressor;

  // Additional byte array output stream used to fill block cache
  private ByteArrayOutputStream baos;
  private DataOutputStream baosDos;
  private int blockNumber = 0;

  static class WriterFactoryV1 extends HFile.WriterFactory {

    WriterFactoryV1(Configuration conf, CacheConfig cacheConf) {
      super(conf, cacheConf);
    }

    @Override
    public Writer createWriter(FileSystem fs, Path path) throws IOException {
      return new HFileWriterV1(conf, cacheConf, fs, path);
    }

    @Override
    public Writer createWriter(FileSystem fs, Path path, int blockSize,
        Compression.Algorithm compressAlgo, final KeyComparator comparator)
        throws IOException {
      return new HFileWriterV1(conf, cacheConf, fs, path, blockSize,
          compressAlgo, comparator);
    }

    @Override
    public Writer createWriter(FileSystem fs, Path path, int blockSize,
        String compressAlgoName,
        final KeyComparator comparator) throws IOException {
      return new HFileWriterV1(conf, cacheConf, fs, path, blockSize,
          compressAlgoName, comparator);
    }

    @Override
    public Writer createWriter(final FSDataOutputStream ostream,
        final int blockSize, final String compress,
        final KeyComparator comparator) throws IOException {
      return new HFileWriterV1(cacheConf, ostream, blockSize, compress,
          comparator);
    }

    @Override
    public Writer createWriter(final FSDataOutputStream ostream,
        final int blockSize, final Compression.Algorithm compress,
        final KeyComparator c) throws IOException {
      return new HFileWriterV1(cacheConf, ostream, blockSize, compress, c);
    }
  }

  /** Constructor that uses all defaults for compression and block size. */
  public HFileWriterV1(Configuration conf, CacheConfig cacheConf,
      FileSystem fs, Path path)
      throws IOException {
    this(conf, cacheConf, fs, path, HFile.DEFAULT_BLOCKSIZE,
        HFile.DEFAULT_COMPRESSION_ALGORITHM,
        null);
  }

  /**
   * Constructor that takes a path, creates and closes the output stream. Takes
   * compression algorithm name as string.
   */
  public HFileWriterV1(Configuration conf, CacheConfig cacheConf, FileSystem fs,
      Path path, int blockSize, String compressAlgoName,
      final KeyComparator comparator) throws IOException {
    this(conf, cacheConf, fs, path, blockSize,
        compressionByName(compressAlgoName), comparator);
  }

  /** Constructor that takes a path, creates and closes the output stream. */
  public HFileWriterV1(Configuration conf, CacheConfig cacheConf, FileSystem fs,
      Path path, int blockSize, Compression.Algorithm compress,
      final KeyComparator comparator) throws IOException {
    super(cacheConf, createOutputStream(conf, fs, path), path,
        blockSize, compress, comparator);
  }

  /** Constructor that takes a stream. */
  public HFileWriterV1(CacheConfig cacheConf,
      final FSDataOutputStream outputStream, final int blockSize,
      final String compressAlgoName, final KeyComparator comparator)
      throws IOException {
    this(cacheConf, outputStream, blockSize,
        Compression.getCompressionAlgorithmByName(compressAlgoName),
        comparator);
  }

  /** Constructor that takes a stream. */
  public HFileWriterV1(CacheConfig cacheConf,
      final FSDataOutputStream outputStream, final int blockSize,
      final Compression.Algorithm compress, final KeyComparator comparator)
      throws IOException {
    super(cacheConf, outputStream, null, blockSize, compress, comparator);
  }

  /**
   * If at block boundary, opens new block.
   *
   * @throws IOException
   */
  private void checkBlockBoundary() throws IOException {
    if (this.out != null && this.out.size() < blockSize)
      return;
    finishBlock();
    newBlock();
  }

  /**
   * Do the cleanup if a current block.
   *
   * @throws IOException
   */
  private void finishBlock() throws IOException {
    if (this.out == null)
      return;
    long startTimeNs = System.nanoTime();

    int size = releaseCompressingStream(this.out);
    this.out = null;
    blockKeys.add(firstKeyInBlock);
    blockOffsets.add(Long.valueOf(blockBegin));
    blockDataSizes.add(Integer.valueOf(size));
    this.totalUncompressedBytes += size;

    HFile.writeTimeNano.addAndGet(System.nanoTime() - startTimeNs);
    HFile.writeOps.incrementAndGet();

    if (cacheConf.shouldCacheDataOnWrite()) {
      baosDos.flush();
      byte[] bytes = baos.toByteArray();
      cacheConf.getBlockCache().cacheBlock(
          HFile.getBlockCacheKey(name, blockBegin),
          new HFileBlock(BlockType.DATA,
              (int) (outputStream.getPos() - blockBegin), bytes.length, -1,
              ByteBuffer.wrap(bytes, 0, bytes.length), true, blockBegin));
      baosDos.close();
    }
    blockNumber++;
  }

  /**
   * Ready a new block for writing.
   *
   * @throws IOException
   */
  private void newBlock() throws IOException {
    // This is where the next block begins.
    blockBegin = outputStream.getPos();
    this.out = getCompressingStream();
    BlockType.DATA.write(out);
    firstKeyInBlock = null;
    if (cacheConf.shouldCacheDataOnWrite()) {
      this.baos = new ByteArrayOutputStream();
      this.baosDos = new DataOutputStream(baos);
      baosDos.write(HFileBlock.DUMMY_HEADER);
    }
  }

  /**
   * Sets up a compressor and creates a compression stream on top of
   * this.outputStream. Get one per block written.
   *
   * @return A compressing stream; if 'none' compression, returned stream does
   * not compress.
   *
   * @throws IOException
   *
   * @see {@link #releaseCompressingStream(DataOutputStream)}
   */
  private DataOutputStream getCompressingStream() throws IOException {
    this.compressor = compressAlgo.getCompressor();
    // Get new DOS compression stream. In tfile, the DOS, is not closed,
    // just finished, and that seems to be fine over there. TODO: Check
    // no memory retention of the DOS. Should I disable the 'flush' on the
    // DOS as the BCFile over in tfile does? It wants to make it so flushes
    // don't go through to the underlying compressed stream. Flush on the
    // compressed downstream should be only when done. I was going to but
    // looks like when we call flush in here, its legitimate flush that
    // should go through to the compressor.
    OutputStream os = this.compressAlgo.createCompressionStream(
        this.outputStream, this.compressor, 0);
    return new DataOutputStream(os);
  }

  /**
   * Let go of block compressor and compressing stream gotten in call {@link
   * #getCompressingStream}.
   *
   * @param dos
   *
   * @return How much was written on this stream since it was taken out.
   *
   * @see #getCompressingStream()
   *
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
    // Write length of key and value and then actual key and value bytes.
    this.out.writeInt(klength);
    totalKeyLength += klength;
    this.out.writeInt(vlength);
    totalValueLength += vlength;
    this.out.write(key, koffset, klength);
    this.out.write(value, voffset, vlength);
    // Are we the first key in this block?
    if (this.firstKeyInBlock == null) {
      // Copy the key.
      this.firstKeyInBlock = new byte[klength];
      System.arraycopy(key, koffset, this.firstKeyInBlock, 0, klength);
    }
    this.lastKeyBuffer = key;
    this.lastKeyOffset = koffset;
    this.lastKeyLength = klength;
    this.entryCount++;
    // If we are pre-caching blocks on write, fill byte array stream
    if (cacheConf.shouldCacheDataOnWrite()) {
      this.baosDos.writeInt(klength);
      this.baosDos.writeInt(vlength);
      this.baosDos.write(key, koffset, klength);
      this.baosDos.write(value, voffset, vlength);
    }
  }

  public void close() throws IOException {
    if (this.outputStream == null) {
      return;
    }
    // Write out the end of the data blocks, then write meta data blocks.
    // followed by fileinfo, data block index and meta block index.

    finishBlock();

    FixedFileTrailer trailer = new FixedFileTrailer(1);

    // Write out the metadata blocks if any.
    ArrayList<Long> metaOffsets = null;
    ArrayList<Integer> metaDataSizes = null;
    if (metaNames.size() > 0) {
      metaOffsets = new ArrayList<Long>(metaNames.size());
      metaDataSizes = new ArrayList<Integer>(metaNames.size());
      for (int i = 0; i < metaNames.size(); ++i) {
        // store the beginning offset
        long curPos = outputStream.getPos();
        metaOffsets.add(curPos);
        // write the metadata content
        DataOutputStream dos = getCompressingStream();
        BlockType.META.write(dos);
        metaData.get(i).write(dos);
        int size = releaseCompressingStream(dos);
        // store the metadata size
        metaDataSizes.add(size);
      }
    }

    writeFileInfo(trailer, outputStream);

    // Write the data block index.
    trailer.setLoadOnOpenOffset(writeBlockIndex(this.outputStream,
        this.blockKeys, this.blockOffsets, this.blockDataSizes));
    LOG.info("Wrote a version 1 block index with " + this.blockKeys.size()
        + " keys");

    if (metaNames.size() > 0) {
      // Write the meta index.
      writeBlockIndex(this.outputStream, metaNames, metaOffsets, metaDataSizes);
    }

    // Now finish off the trailer.
    trailer.setDataIndexCount(blockKeys.size());

    finishClose(trailer);
  }

  @Override
  protected void finishFileInfo() throws IOException {
    super.finishFileInfo();

    // In version 1, we store comparator name in the file info.
    fileInfo.append(FileInfo.COMPARATOR,
        Bytes.toBytes(comparator.getClass().getName()), false);
  }

  @Override
  public void addInlineBlockWriter(InlineBlockWriter bloomWriter) {
    // Inline blocks only exist in HFile format version 2.
    throw new UnsupportedOperationException();
  }

  /**
   * Version 1 Bloom filters are stored in two meta blocks with two different
   * keys.
   */
  @Override
  public void addBloomFilter(BloomFilterWriter bfw) {
    appendMetaBlock(BLOOM_FILTER_META_KEY,
        bfw.getMetaWriter());
    Writable dataWriter = bfw.getDataWriter();
    if (dataWriter != null) {
      appendMetaBlock(BLOOM_FILTER_DATA_KEY, dataWriter);
    }
  }

  /**
   * Write out the index in the version 1 format. This conforms to the legacy
   * version 1 format, but can still be read by
   * {@link HFileBlockIndex.BlockIndexReader#readRootIndex(java.io.DataInputStream,
   * int)}.
   *
   * @param out the stream to write to
   * @param keys
   * @param offsets
   * @param uncompressedSizes in contrast with a version 2 root index format,
   *          the sizes stored in the version 1 are uncompressed sizes
   * @return
   * @throws IOException
   */
  private static long writeBlockIndex(final FSDataOutputStream out,
      final List<byte[]> keys, final List<Long> offsets,
      final List<Integer> uncompressedSizes) throws IOException {
    long pos = out.getPos();
    // Don't write an index if nothing in the index.
    if (keys.size() > 0) {
      BlockType.INDEX_V1.write(out);
      // Write the index.
      for (int i = 0; i < keys.size(); ++i) {
        out.writeLong(offsets.get(i).longValue());
        out.writeInt(uncompressedSizes.get(i).intValue());
        byte[] key = keys.get(i);
        Bytes.writeByteArray(out, key);
      }
    }
    return pos;
  }

}