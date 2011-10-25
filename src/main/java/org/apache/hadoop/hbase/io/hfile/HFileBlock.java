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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.hbase.io.DoubleOutputStream;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CompoundBloomFilter;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import com.google.common.base.Preconditions;

import static org.apache.hadoop.hbase.io.hfile.BlockType.MAGIC_LENGTH;
import static org.apache.hadoop.hbase.io.hfile.Compression.Algorithm.NONE;

/**
 * Reading {@link HFile} version 1 and 2 blocks, and writing version 2 blocks.
 * <ul>
 * <li>In version 1 all blocks are always compressed or uncompressed, as
 * specified by the {@link HFile}'s compression algorithm, with a type-specific
 * magic record stored in the beginning of the compressed data (i.e. one needs
 * to uncompress the compressed block to determine the block type). There is
 * only a single compression algorithm setting for all blocks. Offset and size
 * information from the block index are required to read a block.
 * <li>In version 2 a block is structured as follows:
 * <ul>
 * <li>Magic record identifying the block type (8 bytes)
 * <li>Compressed block size, header not included (4 bytes)
 * <li>Uncompressed block size, header not included (4 bytes)
 * <li>The offset of the previous block of the same type (8 bytes). This is
 * used to be able to navigate to the previous block without going to the block
 * index.
 * <li>Compressed data (or uncompressed data if compression is disabled). The
 * compression algorithm is the same for all the blocks in the {@link HFile},
 * similarly to what was done in version 1.
 * </ul>
 * </ul>
 * The version 2 block representation in the block cache is the same as above,
 * except that the data section is always uncompressed in the cache.
 */
public class HFileBlock implements Cacheable {

  /** The size of a version 2 {@link HFile} block header */
  public static final int HEADER_SIZE = MAGIC_LENGTH + 2 * Bytes.SIZEOF_INT
      + Bytes.SIZEOF_LONG;

  /** Just an array of bytes of the right size. */
  public static final byte[] DUMMY_HEADER = new byte[HEADER_SIZE];

  public static final int BYTE_BUFFER_HEAP_SIZE = (int) ClassSize.estimateBase(
      ByteBuffer.wrap(new byte[0], 0, 0).getClass(), false);

  static final int EXTRA_SERIALIZATION_SPACE = Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;


  private static final CacheableDeserializer<Cacheable> blockDeserializer =
  new CacheableDeserializer<Cacheable>() {
    public HFileBlock deserialize(ByteBuffer buf) throws IOException{
      ByteBuffer newByteBuffer = ByteBuffer.allocate(buf.limit()
          - HFileBlock.EXTRA_SERIALIZATION_SPACE);
      buf.limit(buf.limit()
          - HFileBlock.EXTRA_SERIALIZATION_SPACE).rewind();
      newByteBuffer.put(buf);
      HFileBlock ourBuffer = new HFileBlock(newByteBuffer);

      buf.position(buf.limit());
      buf.limit(buf.limit() + HFileBlock.EXTRA_SERIALIZATION_SPACE);
      ourBuffer.offset = buf.getLong();
      ourBuffer.nextBlockOnDiskSizeWithHeader = buf.getInt();
      return ourBuffer;
    }
  };
  private BlockType blockType;
  private final int onDiskSizeWithoutHeader;
  private final int uncompressedSizeWithoutHeader;
  private final long prevBlockOffset;
  private ByteBuffer buf;

  /**
   * The offset of this block in the file. Populated by the reader for
   * convenience of access. This offset is not part of the block header.
   */
  private long offset = -1;

  /**
   * The on-disk size of the next block, including the header, obtained by
   * peeking into the first {@link HEADER_SIZE} bytes of the next block's
   * header, or -1 if unknown.
   */
  private int nextBlockOnDiskSizeWithHeader = -1;

  /**
   * Creates a new {@link HFile} block from the given fields. This constructor
   * is mostly used when the block data has already been read and uncompressed,
   * and is sitting in a byte buffer.
   *
   * @param blockType the type of this block, see {@link BlockType}
   * @param onDiskSizeWithoutHeader compressed size of the block if compression
   *          is used, otherwise uncompressed size, header size not included
   * @param uncompressedSizeWithoutHeader uncompressed size of the block,
   *          header size not included. Equals onDiskSizeWithoutHeader if
   *          compression is disabled.
   * @param prevBlockOffset the offset of the previous block in the
   *          {@link HFile}
   * @param buf block header ({@link #HEADER_SIZE} bytes) followed by
   *          uncompressed data. This
   * @param fillHeader true to fill in the first {@link #HEADER_SIZE} bytes of
   *          the buffer based on the header fields provided
   * @param offset the file offset the block was read from
   */
  public HFileBlock(BlockType blockType, int onDiskSizeWithoutHeader,
      int uncompressedSizeWithoutHeader, long prevBlockOffset, ByteBuffer buf,
      boolean fillHeader, long offset) {
    this.blockType = blockType;
    this.onDiskSizeWithoutHeader = onDiskSizeWithoutHeader;
    this.uncompressedSizeWithoutHeader = uncompressedSizeWithoutHeader;
    this.prevBlockOffset = prevBlockOffset;
    this.buf = buf;
    if (fillHeader)
      overwriteHeader();
    this.offset = offset;
  }

  /**
   * Creates a block from an existing buffer starting with a header. Rewinds
   * and takes ownership of the buffer. By definition of rewind, ignores the
   * buffer position, but if you slice the buffer beforehand, it will rewind
   * to that point.
   */
  private HFileBlock(ByteBuffer b) throws IOException {
    b.rewind();
    blockType = BlockType.read(b);
    onDiskSizeWithoutHeader = b.getInt();
    uncompressedSizeWithoutHeader = b.getInt();
    prevBlockOffset = b.getLong();
    buf = b;
    buf.rewind();
  }

  public BlockType getBlockType() {
    return blockType;
  }

  /**
   * @return the on-disk size of the block with header size included
   */
  public int getOnDiskSizeWithHeader() {
    return onDiskSizeWithoutHeader + HEADER_SIZE;
  }

  /**
   * Returns the size of the compressed part of the block in case compression
   * is used, or the uncompressed size of the data part otherwise. Header size
   * is not included.
   *
   * @return the on-disk size of the data part of the block, header not
   *         included
   */
  public int getOnDiskSizeWithoutHeader() {
    return onDiskSizeWithoutHeader;
  }

  /**
   * @return the uncompressed size of the data part of the block, header not
   *         included
   */
  public int getUncompressedSizeWithoutHeader() {
    return uncompressedSizeWithoutHeader;
  }

  /**
   * @return the offset of the previous block of the same type in the file, or
   *         -1 if unknown
   */
  public long getPrevBlockOffset() {
    return prevBlockOffset;
  }

  /**
   * Writes header fields into the first {@link HEADER_SIZE} bytes of the
   * buffer. Resets the buffer position to the end of header as side effect.
   */
  private void overwriteHeader() {
    buf.rewind();
    blockType.write(buf);
    buf.putInt(onDiskSizeWithoutHeader);
    buf.putInt(uncompressedSizeWithoutHeader);
    buf.putLong(prevBlockOffset);
  }

  /**
   * Returns a buffer that does not include the header. The array offset points
   * to the start of the block data right after the header. The underlying data
   * array is not copied.
   *
   * @return the buffer with header skipped
   */
  public ByteBuffer getBufferWithoutHeader() {
    return ByteBuffer.wrap(buf.array(), buf.arrayOffset() + HEADER_SIZE,
        buf.limit() - HEADER_SIZE).slice();
  }

  /**
   * Returns the buffer this block stores internally. The clients must not
   * modify the buffer object. This method has to be public because it is
   * used in {@link CompoundBloomFilter} to avoid object creation on every
   * Bloom filter lookup, but has to be used with caution.
   *
   * @return the buffer of this block for read-only operations
   */
  public ByteBuffer getBufferReadOnly() {
    return buf;
  }

  /**
   * Returns a byte buffer of this block, including header data, positioned at
   * the beginning of header. The underlying data array is not copied.
   *
   * @return the byte buffer with header included
   */
  public ByteBuffer getBufferWithHeader() {
    ByteBuffer dupBuf = buf.duplicate();
    dupBuf.rewind();
    return dupBuf;
  }

  /**
   * Deserializes fields of the given writable using the data portion of this
   * block. Does not check that all the block data has been read.
   */
  public void readInto(Writable w) throws IOException {
    Preconditions.checkNotNull(w);

    if (Writables.getWritable(buf.array(), buf.arrayOffset() + HEADER_SIZE,
        buf.limit() - HEADER_SIZE, w) == null) {
      throw new IOException("Failed to deserialize block " + this + " into a "
          + w.getClass().getSimpleName());
    }
  }

  private void sanityCheckAssertion(long valueFromBuf, long valueFromField,
      String fieldName) throws IOException {
    if (valueFromBuf != valueFromField) {
      throw new AssertionError(fieldName + " in the buffer (" + valueFromBuf
          + ") is different from that in the field (" + valueFromField + ")");
    }
  }

  /**
   * Checks if the block is internally consistent, i.e. the first
   * {@link #HEADER_SIZE} bytes of the buffer contain a valid header consistent
   * with the fields. This function is primary for testing and debugging, and
   * is not thread-safe, because it alters the internal buffer pointer.
   */
  void sanityCheck() throws IOException {
    buf.rewind();

    {
      BlockType blockTypeFromBuf = BlockType.read(buf);
      if (blockTypeFromBuf != blockType) {
        throw new IOException("Block type stored in the buffer: " +
            blockTypeFromBuf + ", block type field: " + blockType);
      }
    }

    sanityCheckAssertion(buf.getInt(), onDiskSizeWithoutHeader,
        "onDiskSizeWithoutHeader");

    sanityCheckAssertion(buf.getInt(), uncompressedSizeWithoutHeader,
        "uncompressedSizeWithoutHeader");

    sanityCheckAssertion(buf.getLong(), prevBlockOffset, "prevBlocKOffset");

    int expectedBufLimit = uncompressedSizeWithoutHeader + HEADER_SIZE;
    if (buf.limit() != expectedBufLimit) {
      throw new AssertionError("Expected buffer limit " + expectedBufLimit
          + ", got " + buf.limit());
    }

    // We might optionally allocate HEADER_SIZE more bytes to read the next
    // block's, header, so there are two sensible values for buffer capacity.
    if (buf.capacity() != uncompressedSizeWithoutHeader + HEADER_SIZE &&
        buf.capacity() != uncompressedSizeWithoutHeader + 2 * HEADER_SIZE) {
      throw new AssertionError("Invalid buffer capacity: " + buf.capacity() +
          ", expected " + (uncompressedSizeWithoutHeader + HEADER_SIZE) +
          " or " + (uncompressedSizeWithoutHeader + 2 * HEADER_SIZE));
    }
  }

  @Override
  public String toString() {
    return "blockType="
        + blockType
        + ", onDiskSizeWithoutHeader="
        + onDiskSizeWithoutHeader
        + ", uncompressedSizeWithoutHeader="
        + uncompressedSizeWithoutHeader
        + ", prevBlockOffset="
        + prevBlockOffset
        + ", dataBeginsWith="
        + Bytes.toStringBinary(buf.array(), buf.arrayOffset() + HEADER_SIZE,
            Math.min(32, buf.limit() - buf.arrayOffset() - HEADER_SIZE))
        + ", fileOffset=" + offset;
  }

  private void validateOnDiskSizeWithoutHeader(
      int expectedOnDiskSizeWithoutHeader) throws IOException {
    if (onDiskSizeWithoutHeader != expectedOnDiskSizeWithoutHeader) {
      String blockInfoMsg =
        "Block offset: " + offset + ", data starts with: "
          + Bytes.toStringBinary(buf.array(), buf.arrayOffset(),
              buf.arrayOffset() + Math.min(32, buf.limit()));
      throw new IOException("On-disk size without header provided is "
          + expectedOnDiskSizeWithoutHeader + ", but block "
          + "header contains " + onDiskSizeWithoutHeader + ". " +
          blockInfoMsg);
    }
  }

  /**
   * Always allocates a new buffer of the correct size. Copies header bytes
   * from the existing buffer. Does not change header fields.
   *
   * @param extraBytes whether to reserve room in the buffer to read the next
   *          block's header
   */
  private void allocateBuffer(boolean extraBytes) {
    int capacityNeeded = HEADER_SIZE + uncompressedSizeWithoutHeader +
        (extraBytes ? HEADER_SIZE : 0);

    ByteBuffer newBuf = ByteBuffer.allocate(capacityNeeded);

    // Copy header bytes.
    System.arraycopy(buf.array(), buf.arrayOffset(), newBuf.array(),
        newBuf.arrayOffset(), HEADER_SIZE);

    buf = newBuf;
    buf.limit(HEADER_SIZE + uncompressedSizeWithoutHeader);
  }

  /** An additional sanity-check in case no compression is being used. */
  public void assumeUncompressed() throws IOException {
    if (onDiskSizeWithoutHeader != uncompressedSizeWithoutHeader) {
      throw new IOException("Using no compression but "
          + "onDiskSizeWithoutHeader=" + onDiskSizeWithoutHeader + ", "
          + "uncompressedSizeWithoutHeader=" + uncompressedSizeWithoutHeader);
    }
  }

  /**
   * @param expectedType the expected type of this block
   * @throws IOException if this block's type is different than expected
   */
  public void expectType(BlockType expectedType) throws IOException {
    if (blockType != expectedType) {
      throw new IOException("Invalid block type: expected=" + expectedType
          + ", actual=" + blockType);
    }
  }

  /** @return the offset of this block in the file it was read from */
  public long getOffset() {
    if (offset < 0) {
      throw new IllegalStateException(
          "HFile block offset not initialized properly");
    }
    return offset;
  }

  /**
   * @return a byte stream reading the data section of this block
   */
  public DataInputStream getByteStream() {
    return new DataInputStream(new ByteArrayInputStream(buf.array(),
        buf.arrayOffset() + HEADER_SIZE, buf.limit() - HEADER_SIZE));
  }

  @Override
  public long heapSize() {
    // This object, block type and byte buffer reference, on-disk and
    // uncompressed size, next block's on-disk size, offset and previous
    // offset, byte buffer object, and its byte array. Might also need to add
    // some fields inside the byte buffer.

    // We only add one BYTE_BUFFER_HEAP_SIZE because at any given moment, one of
    // the bytebuffers will be null. But we do account for both references.

    // If we are on heap, then we add the capacity of buf.
    if (buf != null) {
      return ClassSize.align(ClassSize.OBJECT + 2 * ClassSize.REFERENCE + 3
          * Bytes.SIZEOF_INT + 2 * Bytes.SIZEOF_LONG + BYTE_BUFFER_HEAP_SIZE)
          + ClassSize.align(buf.capacity());
    } else {

      return ClassSize.align(ClassSize.OBJECT + 2 * ClassSize.REFERENCE + 3
          * Bytes.SIZEOF_INT + 2 * Bytes.SIZEOF_LONG + BYTE_BUFFER_HEAP_SIZE);
    }
  }

  /**
   * Read from an input stream. Analogous to
   * {@link IOUtils#readFully(InputStream, byte[], int, int)}, but specifies a
   * number of "extra" bytes that would be desirable but not absolutely
   * necessary to read.
   *
   * @param in the input stream to read from
   * @param buf the buffer to read into
   * @param bufOffset the destination offset in the buffer
   * @param necessaryLen the number of bytes that are absolutely necessary to
   *          read
   * @param extraLen the number of extra bytes that would be nice to read
   * @return true if succeeded reading the extra bytes
   * @throws IOException if failed to read the necessary bytes
   */
  public static boolean readWithExtra(InputStream in, byte buf[],
      int bufOffset, int necessaryLen, int extraLen) throws IOException {
    int bytesRemaining = necessaryLen + extraLen;
    while (bytesRemaining > 0) {
      int ret = in.read(buf, bufOffset, bytesRemaining);
      if (ret == -1 && bytesRemaining <= extraLen) {
        // We could not read the "extra data", but that is OK.
        break;
      }

      if (ret < 0) {
        throw new IOException("Premature EOF from inputStream (read "
            + "returned " + ret + ", was trying to read " + necessaryLen
            + " necessary bytes and " + extraLen + " extra bytes, "
            + "successfully read "
            + (necessaryLen + extraLen - bytesRemaining));
      }
      bufOffset += ret;
      bytesRemaining -= ret;
    }
    return bytesRemaining <= 0;
  }

  /**
   * @return the on-disk size of the next block (including the header size)
   *         that was read by peeking into the next block's header
   */
  public int getNextBlockOnDiskSizeWithHeader() {
    return nextBlockOnDiskSizeWithHeader;
  }


  /**
   * Unified version 2 {@link HFile} block writer. The intended usage pattern
   * is as follows:
   * <ul>
   * <li>Construct an {@link HFileBlock.Writer}, providing a compression
   * algorithm
   * <li>Call {@link Writer#startWriting(BlockType, boolean)} and get a data stream to
   * write to
   * <li>Write your data into the stream
   * <li>Call {@link Writer#writeHeaderAndData(FSDataOutputStream)} as many times as you need to
   * store the serialized block into an external stream, or call
   * {@link Writer#getHeaderAndData()} to get it as a byte array.
   * <li>Repeat to write more blocks
   * </ul>
   * <p>
   */
  public static class Writer {

    private enum State {
      INIT,
      WRITING,
      BLOCK_READY
    };

    /** Writer state. Used to ensure the correct usage protocol. */
    private State state = State.INIT;

    /** Compression algorithm for all blocks this instance writes. */
    private final Compression.Algorithm compressAlgo;

    /**
     * The stream we use to accumulate data in the on-disk format for each
     * block (i.e. compressed data, or uncompressed if using no compression).
     * We reset this stream at the end of each block and reuse it. The header
     * is written as the first {@link #HEADER_SIZE} bytes into this stream.
     */
    private ByteArrayOutputStream baosOnDisk;

    /**
     * The stream we use to accumulate uncompressed block data for
     * cache-on-write. Null when cache-on-write is turned off.
     */
    private ByteArrayOutputStream baosInMemory;

    /** Compressor, which is also reused between consecutive blocks. */
    private Compressor compressor;

    /** Current block type. Set in {@link #startWriting(BlockType)}. */
    private BlockType blockType;

    /**
     * A stream that we write uncompressed bytes to, which compresses them and
     * writes them to {@link #baosOnDisk}.
     */
    private DataOutputStream userDataStream;

    /**
     * Bytes to be written to the file system, including the header. Compressed
     * if compression is turned on.
     */
    private byte[] onDiskBytesWithHeader;

    /**
     * The total number of uncompressed bytes written into the current block,
     * with header size not included. Valid in the READY state.
     */
    private int uncompressedSizeWithoutHeader;

    /**
     * Only used when we are using cache-on-write. Valid in the READY state.
     * Contains the header and the uncompressed bytes, so the length is
     * {@link #uncompressedSizeWithoutHeader} + {@link HFileBlock#HEADER_SIZE}.
     */
    private byte[] uncompressedBytesWithHeader;

    /**
     * Current block's start offset in the {@link HFile}. Set in
     * {@link #writeHeaderAndData(FSDataOutputStream)}.
     */
    private long startOffset;

    /**
     * Offset of previous block by block type. Updated when the next block is
     * started.
     */
    private long[] prevOffsetByType;

    /**
     * Whether we are accumulating uncompressed bytes for the purpose of
     * caching on write.
     */
    private boolean cacheOnWrite;

    /** The offset of the previous block of the same type */
    private long prevOffset;

    /**
     * @param compressionAlgorithm
     *          compression algorithm to use
     */
    public Writer(Compression.Algorithm compressionAlgorithm) {
      compressAlgo = compressionAlgorithm == null ? NONE
          : compressionAlgorithm;

      baosOnDisk = new ByteArrayOutputStream();
      if (compressAlgo != NONE)
        compressor = compressionAlgorithm.getCompressor();

      prevOffsetByType = new long[BlockType.values().length];
      for (int i = 0; i < prevOffsetByType.length; ++i)
        prevOffsetByType[i] = -1;
    }

    /**
     * Starts writing into the block. The previous block's data is discarded.
     *
     * @return the stream the user can write their data into
     * @throws IOException
     */
    public DataOutputStream startWriting(BlockType newBlockType,
        boolean cacheOnWrite) throws IOException {
      if (state == State.BLOCK_READY && startOffset != -1) {
        // We had a previous block that was written to a stream at a specific
        // offset. Save that offset as the last offset of a block of that type.
        prevOffsetByType[blockType.ordinal()] = startOffset;
      }

      this.cacheOnWrite = cacheOnWrite;

      startOffset = -1;
      blockType = newBlockType;

      baosOnDisk.reset();
      baosOnDisk.write(DUMMY_HEADER);

      state = State.WRITING;
      if (compressAlgo == NONE) {
        // We do not need a compression stream or a second uncompressed stream
        // for cache-on-write.
        userDataStream = new DataOutputStream(baosOnDisk);
      } else {
        OutputStream compressingOutputStream =
          compressAlgo.createCompressionStream(baosOnDisk, compressor, 0);

        if (cacheOnWrite) {
          // We save uncompressed data in a cache-on-write mode.
          if (baosInMemory == null)
            baosInMemory = new ByteArrayOutputStream();
          baosInMemory.reset();
          baosInMemory.write(DUMMY_HEADER);
          userDataStream = new DataOutputStream(new DoubleOutputStream(
              compressingOutputStream, baosInMemory));
        } else {
          userDataStream = new DataOutputStream(compressingOutputStream);
        }
      }

      return userDataStream;
    }

    /**
     * Returns the stream for the user to write to. The block writer takes care
     * of handling compression and buffering for caching on write. Can only be
     * called in the "writing" state.
     *
     * @return the data output stream for the user to write to
     */
    DataOutputStream getUserDataStream() {
      expectState(State.WRITING);
      return userDataStream;
    }

    /**
     * Transitions the block writer from the "writing" state to the "block
     * ready" state.  Does nothing if a block is already finished.
     */
    private void ensureBlockReady() throws IOException {
      Preconditions.checkState(state != State.INIT,
          "Unexpected state: " + state);

      if (state == State.BLOCK_READY)
        return;

      finishBlock();
      state = State.BLOCK_READY;
    }

    /**
     * An internal method that flushes the compressing stream (if using
     * compression), serializes the header, and takes care of the separate
     * uncompressed stream for caching on write, if applicable. Block writer
     * state transitions must be managed by the caller.
     */
    private void finishBlock() throws IOException {
      userDataStream.flush();
      uncompressedSizeWithoutHeader = userDataStream.size();

      onDiskBytesWithHeader = baosOnDisk.toByteArray();
      prevOffset = prevOffsetByType[blockType.ordinal()];
      putHeader(onDiskBytesWithHeader, 0);

      if (cacheOnWrite && compressAlgo != NONE) {
        uncompressedBytesWithHeader = baosInMemory.toByteArray();

        if (uncompressedSizeWithoutHeader !=
            uncompressedBytesWithHeader.length - HEADER_SIZE) {
          throw new IOException("Uncompressed size mismatch: "
              + uncompressedSizeWithoutHeader + " vs. "
              + (uncompressedBytesWithHeader.length - HEADER_SIZE));
        }

        // Write the header into the beginning of the uncompressed byte array.
        putHeader(uncompressedBytesWithHeader, 0);
      }
    }

    /** Put the header into the given byte array at the given offset. */
    private void putHeader(byte[] dest, int offset) {
      offset = blockType.put(dest, offset);
      offset = Bytes.putInt(dest, offset, onDiskBytesWithHeader.length
          - HEADER_SIZE);
      offset = Bytes.putInt(dest, offset, uncompressedSizeWithoutHeader);
      Bytes.putLong(dest, offset, prevOffset);
    }

    /**
     * Similar to {@link #writeHeaderAndData(FSDataOutputStream)}, but records
     * the offset of this block so that it can be referenced in the next block
     * of the same type.
     *
     * @param out
     * @throws IOException
     */
    public void writeHeaderAndData(FSDataOutputStream out) throws IOException {
      long offset = out.getPos();
      if (startOffset != -1 && offset != startOffset) {
        throw new IOException("A " + blockType + " block written to a "
            + "stream twice, first at offset " + startOffset + ", then at "
            + offset);
      }
      startOffset = offset;

      writeHeaderAndData((DataOutputStream) out);
    }

    /**
     * Writes the header and the compressed data of this block (or uncompressed
     * data when not using compression) into the given stream. Can be called in
     * the "writing" state or in the "block ready" state. If called in the
     * "writing" state, transitions the writer to the "block ready" state.
     *
     * @param out the output stream to write the
     * @throws IOException
     */
    private void writeHeaderAndData(DataOutputStream out) throws IOException {
      ensureBlockReady();
      out.write(onDiskBytesWithHeader);
    }

    /**
     * Returns the header or the compressed data (or uncompressed data when not
     * using compression) as a byte array. Can be called in the "writing" state
     * or in the "block ready" state. If called in the "writing" state,
     * transitions the writer to the "block ready" state.
     *
     * @return header and data as they would be stored on disk in a byte array
     * @throws IOException
     */
    public byte[] getHeaderAndData() throws IOException {
      ensureBlockReady();
      return onDiskBytesWithHeader;
    }

    /**
     * Releases the compressor this writer uses to compress blocks into the
     * compressor pool. Needs to be called before the writer is discarded.
     */
    public void releaseCompressor() {
      if (compressor != null) {
        compressAlgo.returnCompressor(compressor);
        compressor = null;
      }
    }

    /**
     * Returns the on-disk size of the data portion of the block. This is the
     * compressed size if compression is enabled. Can only be called in the
     * "block ready" state. Header is not compressed, and its size is not
     * included in the return value.
     *
     * @return the on-disk size of the block, not including the header.
     */
    public int getOnDiskSizeWithoutHeader() {
      expectState(State.BLOCK_READY);
      return onDiskBytesWithHeader.length - HEADER_SIZE;
    }

    /**
     * Returns the on-disk size of the block. Can only be called in the
     * "block ready" state.
     *
     * @return the on-disk size of the block ready to be written, including the
     *         header size
     */
    public int getOnDiskSizeWithHeader() {
      expectState(State.BLOCK_READY);
      return onDiskBytesWithHeader.length;
    }

    /**
     * The uncompressed size of the block data. Does not include header size.
     */
    public int getUncompressedSizeWithoutHeader() {
      expectState(State.BLOCK_READY);
      return uncompressedSizeWithoutHeader;
    }

    /**
     * The uncompressed size of the block data, including header size.
     */
    public int getUncompressedSizeWithHeader() {
      expectState(State.BLOCK_READY);
      return uncompressedSizeWithoutHeader + HEADER_SIZE;
    }

    /** @return true if a block is being written  */
    public boolean isWriting() {
      return state == State.WRITING;
    }

    /**
     * Returns the number of bytes written into the current block so far, or
     * zero if not writing the block at the moment. Note that this will return
     * zero in the "block ready" state as well.
     *
     * @return the number of bytes written
     */
    public int blockSizeWritten() {
      if (state != State.WRITING)
        return 0;
      return userDataStream.size();
    }

    /**
     * Returns the header followed by the uncompressed data, even if using
     * compression. This is needed for storing uncompressed blocks in the block
     * cache. Can be called in the "writing" state or the "block ready" state.
     *
     * @return uncompressed block bytes for caching on write
     */
    private byte[] getUncompressedDataWithHeader() {
      expectState(State.BLOCK_READY);

      if (compressAlgo == NONE)
        return onDiskBytesWithHeader;

      if (!cacheOnWrite)
        throw new IllegalStateException("Cache-on-write is turned off");

      if (uncompressedBytesWithHeader == null)
        throw new NullPointerException();

      return uncompressedBytesWithHeader;
    }

    private void expectState(State expectedState) {
      if (state != expectedState) {
        throw new IllegalStateException("Expected state: " + expectedState +
            ", actual state: " + state);
      }
    }

    /**
     * Similar to {@link #getUncompressedBufferWithHeader()} but returns a byte
     * buffer.
     *
     * @return uncompressed block for caching on write in the form of a buffer
     */
    public ByteBuffer getUncompressedBufferWithHeader() {
      byte[] b = getUncompressedDataWithHeader();
      return ByteBuffer.wrap(b, 0, b.length);
    }

    /**
     * Takes the given {@link BlockWritable} instance, creates a new block of
     * its appropriate type, writes the writable into this block, and flushes
     * the block into the output stream. The writer is instructed not to buffer
     * uncompressed bytes for cache-on-write.
     *
     * @param bw the block-writable object to write as a block
     * @param out the file system output stream
     * @throws IOException
     */
    public void writeBlock(BlockWritable bw, FSDataOutputStream out)
        throws IOException {
      bw.writeToBlock(startWriting(bw.getBlockType(), false));
      writeHeaderAndData(out);
    }

    public HFileBlock getBlockForCaching() {
      return new HFileBlock(blockType, onDiskBytesWithHeader.length
          - HEADER_SIZE, uncompressedSizeWithoutHeader, prevOffset,
          getUncompressedBufferWithHeader(), false, startOffset);
    }

  }

  /** Something that can be written into a block. */
  public interface BlockWritable {

    /** The type of block this data should use. */
    BlockType getBlockType();

    /**
     * Writes the block to the provided stream. Must not write any magic
     * records.
     *
     * @param out a stream to write uncompressed data into
     */
    void writeToBlock(DataOutput out) throws IOException;
  }

  // Block readers and writers

  /** An interface allowing to iterate {@link HFileBlock}s. */
  public interface BlockIterator {

    /**
     * Get the next block, or null if there are no more blocks to iterate.
     */
    HFileBlock nextBlock() throws IOException;

    /**
     * Similar to {@link #nextBlock()} but checks block type, throws an
     * exception if incorrect, and returns the data portion of the block as
     * an input stream.
     */
    DataInputStream nextBlockAsStream(BlockType blockType) throws IOException;
  }

  /** A full-fledged reader with iteration ability. */
  public interface FSReader {

    /**
     * Reads the block at the given offset in the file with the given on-disk
     * size and uncompressed size.
     *
     * @param offset
     * @param onDiskSize the on-disk size of the entire block, including all
     *          applicable headers, or -1 if unknown
     * @param uncompressedSize the uncompressed size of the compressed part of
     *          the block, or -1 if unknown
     * @return the newly read block
     */
    HFileBlock readBlockData(long offset, long onDiskSize,
        int uncompressedSize, boolean pread) throws IOException;

    /**
     * Creates a block iterator over the given portion of the {@link HFile}.
     * The iterator returns blocks starting with offset such that offset <=
     * startOffset < endOffset.
     *
     * @param startOffset the offset of the block to start iteration with
     * @param endOffset the offset to end iteration at (exclusive)
     * @return an iterator of blocks between the two given offsets
     */
    BlockIterator blockRange(long startOffset, long endOffset);
  }

  /**
   * A common implementation of some methods of {@link FSReader} and some
   * tools for implementing HFile format version-specific block readers.
   */
  public abstract static class AbstractFSReader implements FSReader {

    /** The file system stream of the underlying {@link HFile} */
    protected FSDataInputStream istream;

    /** Compression algorithm used by the {@link HFile} */
    protected Compression.Algorithm compressAlgo;

    /** The size of the file we are reading from, or -1 if unknown. */
    protected long fileSize;

    /** The default buffer size for our buffered streams */
    public static final int DEFAULT_BUFFER_SIZE = 1 << 20;

    public AbstractFSReader(FSDataInputStream istream, Algorithm compressAlgo,
        long fileSize) {
      this.istream = istream;
      this.compressAlgo = compressAlgo;
      this.fileSize = fileSize;
    }

    @Override
    public BlockIterator blockRange(final long startOffset,
        final long endOffset) {
      return new BlockIterator() {
        private long offset = startOffset;

        @Override
        public HFileBlock nextBlock() throws IOException {
          if (offset >= endOffset)
            return null;
          HFileBlock b = readBlockData(offset, -1, -1, false);
          offset += b.getOnDiskSizeWithHeader();
          return b;
        }

        @Override
        public DataInputStream nextBlockAsStream(BlockType blockType)
            throws IOException {
          HFileBlock blk = nextBlock();
          if (blk.getBlockType() != blockType) {
            throw new IOException("Expected block of type " + blockType
                + " but found " + blk.getBlockType());
          }
          return blk.getByteStream();
        }
      };
    }

    /**
     * Does a positional read or a seek and read into the given buffer. Returns
     * the on-disk size of the next block, or -1 if it could not be determined.
     *
     * @param dest destination buffer
     * @param destOffset offset in the destination buffer
     * @param size size of the block to be read
     * @param peekIntoNextBlock whether to read the next block's on-disk size
     * @param fileOffset position in the stream to read at
     * @param pread whether we should do a positional read
     * @return the on-disk size of the next block with header size included, or
     *         -1 if it could not be determined
     * @throws IOException
     */
    protected int readAtOffset(byte[] dest, int destOffset, int size,
        boolean peekIntoNextBlock, long fileOffset, boolean pread)
        throws IOException {
      if (peekIntoNextBlock &&
          destOffset + size + HEADER_SIZE > dest.length) {
        // We are asked to read the next block's header as well, but there is
        // not enough room in the array.
        throw new IOException("Attempted to read " + size + " bytes and " +
            HEADER_SIZE + " bytes of next header into a " + dest.length +
            "-byte array at offset " + destOffset);
      }

      if (pread) {
        // Positional read. Better for random reads.
        int extraSize = peekIntoNextBlock ? HEADER_SIZE : 0;

        int ret = istream.read(fileOffset, dest, destOffset, size + extraSize);
        if (ret < size) {
          throw new IOException("Positional read of " + size + " bytes " +
              "failed at offset " + fileOffset + " (returned " + ret + ")");
        }

        if (ret == size || ret < size + extraSize) {
          // Could not read the next block's header, or did not try.
          return -1;
        }
      } else {
        // Seek + read. Better for scanning.
        synchronized (istream) {
          istream.seek(fileOffset);

          long realOffset = istream.getPos();
          if (realOffset != fileOffset) {
            throw new IOException("Tried to seek to " + fileOffset + " to "
                + "read " + size + " bytes, but pos=" + realOffset
                + " after seek");
          }

          if (!peekIntoNextBlock) {
            IOUtils.readFully(istream, dest, destOffset, size);
            return -1;
          }

          // Try to read the next block header.
          if (!readWithExtra(istream, dest, destOffset, size, HEADER_SIZE))
            return -1;
        }
      }

      assert peekIntoNextBlock;
      return Bytes.toInt(dest, destOffset + size + BlockType.MAGIC_LENGTH) +
          HEADER_SIZE;
    }

    /**
     * Decompresses data from the given stream using the configured compression
     * algorithm.
     * @param dest
     * @param destOffset
     * @param bufferedBoundedStream
     *          a stream to read compressed data from, bounded to the exact
     *          amount of compressed data
     * @param compressedSize
     *          compressed data size, header not included
     * @param uncompressedSize
     *          uncompressed data size, header not included
     * @throws IOException
     */
    protected void decompress(byte[] dest, int destOffset,
        InputStream bufferedBoundedStream, int compressedSize,
        int uncompressedSize) throws IOException {
      Decompressor decompressor = null;
      try {
        decompressor = compressAlgo.getDecompressor();
        InputStream is = compressAlgo.createDecompressionStream(
            bufferedBoundedStream, decompressor, 0);

        IOUtils.readFully(is, dest, destOffset, uncompressedSize);
        is.close();
      } finally {
        if (decompressor != null) {
          compressAlgo.returnDecompressor(decompressor);
        }
      }
    }

    /**
     * Creates a buffered stream reading a certain slice of the file system
     * input stream. We need this because the decompression we use seems to
     * expect the input stream to be bounded.
     *
     * @param offset the starting file offset the bounded stream reads from
     * @param size the size of the segment of the file the stream should read
     * @param pread whether to use position reads
     * @return a stream restricted to the given portion of the file
     */
    protected InputStream createBufferedBoundedStream(long offset,
        int size, boolean pread) {
      return new BufferedInputStream(new BoundedRangeFileInputStream(istream,
          offset, size, pread), Math.min(DEFAULT_BUFFER_SIZE, size));
    }

  }

  /**
   * Reads version 1 blocks from the file system. In version 1 blocks,
   * everything is compressed, including the magic record, if compression is
   * enabled. Everything might be uncompressed if no compression is used. This
   * reader returns blocks represented in the uniform version 2 format in
   * memory.
   */
  public static class FSReaderV1 extends AbstractFSReader {

    /** Header size difference between version 1 and 2 */
    private static final int HEADER_DELTA = HEADER_SIZE - MAGIC_LENGTH;

    public FSReaderV1(FSDataInputStream istream, Algorithm compressAlgo,
        long fileSize) {
      super(istream, compressAlgo, fileSize);
    }

    /**
     * Read a version 1 block. There is no uncompressed header, and the block
     * type (the magic record) is part of the compressed data. This
     * implementation assumes that the bounded range file input stream is
     * needed to stop the decompressor reading into next block, because the
     * decompressor just grabs a bunch of data without regard to whether it is
     * coming to end of the compressed section.
     *
     * The block returned is still a version 2 block, and in particular, its
     * first {@link #HEADER_SIZE} bytes contain a valid version 2 header.
     *
     * @param offset the offset of the block to read in the file
     * @param onDiskSizeWithMagic the on-disk size of the version 1 block,
     *          including the magic record, which is the part of compressed
     *          data if using compression
     * @param uncompressedSizeWithMagic uncompressed size of the version 1
     *          block, including the magic record
     */
    @Override
    public HFileBlock readBlockData(long offset, long onDiskSizeWithMagic,
        int uncompressedSizeWithMagic, boolean pread) throws IOException {
      if (uncompressedSizeWithMagic <= 0) {
        throw new IOException("Invalid uncompressedSize="
            + uncompressedSizeWithMagic + " for a version 1 block");
      }

      if (onDiskSizeWithMagic <= 0 || onDiskSizeWithMagic >= Integer.MAX_VALUE)
      {
        throw new IOException("Invalid onDiskSize=" + onDiskSizeWithMagic
            + " (maximum allowed: " + Integer.MAX_VALUE + ")");
      }

      int onDiskSize = (int) onDiskSizeWithMagic;

      if (uncompressedSizeWithMagic < MAGIC_LENGTH) {
        throw new IOException("Uncompressed size for a version 1 block is "
            + uncompressedSizeWithMagic + " but must be at least "
            + MAGIC_LENGTH);
      }

      // The existing size already includes magic size, and we are inserting
      // a version 2 header.
      ByteBuffer buf = ByteBuffer.allocate(uncompressedSizeWithMagic
          + HEADER_DELTA);

      int onDiskSizeWithoutHeader;
      if (compressAlgo == Compression.Algorithm.NONE) {
        // A special case when there is no compression.
        if (onDiskSize != uncompressedSizeWithMagic) {
          throw new IOException("onDiskSize=" + onDiskSize
              + " and uncompressedSize=" + uncompressedSizeWithMagic
              + " must be equal for version 1 with no compression");
        }

        // The first MAGIC_LENGTH bytes of what this will read will be
        // overwritten.
        readAtOffset(buf.array(), buf.arrayOffset() + HEADER_DELTA,
            onDiskSize, false, offset, pread);

        onDiskSizeWithoutHeader = uncompressedSizeWithMagic - MAGIC_LENGTH;
      } else {
        InputStream bufferedBoundedStream = createBufferedBoundedStream(
            offset, onDiskSize, pread);
        decompress(buf.array(), buf.arrayOffset() + HEADER_DELTA,
            bufferedBoundedStream, onDiskSize, uncompressedSizeWithMagic);

        // We don't really have a good way to exclude the "magic record" size
        // from the compressed block's size, since it is compressed as well.
        onDiskSizeWithoutHeader = onDiskSize;
      }

      BlockType newBlockType = BlockType.parse(buf.array(), buf.arrayOffset()
          + HEADER_DELTA, MAGIC_LENGTH);

      // We set the uncompressed size of the new HFile block we are creating
      // to the size of the data portion of the block without the magic record,
      // since the magic record gets moved to the header.
      HFileBlock b = new HFileBlock(newBlockType, onDiskSizeWithoutHeader,
          uncompressedSizeWithMagic - MAGIC_LENGTH, -1L, buf, true, offset);
      return b;
    }
  }

  /**
   * We always prefetch the header of the next block, so that we know its
   * on-disk size in advance and can read it in one operation.
   */
  private static class PrefetchedHeader {
    long offset = -1;
    byte[] header = new byte[HEADER_SIZE];
    ByteBuffer buf = ByteBuffer.wrap(header, 0, HEADER_SIZE);
  }

  /** Reads version 2 blocks from the filesystem. */
  public static class FSReaderV2 extends AbstractFSReader {

    private ThreadLocal<PrefetchedHeader> prefetchedHeaderForThread =
        new ThreadLocal<PrefetchedHeader>() {
          @Override
          public PrefetchedHeader initialValue() {
            return new PrefetchedHeader();
          }
        };

    public FSReaderV2(FSDataInputStream istream, Algorithm compressAlgo,
        long fileSize) {
      super(istream, compressAlgo, fileSize);
    }

    /**
     * Reads a version 2 block. Tries to do as little memory allocation as
     * possible, using the provided on-disk size.
     *
     * @param offset the offset in the stream to read at
     * @param onDiskSizeWithHeaderL the on-disk size of the block, including
     *          the header, or -1 if unknown
     * @param uncompressedSize the uncompressed size of the the block. Always
     *          expected to be -1. This parameter is only used in version 1.
     * @param pread whether to use a positional read
     */
    @Override
    public HFileBlock readBlockData(long offset, long onDiskSizeWithHeaderL,
        int uncompressedSize, boolean pread) throws IOException {
      if (offset < 0) {
        throw new IOException("Invalid offset=" + offset + " trying to read "
            + "block (onDiskSize=" + onDiskSizeWithHeaderL
            + ", uncompressedSize=" + uncompressedSize + ")");
      }
      if (uncompressedSize != -1) {
        throw new IOException("Version 2 block reader API does not need " +
            "the uncompressed size parameter");
      }

      if ((onDiskSizeWithHeaderL < HEADER_SIZE && onDiskSizeWithHeaderL != -1)
          || onDiskSizeWithHeaderL >= Integer.MAX_VALUE) {
        throw new IOException("Invalid onDisksize=" + onDiskSizeWithHeaderL
            + ": expected to be at least " + HEADER_SIZE
            + " and at most " + Integer.MAX_VALUE + ", or -1 (offset="
            + offset + ", uncompressedSize=" + uncompressedSize + ")");
      }

      int onDiskSizeWithHeader = (int) onDiskSizeWithHeaderL;

      HFileBlock b;
      if (onDiskSizeWithHeader > 0) {
        // We know the total on-disk size but not the uncompressed size. Read
        // the entire block into memory, then parse the header and decompress
        // from memory if using compression. This code path is used when
        // doing a random read operation relying on the block index, as well as
        // when the client knows the on-disk size from peeking into the next
        // block's header (e.g. this block's header) when reading the previous
        // block. This is the faster and more preferable case.

        int onDiskSizeWithoutHeader = onDiskSizeWithHeader - HEADER_SIZE;
        assert onDiskSizeWithoutHeader >= 0;

        // See if we can avoid reading the header. This is desirable, because
        // we will not incur a seek operation to seek back if we have already
        // read this block's header as part of the previous read's look-ahead.
        PrefetchedHeader prefetchedHeader = prefetchedHeaderForThread.get();
        byte[] header = prefetchedHeader.offset == offset
            ? prefetchedHeader.header : null;

        // Size that we have to skip in case we have already read the header.
        int preReadHeaderSize = header == null ? 0 : HEADER_SIZE;

        if (compressAlgo == Compression.Algorithm.NONE) {
          // Just read the whole thing. Allocate enough space to read the
          // next block's header too.

          ByteBuffer headerAndData = ByteBuffer.allocate(onDiskSizeWithHeader
              + HEADER_SIZE);
          headerAndData.limit(onDiskSizeWithHeader);

          if (header != null) {
            System.arraycopy(header, 0, headerAndData.array(), 0,
                HEADER_SIZE);
          }

          int nextBlockOnDiskSizeWithHeader = readAtOffset(
              headerAndData.array(), headerAndData.arrayOffset()
                  + preReadHeaderSize, onDiskSizeWithHeader
                  - preReadHeaderSize, true, offset + preReadHeaderSize,
                  pread);

          b = new HFileBlock(headerAndData);
          b.assumeUncompressed();
          b.validateOnDiskSizeWithoutHeader(onDiskSizeWithoutHeader);
          b.nextBlockOnDiskSizeWithHeader = nextBlockOnDiskSizeWithHeader;

          if (b.nextBlockOnDiskSizeWithHeader > 0)
            setNextBlockHeader(offset, b);
        } else {
          // Allocate enough space to fit the next block's header too.
          byte[] onDiskBlock = new byte[onDiskSizeWithHeader + HEADER_SIZE];

          int nextBlockOnDiskSize = readAtOffset(onDiskBlock,
              preReadHeaderSize, onDiskSizeWithHeader - preReadHeaderSize,
              true, offset + preReadHeaderSize, pread);

          if (header == null)
            header = onDiskBlock;

          try {
            b = new HFileBlock(ByteBuffer.wrap(header, 0, HEADER_SIZE));
          } catch (IOException ex) {
            // Seen in load testing. Provide comprehensive debug info.
            throw new IOException("Failed to read compressed block at "
                + offset + ", onDiskSizeWithoutHeader=" + onDiskSizeWithHeader
                + ", preReadHeaderSize=" + preReadHeaderSize
                + ", header.length=" + header.length + ", header bytes: "
                + Bytes.toStringBinary(header, 0, HEADER_SIZE), ex);
          }
          b.validateOnDiskSizeWithoutHeader(onDiskSizeWithoutHeader);
          b.nextBlockOnDiskSizeWithHeader = nextBlockOnDiskSize;

          DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
              onDiskBlock, HEADER_SIZE, onDiskSizeWithoutHeader));

          // This will allocate a new buffer but keep header bytes.
          b.allocateBuffer(b.nextBlockOnDiskSizeWithHeader > 0);

          decompress(b.buf.array(), b.buf.arrayOffset() + HEADER_SIZE, dis,
              onDiskSizeWithoutHeader, b.uncompressedSizeWithoutHeader);

          // Copy next block's header bytes into the new block if we have them.
          if (nextBlockOnDiskSize > 0) {
            System.arraycopy(onDiskBlock, onDiskSizeWithHeader, b.buf.array(),
                b.buf.arrayOffset() + HEADER_SIZE
                    + b.uncompressedSizeWithoutHeader, HEADER_SIZE);

            setNextBlockHeader(offset, b);
          }
        }

      } else {
        // We don't know the on-disk size. Read the header first, determine the
        // on-disk size from it, and read the remaining data, thereby incurring
        // two read operations. This might happen when we are doing the first
        // read in a series of reads or a random read, and we don't have access
        // to the block index. This is costly and should happen very rarely.

        // Check if we have read this block's header as part of reading the
        // previous block. If so, don't read the header again.
        PrefetchedHeader prefetchedHeader = prefetchedHeaderForThread.get();
        ByteBuffer headerBuf = prefetchedHeader.offset == offset ?
            prefetchedHeader.buf : null;

        if (headerBuf == null) {
          // Unfortunately, we still have to do a separate read operation to
          // read the header.
          headerBuf = ByteBuffer.allocate(HEADER_SIZE);;
          readAtOffset(headerBuf.array(), headerBuf.arrayOffset(), HEADER_SIZE,
              false, offset, pread);
        }

        b = new HFileBlock(headerBuf);

        // This will also allocate enough room for the next block's header.
        b.allocateBuffer(true);

        if (compressAlgo == Compression.Algorithm.NONE) {

          // Avoid creating bounded streams and using a "codec" that does
          // nothing.
          b.assumeUncompressed();
          b.nextBlockOnDiskSizeWithHeader = readAtOffset(b.buf.array(),
              b.buf.arrayOffset() + HEADER_SIZE,
              b.uncompressedSizeWithoutHeader, true, offset + HEADER_SIZE,
              pread);

          if (b.nextBlockOnDiskSizeWithHeader > 0) {
            setNextBlockHeader(offset, b);
          }
        } else {
          // Allocate enough space for the block's header and compressed data.
          byte[] compressedBytes = new byte[b.getOnDiskSizeWithHeader()
              + HEADER_SIZE];

          b.nextBlockOnDiskSizeWithHeader = readAtOffset(compressedBytes,
              HEADER_SIZE, b.onDiskSizeWithoutHeader, true, offset
                  + HEADER_SIZE, pread);
          DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
              compressedBytes, HEADER_SIZE, b.onDiskSizeWithoutHeader));

          decompress(b.buf.array(), b.buf.arrayOffset() + HEADER_SIZE, dis,
              b.onDiskSizeWithoutHeader, b.uncompressedSizeWithoutHeader);

          if (b.nextBlockOnDiskSizeWithHeader > 0) {
            // Copy the next block's header into the new block.
            int nextHeaderOffset = b.buf.arrayOffset() + HEADER_SIZE
                + b.uncompressedSizeWithoutHeader;
            System.arraycopy(compressedBytes,
                compressedBytes.length - HEADER_SIZE,
                b.buf.array(),
                nextHeaderOffset,
                HEADER_SIZE);

            setNextBlockHeader(offset, b);
          }
        }
      }

      b.offset = offset;
      return b;
    }

    private void setNextBlockHeader(long offset, HFileBlock b) {
      PrefetchedHeader prefetchedHeader = prefetchedHeaderForThread.get();
      prefetchedHeader.offset = offset + b.getOnDiskSizeWithHeader();
      int nextHeaderOffset = b.buf.arrayOffset() + HEADER_SIZE
          + b.uncompressedSizeWithoutHeader;
      System.arraycopy(b.buf.array(), nextHeaderOffset,
          prefetchedHeader.header, 0, HEADER_SIZE);
    }

  }

  @Override
  public int getSerializedLength() {
    if (buf != null) {
      return this.buf.limit() + HFileBlock.EXTRA_SERIALIZATION_SPACE;
    }
    return 0;
  }

  @Override
  public void serialize(ByteBuffer destination) {
    destination.put(this.buf.duplicate());
    destination.putLong(this.offset);
    destination.putInt(this.nextBlockOnDiskSizeWithHeader);
    destination.rewind();
  }

  @Override
  public CacheableDeserializer<Cacheable> getDeserializer() {
    return HFileBlock.blockDeserializer;
  }

  @Override
  public boolean equals(Object comparison) {
    if (this == comparison) {
      return true;
    }
    if (comparison == null) {
      return false;
    }
    if (comparison.getClass() != this.getClass()) {
      return false;
    }

    HFileBlock castedComparison = (HFileBlock) comparison;

    if (castedComparison.blockType != this.blockType) {
      return false;
    }
    if (castedComparison.nextBlockOnDiskSizeWithHeader != this.nextBlockOnDiskSizeWithHeader) {
      return false;
    }
    if (castedComparison.offset != this.offset) {
      return false;
    }
    if (castedComparison.onDiskSizeWithoutHeader != this.onDiskSizeWithoutHeader) {
      return false;
    }
    if (castedComparison.prevBlockOffset != this.prevBlockOffset) {
      return false;
    }
    if (castedComparison.uncompressedSizeWithoutHeader != this.uncompressedSizeWithoutHeader) {
      return false;
    }
    if (this.buf.compareTo(castedComparison.buf) != 0) {
      return false;
    }
    if (this.buf.position() != castedComparison.buf.position()){
      return false;
    }
    if (this.buf.limit() != castedComparison.buf.limit()){
      return false;
    }
    return true;
  }


}
