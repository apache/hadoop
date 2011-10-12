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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.KeyValue.KeyComparator;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

/**
 * Common functionality needed by all versions of {@link HFile} writers.
 */
public abstract class AbstractHFileWriter implements HFile.Writer {

  /** Key previously appended. Becomes the last key in the file. */
  protected byte[] lastKeyBuffer = null;

  protected int lastKeyOffset = -1;
  protected int lastKeyLength = -1;

  /** FileSystem stream to write into. */
  protected FSDataOutputStream outputStream;

  /** True if we opened the <code>outputStream</code> (and so will close it). */
  protected final boolean closeOutputStream;

  /** A "file info" block: a key-value map of file-wide metadata. */
  protected FileInfo fileInfo = new HFile.FileInfo();

  /** Number of uncompressed bytes we allow per block. */
  protected final int blockSize;

  /** Total # of key/value entries, i.e. how many times add() was called. */
  protected long entryCount = 0;

  /** Used for calculating the average key length. */
  protected long totalKeyLength = 0;

  /** Used for calculating the average value length. */
  protected long totalValueLength = 0;

  /** Total uncompressed bytes, maybe calculate a compression ratio later. */
  protected long totalUncompressedBytes = 0;

  /** Key comparator. Used to ensure we write in order. */
  protected final RawComparator<byte[]> comparator;

  /** Meta block names. */
  protected List<byte[]> metaNames = new ArrayList<byte[]>();

  /** {@link Writable}s representing meta block data. */
  protected List<Writable> metaData = new ArrayList<Writable>();

  /** The compression algorithm used. NONE if no compression. */
  protected final Compression.Algorithm compressAlgo;

  /** First key in a block. */
  protected byte[] firstKeyInBlock = null;

  /** May be null if we were passed a stream. */
  protected final Path path;

  /** Cache configuration for caching data on write. */
  protected final CacheConfig cacheConf;

  /**
   * Name for this object used when logging or in toString. Is either
   * the result of a toString on stream or else toString of passed file Path.
   */
  protected final String name;

  public AbstractHFileWriter(CacheConfig cacheConf,
      FSDataOutputStream outputStream, Path path, int blockSize,
      Compression.Algorithm compressAlgo, KeyComparator comparator) {
    this.outputStream = outputStream;
    this.path = path;
    this.name = path != null ? path.getName() : outputStream.toString();
    this.blockSize = blockSize;
    this.compressAlgo = compressAlgo == null
        ? HFile.DEFAULT_COMPRESSION_ALGORITHM : compressAlgo;
    this.comparator = comparator != null ? comparator
        : Bytes.BYTES_RAWCOMPARATOR;

    closeOutputStream = path != null;
    this.cacheConf = cacheConf;
  }

  /**
   * Add last bits of metadata to file info before it is written out.
   */
  protected void finishFileInfo() throws IOException {
    if (lastKeyBuffer != null) {
      // Make a copy. The copy is stuffed into HMapWritable. Needs a clean
      // byte buffer. Won't take a tuple.
      fileInfo.append(FileInfo.LASTKEY, Arrays.copyOfRange(lastKeyBuffer,
          lastKeyOffset, lastKeyOffset + lastKeyLength), false);
    }

    // Average key length.
    int avgKeyLen =
        entryCount == 0 ? 0 : (int) (totalKeyLength / entryCount);
    fileInfo.append(FileInfo.AVG_KEY_LEN, Bytes.toBytes(avgKeyLen), false);

    // Average value length.
    int avgValueLen =
        entryCount == 0 ? 0 : (int) (totalValueLength / entryCount);
    fileInfo.append(FileInfo.AVG_VALUE_LEN, Bytes.toBytes(avgValueLen), false);
  }

  /**
   * Add to the file info. All added key/value pairs can be obtained using
   * {@link HFile.Reader#loadFileInfo()}.
   *
   * @param k Key
   * @param v Value
   * @throws IOException in case the key or the value are invalid
   */
  @Override
  public void appendFileInfo(final byte[] k, final byte[] v)
      throws IOException {
    fileInfo.append(k, v, true);
  }

  /**
   * Sets the file info offset in the trailer, finishes up populating fields in
   * the file info, and writes the file info into the given data output. The
   * reason the data output is not always {@link #outputStream} is that we store
   * file info as a block in version 2.
   *
   * @param trailer fixed file trailer
   * @param out the data output to write the file info to
   * @throws IOException
   */
  protected final void writeFileInfo(FixedFileTrailer trailer, DataOutput out)
      throws IOException {
    trailer.setFileInfoOffset(outputStream.getPos());
    finishFileInfo();
    fileInfo.write(out);
  }

  /**
   * Checks that the given key does not violate the key order.
   *
   * @param key Key to check.
   * @return true if the key is duplicate
   * @throws IOException if the key or the key order is wrong
   */
  protected boolean checkKey(final byte[] key, final int offset,
      final int length) throws IOException {
    boolean isDuplicateKey = false;

    if (key == null || length <= 0) {
      throw new IOException("Key cannot be null or empty");
    }
    if (length > HFile.MAXIMUM_KEY_LENGTH) {
      throw new IOException("Key length " + length + " > "
          + HFile.MAXIMUM_KEY_LENGTH);
    }
    if (lastKeyBuffer != null) {
      int keyComp = comparator.compare(lastKeyBuffer, lastKeyOffset,
          lastKeyLength, key, offset, length);
      if (keyComp > 0) {
        throw new IOException("Added a key not lexically larger than"
            + " previous key="
            + Bytes.toStringBinary(key, offset, length)
            + ", lastkey="
            + Bytes.toStringBinary(lastKeyBuffer, lastKeyOffset,
                lastKeyLength));
      } else if (keyComp == 0) {
        isDuplicateKey = true;
      }
    }
    return isDuplicateKey;
  }

  /** Checks the given value for validity. */
  protected void checkValue(final byte[] value, final int offset,
      final int length) throws IOException {
    if (value == null) {
      throw new IOException("Value cannot be null");
    }
  }

  /**
   * @return Path or null if we were passed a stream rather than a Path.
   */
  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return "writer=" + (path != null ? path.toString() : null) + ", name="
        + name + ", compression=" + compressAlgo.getName();
  }

  /**
   * Sets remaining trailer fields, writes the trailer to disk, and optionally
   * closes the output stream.
   */
  protected void finishClose(FixedFileTrailer trailer) throws IOException {
    trailer.setMetaIndexCount(metaNames.size());
    trailer.setTotalUncompressedBytes(totalUncompressedBytes);
    trailer.setEntryCount(entryCount);
    trailer.setCompressionCodec(compressAlgo);

    trailer.serialize(outputStream);

    if (closeOutputStream) {
      outputStream.close();
      outputStream = null;
    }
  }

  public static Compression.Algorithm compressionByName(String algoName) {
    if (algoName == null)
      return HFile.DEFAULT_COMPRESSION_ALGORITHM;
    return Compression.getCompressionAlgorithmByName(algoName);
  }

  /** A helper method to create HFile output streams in constructors */
  protected static FSDataOutputStream createOutputStream(Configuration conf,
      FileSystem fs, Path path) throws IOException {
    return fs.create(path, FsPermission.getDefault(), true,
        fs.getConf().getInt("io.file.buffer.size", 4096),
        fs.getDefaultReplication(), fs.getDefaultBlockSize(),
        null);
  }
}
