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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.RawComparator;

import static org.apache.hadoop.hbase.io.hfile.HFile.MIN_FORMAT_VERSION;
import static org.apache.hadoop.hbase.io.hfile.HFile.MAX_FORMAT_VERSION;

import com.google.common.io.NullOutputStream;

/**
 * The {@link HFile} has a fixed trailer which contains offsets to other
 * variable parts of the file. Also includes basic metadata on this file. The
 * trailer size is fixed within a given {@link HFile} format version only, but
 * we always store the version number as the last four-byte integer of the file.
 */
public class FixedFileTrailer {

  private static final Log LOG = LogFactory.getLog(FixedFileTrailer.class);

  /**
   * We store the comparator class name as a fixed-length field in the trailer.
   */
  private static final int MAX_COMPARATOR_NAME_LENGTH = 128;

  /**
   * Offset to the fileinfo data, a small block of vitals. Necessary in v1 but
   * only potentially useful for pretty-printing in v2.
   */
  private long fileInfoOffset;

  /**
   * In version 1, the offset to the data block index. Starting from version 2,
   * the meaning of this field is the offset to the section of the file that
   * should be loaded at the time the file is being opened, and as of the time
   * of writing, this happens to be the offset of the file info section.
   */
  private long loadOnOpenDataOffset;

  /** The number of entries in the root data index. */
  private int dataIndexCount;

  /** Total uncompressed size of all blocks of the data index */
  private long uncompressedDataIndexSize;

  /** The number of entries in the meta index */
  private int metaIndexCount;

  /** The total uncompressed size of keys/values stored in the file. */
  private long totalUncompressedBytes;

  /**
   * The number of key/value pairs in the file. This field was int in version 1,
   * but is now long.
   */
  private long entryCount;

  /** The compression codec used for all blocks. */
  private Compression.Algorithm compressionCodec = Compression.Algorithm.NONE;

  /**
   * The number of levels in the potentially multi-level data index. Used from
   * version 2 onwards.
   */
  private int numDataIndexLevels;

  /** The offset of the first data block. */
  private long firstDataBlockOffset;

  /**
   * It is guaranteed that no key/value data blocks start after this offset in
   * the file.
   */
  private long lastDataBlockOffset;

  /** Raw key comparator class name in version 2 */
  private String comparatorClassName = RawComparator.class.getName();

  /** The {@link HFile} format version. */
  private final int version;

  FixedFileTrailer(int version) {
    this.version = version;
    HFile.checkFormatVersion(version);
  }

  private static int[] computeTrailerSizeByVersion() {
    int versionToSize[] = new int[HFile.MAX_FORMAT_VERSION + 1];
    for (int version = MIN_FORMAT_VERSION;
         version <= MAX_FORMAT_VERSION;
         ++version) {
      FixedFileTrailer fft = new FixedFileTrailer(version);
      DataOutputStream dos = new DataOutputStream(new NullOutputStream());
      try {
        fft.serialize(dos);
      } catch (IOException ex) {
        // The above has no reason to fail.
        throw new RuntimeException(ex);
      }
      versionToSize[version] = dos.size();
    }
    return versionToSize;
  }

  private static int getMaxTrailerSize() {
    int maxSize = 0;
    for (int version = MIN_FORMAT_VERSION;
         version <= MAX_FORMAT_VERSION;
         ++version)
      maxSize = Math.max(getTrailerSize(version), maxSize);
    return maxSize;
  }

  private static final int TRAILER_SIZE[] = computeTrailerSizeByVersion();
  private static final int MAX_TRAILER_SIZE = getMaxTrailerSize();

  static int getTrailerSize(int version) {
    return TRAILER_SIZE[version];
  }

  public int getTrailerSize() {
    return getTrailerSize(version);
  }

  /**
   * Write the trailer to a data stream. We support writing version 1 for
   * testing and for determining version 1 trailer size. It is also easy to see
   * what fields changed in version 2.
   *
   * @param outputStream
   * @throws IOException
   */
  void serialize(DataOutputStream outputStream) throws IOException {
    HFile.checkFormatVersion(version);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput baosDos = new DataOutputStream(baos);

    BlockType.TRAILER.write(baosDos);
    baosDos.writeLong(fileInfoOffset);
    baosDos.writeLong(loadOnOpenDataOffset);
    baosDos.writeInt(dataIndexCount);

    if (version == 1) {
      // This used to be metaIndexOffset, but it was not used in version 1.
      baosDos.writeLong(0);
    } else {
      baosDos.writeLong(uncompressedDataIndexSize);
    }

    baosDos.writeInt(metaIndexCount);
    baosDos.writeLong(totalUncompressedBytes);
    if (version == 1) {
      baosDos.writeInt((int) Math.min(Integer.MAX_VALUE, entryCount));
    } else {
      // This field is long from version 2 onwards.
      baosDos.writeLong(entryCount);
    }
    baosDos.writeInt(compressionCodec.ordinal());

    if (version > 1) {
      baosDos.writeInt(numDataIndexLevels);
      baosDos.writeLong(firstDataBlockOffset);
      baosDos.writeLong(lastDataBlockOffset);
      Bytes.writeStringFixedSize(baosDos, comparatorClassName,
          MAX_COMPARATOR_NAME_LENGTH);
    }
    baosDos.writeInt(version);

    outputStream.write(baos.toByteArray());
  }

  /**
   * Deserialize the fixed file trailer from the given stream. The version needs
   * to already be specified. Make sure this is consistent with
   * {@link #serialize(DataOutputStream)}.
   *
   * @param inputStream
   * @param version
   * @throws IOException
   */
  void deserialize(DataInputStream inputStream) throws IOException {
    HFile.checkFormatVersion(version);

    BlockType.TRAILER.readAndCheck(inputStream);

    fileInfoOffset = inputStream.readLong();
    loadOnOpenDataOffset = inputStream.readLong();
    dataIndexCount = inputStream.readInt();

    if (version == 1) {
      inputStream.readLong(); // Read and skip metaIndexOffset.
    } else {
      uncompressedDataIndexSize = inputStream.readLong();
    }
    metaIndexCount = inputStream.readInt();

    totalUncompressedBytes = inputStream.readLong();
    entryCount = version == 1 ? inputStream.readInt() : inputStream.readLong();
    compressionCodec = Compression.Algorithm.values()[inputStream.readInt()];
    if (version > 1) {
      numDataIndexLevels = inputStream.readInt();
      firstDataBlockOffset = inputStream.readLong();
      lastDataBlockOffset = inputStream.readLong();
      comparatorClassName =
          Bytes.readStringFixedSize(inputStream, MAX_COMPARATOR_NAME_LENGTH);
    }

    expectVersion(inputStream.readInt());
  }

  private void append(StringBuilder sb, String s) {
    if (sb.length() > 0)
      sb.append(", ");
    sb.append(s);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    append(sb, "fileinfoOffset=" + fileInfoOffset);
    append(sb, "loadOnOpenDataOffset=" + loadOnOpenDataOffset);
    append(sb, "dataIndexCount=" + dataIndexCount);
    append(sb, "metaIndexCount=" + metaIndexCount);
    append(sb, "totalUncomressedBytes=" + totalUncompressedBytes);
    append(sb, "entryCount=" + entryCount);
    append(sb, "compressionCodec=" + compressionCodec);
    if (version == 2) {
      append(sb, "uncompressedDataIndexSize=" + uncompressedDataIndexSize);
      append(sb, "numDataIndexLevels=" + numDataIndexLevels);
      append(sb, "firstDataBlockOffset=" + firstDataBlockOffset);
      append(sb, "lastDataBlockOffset=" + lastDataBlockOffset);
      append(sb, "comparatorClassName=" + comparatorClassName);
    }
    append(sb, "version=" + version);

    return sb.toString();
  }

  /**
   * Reads a file trailer from the given file.
   *
   * @param istream the input stream with the ability to seek. Does not have to
   *          be buffered, as only one read operation is made.
   * @param fileSize the file size. Can be obtained using
   *          {@link org.apache.hadoop.fs.FileSystem#getFileStatus(
   *          org.apache.hadoop.fs.Path)}.
   * @return the fixed file trailer read
   * @throws IOException if failed to read from the underlying stream, or the
   *           trailer is corrupted, or the version of the trailer is
   *           unsupported
   */
  public static FixedFileTrailer readFromStream(FSDataInputStream istream,
      long fileSize) throws IOException {
    int bufferSize = MAX_TRAILER_SIZE;
    long seekPoint = fileSize - bufferSize;
    if (seekPoint < 0) {
      // It is hard to imagine such a small HFile.
      seekPoint = 0;
      bufferSize = (int) fileSize;
    }

    istream.seek(seekPoint);
    ByteBuffer buf = ByteBuffer.allocate(bufferSize);
    istream.readFully(buf.array(), buf.arrayOffset(),
        buf.arrayOffset() + buf.limit());

    // Read the version from the last int of the file.
    buf.position(buf.limit() - Bytes.SIZEOF_INT);
    int version = buf.getInt();

    try {
      HFile.checkFormatVersion(version);
    } catch (IllegalArgumentException iae) {
      // In this context, an invalid version might indicate a corrupt HFile.
      throw new IOException(iae);
    }

    int trailerSize = getTrailerSize(version);

    FixedFileTrailer fft = new FixedFileTrailer(version);
    fft.deserialize(new DataInputStream(new ByteArrayInputStream(buf.array(),
        buf.arrayOffset() + bufferSize - trailerSize, trailerSize)));
    return fft;
  }

  public void expectVersion(int expected) {
    if (version != expected) {
      throw new IllegalArgumentException("Invalid HFile version: " + version
          + " (expected: " + expected + ")");
    }
  }

  public void expectAtLeastVersion(int lowerBound) {
    if (version < lowerBound) {
      throw new IllegalArgumentException("Invalid HFile version: " + version
          + " (expected: " + lowerBound + " or higher).");
    }
  }

  public long getFileInfoOffset() {
    return fileInfoOffset;
  }

  public void setFileInfoOffset(long fileInfoOffset) {
    this.fileInfoOffset = fileInfoOffset;
  }

  public long getLoadOnOpenDataOffset() {
    return loadOnOpenDataOffset;
  }

  public void setLoadOnOpenOffset(long loadOnOpenDataOffset) {
    this.loadOnOpenDataOffset = loadOnOpenDataOffset;
  }

  public int getDataIndexCount() {
    return dataIndexCount;
  }

  public void setDataIndexCount(int dataIndexCount) {
    this.dataIndexCount = dataIndexCount;
  }

  public int getMetaIndexCount() {
    return metaIndexCount;
  }

  public void setMetaIndexCount(int metaIndexCount) {
    this.metaIndexCount = metaIndexCount;
  }

  public long getTotalUncompressedBytes() {
    return totalUncompressedBytes;
  }

  public void setTotalUncompressedBytes(long totalUncompressedBytes) {
    this.totalUncompressedBytes = totalUncompressedBytes;
  }

  public long getEntryCount() {
    return entryCount;
  }

  public void setEntryCount(long newEntryCount) {
    if (version == 1) {
      int intEntryCount = (int) Math.min(Integer.MAX_VALUE, newEntryCount);
      if (intEntryCount != newEntryCount) {
        LOG.info("Warning: entry count is " + newEntryCount + " but writing "
            + intEntryCount + " into the version " + version + " trailer");
      }
      entryCount = intEntryCount;
      return;
    }
    entryCount = newEntryCount;
  }

  public Compression.Algorithm getCompressionCodec() {
    return compressionCodec;
  }

  public void setCompressionCodec(Compression.Algorithm compressionCodec) {
    this.compressionCodec = compressionCodec;
  }

  public int getNumDataIndexLevels() {
    expectAtLeastVersion(2);
    return numDataIndexLevels;
  }

  public void setNumDataIndexLevels(int numDataIndexLevels) {
    expectAtLeastVersion(2);
    this.numDataIndexLevels = numDataIndexLevels;
  }

  public long getLastDataBlockOffset() {
    expectAtLeastVersion(2);
    return lastDataBlockOffset;
  }

  public void setLastDataBlockOffset(long lastDataBlockOffset) {
    expectAtLeastVersion(2);
    this.lastDataBlockOffset = lastDataBlockOffset;
  }

  public long getFirstDataBlockOffset() {
    expectAtLeastVersion(2);
    return firstDataBlockOffset;
  }

  public void setFirstDataBlockOffset(long firstDataBlockOffset) {
    expectAtLeastVersion(2);
    this.firstDataBlockOffset = firstDataBlockOffset;
  }

  public int getVersion() {
    return version;
  }

  @SuppressWarnings("rawtypes")
  public void setComparatorClass(Class<? extends RawComparator> klass) {
    expectAtLeastVersion(2);
    comparatorClassName = klass.getName();
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends RawComparator<byte[]>> getComparatorClass(
      String comparatorClassName) throws IOException {
    try {
      return (Class<? extends RawComparator<byte[]>>)
          Class.forName(comparatorClassName);
    } catch (ClassNotFoundException ex) {
      throw new IOException(ex);
    }
  }

  public static RawComparator<byte[]> createComparator(
      String comparatorClassName) throws IOException {
    try {
      return getComparatorClass(comparatorClassName).newInstance();
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  RawComparator<byte[]> createComparator() throws IOException {
    expectAtLeastVersion(2);
    return createComparator(comparatorClassName);
  }

  public long getUncompressedDataIndexSize() {
    if (version == 1)
      return 0;
    return uncompressedDataIndexSize;
  }

  public void setUncompressedDataIndexSize(
      long uncompressedDataIndexSize) {
    expectAtLeastVersion(2);
    this.uncompressedDataIndexSize = uncompressedDataIndexSize;
  }

}
