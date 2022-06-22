/**
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

package org.apache.hadoop.runc.squashfs.metadata;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlockFlag;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.BINARY;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.DECIMAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

public class MetadataBlock {

  public static final int MAX_SIZE = 8192;
  public static final int HEADER_SIZE = 2;

  private static final byte[] EMPTY = new byte[0];

  private short header;
  private byte[] data = EMPTY;
  private short fileLength = 0;

  public static MetadataReader reader(MetadataBlockReader metaReader,
      MetadataReference metaRef) throws IOException {
    return new MetadataReader(metaReader, metaRef);
  }

  public static MetadataBlock read(DataInput in, SuperBlock sb)
      throws IOException, SquashFsException {
    MetadataBlock block = new MetadataBlock();
    block.readData(in, sb);
    return block;
  }

  public byte[] getData() {
    return data;
  }

  public short getDataSize() {
    return (short) (header & 0x7fff);
  }

  public boolean isCompressed() {
    return (header & 0x8000) == 0;
  }

  public short getFileLength() {
    return fileLength;
  }

  public void readData(DataInput in, SuperBlock sb)
      throws IOException, SquashFsException {
    readHeader(in);
    fileLength = (short) (HEADER_SIZE + readPayload(in, sb));
  }

  private void readHeader(DataInput in) throws IOException {
    byte[] raw = new byte[HEADER_SIZE];
    in.readFully(raw);
    ByteBuffer buffer = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
    header = buffer.getShort();
  }

  private int readPayload(DataInput in, SuperBlock sb)
      throws IOException, SquashFsException {
    if (isCompressed()) {
      return readCompressed(in, sb);
    } else {
      return readUncompressed(in);
    }
  }

  private int readUncompressed(DataInput in) throws IOException {
    int size = getDataSize();
    if (size > MAX_SIZE) {
      throw new SquashFsException(
          String.format("Corrupt metadata block: Got size %d (max = %d)", size,
              MAX_SIZE));
    }
    data = new byte[size];
    in.readFully(data);
    return data.length;
  }

  private int readCompressed(DataInput in, SuperBlock sb)
      throws IOException, SquashFsException {
    switch (sb.getCompressionId()) {
    case NONE:
      throw new SquashFsException(
          "Archive claims no compression, but found compressed data");
    case ZLIB:
      return readCompressedZlib(in, sb);
    default:
      throw new UnsupportedOperationException(
          String.format("Reading compressed data of type %s not yet supported",
              sb.getCompressionId()));
    }
  }

  private int readCompressedZlib(DataInput in, SuperBlock sb)
      throws IOException, SquashFsException {
    // see if there are compression flags
    if (sb.hasFlag(SuperBlockFlag.COMPRESSOR_OPTIONS)) {
      throw new UnsupportedOperationException("Reading ZLIB compressed data "
          + "with non-standard options not yet supported");
    }

    int dataSize = getDataSize();
    byte[] buf = new byte[dataSize];
    in.readFully(buf);

    byte[] xfer = new byte[MAX_SIZE];
    try (ByteArrayInputStream bis = new ByteArrayInputStream(buf)) {
      try (
          InflaterInputStream iis = new InflaterInputStream(bis, new Inflater(),
              MAX_SIZE)) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(MAX_SIZE)) {
          int c = 0;
          while ((c = iis.read(xfer, 0, MAX_SIZE)) >= 0) {
            if (c > 0) {
              bos.write(xfer, 0, c);
            }
          }
          data = bos.toByteArray();
          if (data.length > MAX_SIZE) {
            throw new SquashFsException(String.format(
                "Corrupt metadata block: Got size %d (max = %d)", data.length,
                MAX_SIZE));
          }
        }
      }
    }

    return dataSize;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(String.format("metadata-block: {%n"));
    int width = 22;
    dumpBin(buf, width, "header", header, BINARY, UNSIGNED);
    dumpBin(buf, width, "dataSize (decoded)", getDataSize(), DECIMAL, UNSIGNED);
    dumpBin(buf, width, "fileLength", (short) fileLength, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "compressed (decoded)",
        isCompressed() ? "true" : "false");
    dumpBin(buf, width, "blockSize", data.length, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "data", data, 0, data.length, 16, 2);
    buf.append("}");
    return buf.toString();
  }

}
