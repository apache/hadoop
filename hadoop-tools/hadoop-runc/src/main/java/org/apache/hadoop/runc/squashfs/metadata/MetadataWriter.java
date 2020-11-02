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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

public class MetadataWriter implements DataOutput {

  private final byte[] xfer = new byte[1];
  private final byte[] currentBlock = new byte[8192];
  private final List<byte[]> blocks = new ArrayList<>();
  private long location = 0L;
  private int offset = 0;

  public void save(DataOutput out) throws IOException {
    flush();
    for (byte[] block : blocks) {
      out.write(block);
    }
    location = 0L;
    offset = 0;
    blocks.clear();
  }

  public MetadataBlockRef getCurrentReference() {
    return new MetadataBlockRef((int) (location & 0xffffffff), (short) offset);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  public void flush() throws IOException {
    if (offset == 0) {
      return;
    }

    byte[] compressed = compress(currentBlock, 0, offset);
    byte[] encoded;
    int size;

    if (compressed != null) {
      size = compressed.length & 0x7fff;
      encoded = new byte[compressed.length + 2];
      System.arraycopy(compressed, 0, encoded, 2, compressed.length);
    } else {
      size = (offset & 0x7fff) | 0x8000;
      encoded = new byte[offset + 2];
      System.arraycopy(currentBlock, 0, encoded, 2, offset);
    }

    encoded[0] = (byte) (size & 0xff);
    encoded[1] = (byte) ((size >> 8) & 0xff);

    blocks.add(encoded);
    location += encoded.length;
    offset = 0;
  }

  private byte[] compress(byte[] data, int offset, int length)
      throws IOException {
    Deflater def = new Deflater(Deflater.BEST_COMPRESSION);
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      try (DeflaterOutputStream dos = new DeflaterOutputStream(
          bos, def, 4096)) {
        dos.write(data, offset, length);
      }
      byte[] result = bos.toByteArray();
      if (result.length > length) {
        return null;
      }
      return result;
    } finally {
      def.end();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    while (len > 0) {
      int capacity = currentBlock.length - offset;
      int bytesToWrite = Math.min(len, capacity);

      System.arraycopy(b, off, currentBlock, offset, bytesToWrite);
      offset += bytesToWrite;
      off += bytesToWrite;
      len -= bytesToWrite;
      if (currentBlock.length == offset) {
        flush();
      }
    }
  }

  private void writeByteInternal(byte b) throws IOException {
    xfer[0] = b;
    write(xfer);
  }

  @Override
  public void write(int b) throws IOException {
    writeByteInternal((byte) (b & 0xff));
  }

  @Override
  public void writeBoolean(boolean v) throws IOException {
    writeByteInternal((byte) (v ? 1 : 0));
  }

  @Override
  public void writeByte(int v) throws IOException {
    writeByteInternal((byte) (v & 0xff));
  }

  @Override
  public void writeShort(int v) throws IOException {
    writeByteInternal((byte) ((v >>> 0) & 0xff));
    writeByteInternal((byte) ((v >>> 8) & 0xff));
  }

  @Override
  public void writeChar(int v) throws IOException {
    writeByteInternal((byte) ((v >>> 0) & 0xff));
    writeByteInternal((byte) ((v >>> 8) & 0xff));
  }

  @Override
  public void writeInt(int v) throws IOException {
    writeByteInternal((byte) ((v >>> 0) & 0xff));
    writeByteInternal((byte) ((v >>> 8) & 0xff));
    writeByteInternal((byte) ((v >>> 16) & 0xff));
    writeByteInternal((byte) ((v >>> 24) & 0xff));
  }

  @Override
  public void writeLong(long v) throws IOException {
    writeByteInternal((byte) ((v >>> 0) & 0xff));
    writeByteInternal((byte) ((v >>> 8) & 0xff));
    writeByteInternal((byte) ((v >>> 16) & 0xff));
    writeByteInternal((byte) ((v >>> 24) & 0xff));
    writeByteInternal((byte) ((v >>> 32) & 0xff));
    writeByteInternal((byte) ((v >>> 40) & 0xff));
    writeByteInternal((byte) ((v >>> 48) & 0xff));
    writeByteInternal((byte) ((v >>> 56) & 0xff));
  }

  @Override
  public void writeFloat(float v) throws IOException {
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public void writeDouble(double v) throws IOException {
    writeLong(Double.doubleToLongBits(v));
  }

  @Override
  public void writeBytes(String s) throws IOException {
    int len = s.length();
    for (int i = 0; i < len; i++) {
      writeByteInternal((byte) s.charAt(i));
    }
  }

  @Override
  public void writeChars(String s) throws IOException {
    int len = s.length();
    for (int i = 0; i < len; i++) {
      int v = s.charAt(i);
      writeByteInternal((byte) ((v >>> 0) & 0xff));
      writeByteInternal((byte) ((v >>> 8) & 0xff));
    }
  }

  @Override
  public void writeUTF(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

}
