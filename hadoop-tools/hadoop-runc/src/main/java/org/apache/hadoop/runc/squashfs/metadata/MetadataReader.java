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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

public class MetadataReader implements DataInput {

  private static final Logger LOG =
      LoggerFactory.getLogger(MetadataReader.class);

  private final MetadataBlockReader blockReader;
  private final int tag;
  private final int maxLength;
  private final int startPosition;

  private MetadataBlock block = null;
  private long nextBlockLocation;
  private int position = -1;
  private int bytesRead = 0;

  MetadataReader(MetadataBlockReader blockReader, MetadataReference metaRef)
      throws IOException {
    this.blockReader = blockReader;
    this.tag = metaRef.getTag();
    this.nextBlockLocation = metaRef.getBlockLocation();
    this.startPosition = (int) metaRef.getOffset();
    this.maxLength = metaRef.getMaxLength() == Integer.MAX_VALUE
        ? Integer.MAX_VALUE
        : metaRef.getMaxLength() + ((int) metaRef.getOffset());
    skipBytes((int) metaRef.getOffset());
    LOG.trace("Reader initialized for reference: \n{}", metaRef);
  }

  public boolean isEof() throws IOException {
    if (bytesRead >= maxLength) {
      return true;
    }
    if (position >= 0 && position < block.getData().length) {
      return false;
    }

    // unknown, read more data
    try {
      return bytesAvailable() <= 0;
    } catch (EOFException e) {
      return true;
    }
  }

  public int position() {
    return bytesRead - startPosition;
  }

  public int available() throws IOException {
    if (block == null || bytesRead >= maxLength) {
      return 0;
    }
    int len = block.getData().length;
    if (position < 0 || position >= len) {
      return 0;
    }
    return Math.max(0, Math.min(len - position, maxLength - bytesRead));
  }

  private int bytesAvailable() throws IOException {
    ensureDataReady();

    if (block == null || bytesRead >= maxLength) {
      return -1; // EOF
    }

    int len = block.getData().length;
    if (position < 0 || position >= len) {
      return 0;
    }

    return Math.max(-1, Math.min(len - position, maxLength - bytesRead));
  }

  private void ensureDataReady() throws SquashFsException, IOException {
    if (bytesRead >= maxLength) {
      block = null;
      return; // EOF
    }

    if (position >= 0 && position < block.getData().length) {
      return;
    }

    block = blockReader.read(tag, nextBlockLocation);

    position = 0;
    nextBlockLocation += block.getFileLength();
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    readFully(b, 0, b.length);
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    int totalRead = 0;
    while (len > 0) {
      int av = bytesAvailable();
      if (av <= 0) {
        throw new EOFException(String.format(
            "Read past end of block list. " +
                "Read %d bytes, caller wanted %d more",
            totalRead, len));
      }
      int read = Math.min(len, av);
      System.arraycopy(block.getData(), position, b, off, read);
      off += read;
      len -= read;
      position += read;
      bytesRead += read;
      totalRead += read;
    }
  }

  @Override
  public int skipBytes(int n) throws IOException {
    int bytesToSkip = n;
    int bytesSkipped = 0;

    while (bytesToSkip > 0) {
      int av = bytesAvailable();
      if (av <= 0) {
        return bytesSkipped;
      }
      int skip = Math.min(bytesToSkip, av);
      position += skip;
      bytesRead += skip;
      bytesSkipped += skip;
      bytesToSkip -= skip;
    }
    return bytesSkipped;
  }

  @Override
  public boolean readBoolean() throws IOException {
    byte in = readByte();
    return in != (byte) 0;
  }

  @Override
  public byte readByte() throws IOException {
    if (bytesAvailable() <= 0) {
      throw new EOFException("Read past end of blocks");
    }
    try {
      return block.getData()[position];
    } finally {
      position++;
      bytesRead++;
    }
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return readByte() & 0xff;
  }

  @Override
  public short readShort() throws IOException {
    byte[] buf = new byte[2];
    readFully(buf);
    short value = (short) ((buf[0] << 8) | (buf[1] & 0xff));
    return Short.reverseBytes(value);
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return readShort() & 0xffff;
  }

  @Override
  public char readChar() throws IOException {
    byte[] buf = new byte[2];
    readFully(buf);
    char value = (char) ((buf[0] << 8) | (buf[1] & 0xff));
    return Character.reverseBytes(value);
  }

  @Override
  public int readInt() throws IOException {
    byte[] buf = new byte[4];
    readFully(buf);
    int value = (((buf[0] & 0xff) << 24) | ((buf[1] & 0xff) << 16) |
        ((buf[2] & 0xff) << 8) | (buf[3] & 0xff));
    return Integer.reverseBytes(value);
  }

  @Override
  public long readLong() throws IOException {
    byte[] buf = new byte[8];
    readFully(buf);
    long value = (((long) (buf[0] & 0xff) << 56) |
        ((long) (buf[1] & 0xff) << 48) |
        ((long) (buf[2] & 0xff) << 40) |
        ((long) (buf[3] & 0xff) << 32) |
        ((long) (buf[4] & 0xff) << 24) |
        ((long) (buf[5] & 0xff) << 16) |
        ((long) (buf[6] & 0xff) << 8) |
        ((long) (buf[7] & 0xff)));
    return Long.reverseBytes(value);
  }

  @Override
  public float readFloat() throws IOException {
    int intValue = readInt();
    return Float.intBitsToFloat(intValue);
  }

  @Override
  public double readDouble() throws IOException {
    long longValue = readLong();
    return Double.longBitsToDouble(longValue);
  }

  @Override
  public String readLine() throws IOException {
    StringBuilder input = new StringBuilder();
    int c = -1;
    boolean eol = false;

    while (!eol) {
      if (bytesAvailable() <= 0) {
        eol = true;
        break;
      }
      c = readByte();
      switch (c) {
      case '\n':
        eol = true;
        break;
      case '\r':
        eol = true;
        if (bytesAvailable() <= 0) {
          break;
        }
        if (((char) block.getData()[position]) == '\n') {
          position++;
          bytesRead++;
        }
        break;
      default:
        input.append((char) c);
        break;
      }
    }

    if ((c == -1) && (input.length() == 0)) {
      return null;
    }
    return input.toString();
  }

  @Override
  public String readUTF() throws IOException {
    throw new UnsupportedOperationException();
  }

}
