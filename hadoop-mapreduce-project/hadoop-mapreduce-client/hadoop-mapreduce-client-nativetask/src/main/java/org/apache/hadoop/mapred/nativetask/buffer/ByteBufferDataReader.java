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
package org.apache.hadoop.mapred.nativetask.buffer;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;

/**
 * read data from a input buffer
 */
public class ByteBufferDataReader extends DataInputStream {
  private ByteBuffer byteBuffer;
  private char lineCache[];

  public ByteBufferDataReader(InputBuffer buffer) {
    if (buffer != null) {
      this.byteBuffer = buffer.getByteBuffer();
    }
  }

  public void reset(InputBuffer buffer) {
    this.byteBuffer = buffer.getByteBuffer();
  }

  @Override
  public int read() throws IOException {
    return byteBuffer.get();
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    byteBuffer.get(b, off, len);
    return len;
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    byteBuffer.get(b, 0, b.length);
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    byteBuffer.get(b, off, len);
  }

  @Override
  public int skipBytes(int n) throws IOException {
    final int remains = byteBuffer.remaining();
    final int skip = (remains < n) ? remains : n;
    final int current = byteBuffer.position();
    byteBuffer.position(current + skip);
    return skip;
  }

  @Override
  public boolean readBoolean() throws IOException {
    return (byteBuffer.get() == 1) ? true : false;
  }

  @Override
  public byte readByte() throws IOException {
    return byteBuffer.get();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    final int ch = byteBuffer.get();
    if (ch < 0) {
      throw new EOFException();
    }
    return ch;
  }

  @Override
  public short readShort() throws IOException {
    return byteBuffer.getShort();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return byteBuffer.getShort();
  }

  @Override
  public char readChar() throws IOException {
    return byteBuffer.getChar();
  }

  @Override
  public int readInt() throws IOException {
    return byteBuffer.getInt();
  }

  @Override
  public long readLong() throws IOException {
    return byteBuffer.getLong();
  }

  @Override
  public float readFloat() throws IOException {
    return byteBuffer.getFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return byteBuffer.getDouble();
  }

  @Override
  public String readLine() throws IOException {

    InputStream in = this;

    char buf[] = lineCache;

    if (buf == null) {
      buf = lineCache = new char[128];
    }

    int room = buf.length;
    int offset = 0;
    int c;

    loop: while (true) {
      switch (c = in.read()) {
      case -1:
      case '\n':
        break loop;

      case '\r':
        final int c2 = in.read();
        if ((c2 != '\n') && (c2 != -1)) {
          if (!(in instanceof PushbackInputStream)) {
            in = new PushbackInputStream(in);
          }
          ((PushbackInputStream) in).unread(c2);
        }
        break loop;

      default:
        if (--room < 0) {
          buf = new char[offset + 128];
          room = buf.length - offset - 1;
          System.arraycopy(lineCache, 0, buf, 0, offset);
          lineCache = buf;
        }
        buf[offset++] = (char) c;
        break;
      }
    }
    if ((c == -1) && (offset == 0)) {
      return null;
    }
    return String.copyValueOf(buf, 0, offset);
  }

  @Override
  public final String readUTF() throws IOException {
    return readUTF(this);
  }

  private final static String readUTF(DataInput in) throws IOException {
    final int utflen = in.readUnsignedShort();
    byte[] bytearr = null;
    char[] chararr = null;

    bytearr = new byte[utflen];
    chararr = new char[utflen];

    int c, char2, char3;
    int count = 0;
    int chararr_count = 0;

    in.readFully(bytearr, 0, utflen);

    while (count < utflen) {
      c = bytearr[count] & 0xff;
      if (c > 127) {
        break;
      }
      count++;
      chararr[chararr_count++] = (char) c;
    }

    while (count < utflen) {
      c = bytearr[count] & 0xff;
      switch (c >> 4) {
      case 0:
      case 1:
      case 2:
      case 3:
      case 4:
      case 5:
      case 6:
      case 7:
        /* 0xxxxxxx */
        count++;
        chararr[chararr_count++] = (char) c;
        break;
      case 12:
      case 13:
        /* 110x xxxx 10xx xxxx */
        count += 2;
        if (count > utflen) {
          throw new UTFDataFormatException("malformed input: partial character at end");
        }
        char2 = bytearr[count - 1];
        if ((char2 & 0xC0) != 0x80) {
          throw new UTFDataFormatException("malformed input around byte " + count);
        }
        chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
        break;
      case 14:
        /* 1110 xxxx 10xx xxxx 10xx xxxx */
        count += 3;
        if (count > utflen) {
          throw new UTFDataFormatException("malformed input: partial character at end");
        }
        char2 = bytearr[count - 2];
        char3 = bytearr[count - 1];
        if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
          throw new UTFDataFormatException("malformed input around byte " + (count - 1));
        }
        chararr[chararr_count++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
        break;
      default:
        /* 10xx xxxx, 1111 xxxx */
        throw new UTFDataFormatException("malformed input around byte " + count);
      }
    }
    // The number of chars produced may be less than utflen
    return new String(chararr, 0, chararr_count);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public boolean hasUnReadData() {
    return null != byteBuffer && byteBuffer.hasRemaining();
  }
}
