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

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;

import org.apache.hadoop.mapred.nativetask.NativeDataTarget;

/**
 * write data to a output buffer
 */
public class ByteBufferDataWriter extends DataOutputStream {
  private ByteBuffer buffer;
  private final NativeDataTarget target;

  private void checkSizeAndFlushNecessary(int length) throws IOException {
    if (buffer.position() > 0 && buffer.remaining() < length) {
      flush();
    }
  }

  public ByteBufferDataWriter(NativeDataTarget handler) {
    if (null != handler) {
      this.buffer = handler.getOutputBuffer().getByteBuffer();
    }
    this.target = handler;
  }

  @Override
  public synchronized void write(int v) throws IOException {
    checkSizeAndFlushNecessary(1);
    buffer.put((byte) v);
  }

  @Override
  public boolean shortOfSpace(int dataLength) throws IOException {
    if (buffer.remaining() < dataLength) {
      return true;
    }
    return false;
  }

  @Override
  public synchronized void write(byte b[], int off, int len) throws IOException {
    int remain = len;
    int offset = off;
    while (remain > 0) {
      int currentFlush = 0;
      if (buffer.remaining() > 0) {
        currentFlush = Math.min(buffer.remaining(), remain);
        buffer.put(b, offset, currentFlush);
        remain -= currentFlush;
        offset += currentFlush;
      } else {
        flush();
      }
    }
  }

  @Override
  public void flush() throws IOException {
    target.sendData();
    buffer.position(0);
  }

  @Override
  public void close() throws IOException {
    if (hasUnFlushedData()) {
      flush();
    }
    target.finishSendData();
  }

  private final static byte TRUE = (byte) 1;
  private final static byte FALSE = (byte) 0;

  @Override
  public final void writeBoolean(boolean v) throws IOException {
    checkSizeAndFlushNecessary(1);
    buffer.put(v ? TRUE : FALSE);
  }

  @Override
  public final void writeByte(int v) throws IOException {
    checkSizeAndFlushNecessary(1);
    buffer.put((byte) v);
  }

  @Override
  public final void writeShort(int v) throws IOException {
    checkSizeAndFlushNecessary(2);
    buffer.putShort((short) v);
  }

  @Override
  public final void writeChar(int v) throws IOException {
    checkSizeAndFlushNecessary(2);
    buffer.put((byte) ((v >>> 8) & 0xFF));
    buffer.put((byte) ((v >>> 0) & 0xFF));
  }

  @Override
  public final void writeInt(int v) throws IOException {
    checkSizeAndFlushNecessary(4);
    buffer.putInt(v);
  }

  @Override
  public final void writeLong(long v) throws IOException {
    checkSizeAndFlushNecessary(8);
    buffer.putLong(v);
  }

  @Override
  public final void writeFloat(float v) throws IOException {
    checkSizeAndFlushNecessary(4);
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public final void writeDouble(double v) throws IOException {
    checkSizeAndFlushNecessary(8);
    writeLong(Double.doubleToLongBits(v));
  }

  @Override
  public final void writeBytes(String s) throws IOException {
    final int len = s.length();

    int remain = len;
    int offset = 0;
    while (remain > 0) {
      int currentFlush = 0;
      if (buffer.remaining() > 0) {
        currentFlush = Math.min(buffer.remaining(), remain);

        for (int i = 0; i < currentFlush; i++) {
          buffer.put((byte) s.charAt(offset + i));
        }

        remain -= currentFlush;
        offset += currentFlush;
      } else {
        flush();
      }
    }
  }

  @Override
  public final void writeChars(String s) throws IOException {
    final int len = s.length();

    int remain = len;
    int offset = 0;

    while (remain > 0) {
      int currentFlush = 0;
      if (buffer.remaining() > 2) {
        currentFlush = Math.min(buffer.remaining() / 2, remain);

        for (int i = 0; i < currentFlush; i++) {
          buffer.putChar(s.charAt(offset + i));
        }

        remain -= currentFlush;
        offset += currentFlush;
      } else {
        flush();
      }
    }
  }

  @Override
  public final void writeUTF(String str) throws IOException {
    writeUTF(str, this);
  }

  private int writeUTF(String str, DataOutput out) throws IOException {
    final int strlen = str.length();
    int utflen = 0;
    int c, count = 0;

    /* use charAt instead of copying String to char array */
    for (int i = 0; i < strlen; i++) {
      c = str.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        utflen++;
      } else if (c > 0x07FF) {
        utflen += 3;
      } else {
        utflen += 2;
      }
    }

    if (utflen > 65535) {
      throw new UTFDataFormatException("encoded string too long: " + utflen + " bytes");
    }

    final byte[] bytearr = new byte[utflen + 2];
    bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
    bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);

    int i = 0;
    for (i = 0; i < strlen; i++) {
      c = str.charAt(i);
      if (!((c >= 0x0001) && (c <= 0x007F))) {
        break;
      }
      bytearr[count++] = (byte) c;
    }

    for (; i < strlen; i++) {
      c = str.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        bytearr[count++] = (byte) c;

      } else if (c > 0x07FF) {
        bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
        bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
        bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
      } else {
        bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
        bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
      }
    }
    write(bytearr, 0, utflen + 2);
    return utflen + 2;
  }

  @Override
  public boolean hasUnFlushedData() {
    return !(buffer.position() == 0);
  }
}
