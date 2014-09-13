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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.nativetask.NativeDataTarget;

import com.google.common.base.Preconditions;

/**
 * DataOutputStream implementation which buffers data in a fixed-size
 * ByteBuffer.
 * When the byte buffer has filled up, synchronously passes the buffer
 * to a downstream NativeDataTarget.
 */
@InterfaceAudience.Private
public class ByteBufferDataWriter extends DataOutputStream {
  private final ByteBuffer buffer;
  private final NativeDataTarget target;

  private final static byte TRUE = (byte) 1;
  private final static byte FALSE = (byte) 0;
  private final java.io.DataOutputStream javaWriter;

  private void checkSizeAndFlushIfNecessary(int length) throws IOException {
    if (buffer.position() > 0 && buffer.remaining() < length) {
      flush();
    }
  }

  public ByteBufferDataWriter(NativeDataTarget handler) {
    Preconditions.checkNotNull(handler);
    this.buffer = handler.getOutputBuffer().getByteBuffer();
    this.target = handler;
    this.javaWriter = new java.io.DataOutputStream(this);
  }

  @Override
  public synchronized void write(int v) throws IOException {
    checkSizeAndFlushIfNecessary(1);
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

  @Override
  public final void writeBoolean(boolean v) throws IOException {
    checkSizeAndFlushIfNecessary(1);
    buffer.put(v ? TRUE : FALSE);
  }

  @Override
  public final void writeByte(int v) throws IOException {
    checkSizeAndFlushIfNecessary(1);
    buffer.put((byte) v);
  }

  @Override
  public final void writeShort(int v) throws IOException {
    checkSizeAndFlushIfNecessary(2);
    buffer.putShort((short) v);
  }

  @Override
  public final void writeChar(int v) throws IOException {
    checkSizeAndFlushIfNecessary(2);
    buffer.put((byte) ((v >>> 8) & 0xFF));
    buffer.put((byte) ((v >>> 0) & 0xFF));
  }

  @Override
  public final void writeInt(int v) throws IOException {
    checkSizeAndFlushIfNecessary(4);
    buffer.putInt(v);
  }

  @Override
  public final void writeLong(long v) throws IOException {
    checkSizeAndFlushIfNecessary(8);
    buffer.putLong(v);
  }

  @Override
  public final void writeFloat(float v) throws IOException {
    checkSizeAndFlushIfNecessary(4);
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public final void writeDouble(double v) throws IOException {
    checkSizeAndFlushIfNecessary(8);
    writeLong(Double.doubleToLongBits(v));
  }

  @Override
  public final void writeBytes(String s) throws IOException {
    javaWriter.writeBytes(s);
  }

  @Override
  public final void writeChars(String s) throws IOException {
    javaWriter.writeChars(s);
  }

  @Override
  public final void writeUTF(String str) throws IOException {
    javaWriter.writeUTF(str);
  }

  @Override
  public boolean hasUnFlushedData() {
    return buffer.position() > 0;
  }
}
