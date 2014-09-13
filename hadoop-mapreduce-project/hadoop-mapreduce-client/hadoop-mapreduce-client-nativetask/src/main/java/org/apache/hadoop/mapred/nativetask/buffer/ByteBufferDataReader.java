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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * read data from a input buffer
 */
@InterfaceAudience.Private
public class ByteBufferDataReader extends DataInputStream {
  private ByteBuffer byteBuffer;
  private java.io.DataInputStream javaReader;

  public ByteBufferDataReader(InputBuffer buffer) {
    if (buffer != null) {
      reset(buffer);
    }
    javaReader = new java.io.DataInputStream(this);
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

  @SuppressWarnings("deprecation")
  @Override
  public String readLine() throws IOException {
    return javaReader.readLine();
  }

  @Override
  public final String readUTF() throws IOException {
    return javaReader.readUTF();
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
