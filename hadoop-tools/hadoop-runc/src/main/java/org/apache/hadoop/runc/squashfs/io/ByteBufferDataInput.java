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

package org.apache.hadoop.runc.squashfs.io;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public class ByteBufferDataInput implements DataInput {
  private final ByteBuffer bb;
  private final byte[] tb = new byte[8];

  public ByteBufferDataInput(ByteBuffer buffer) {
    this.bb = buffer;
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    try {
      bb.get(b);
    } catch (BufferUnderflowException e) {
      throw new EOFException();
    }
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    try {
      bb.get(b, off, len);
    } catch (BufferUnderflowException e) {
      throw new EOFException();
    }
  }

  @Override
  public byte readByte() throws IOException {
    try {
      return bb.get();
    } catch (BufferUnderflowException e) {
      throw new EOFException();
    }
  }

  @Override
  public int skipBytes(int n) throws IOException {
    int bytesToSkip = Math.max(0, Math.min(n, bb.remaining()));
    bb.position(bb.position() + bytesToSkip);
    return bytesToSkip;
  }

  @Override
  public boolean readBoolean() throws IOException {
    byte in = readByte();
    return in != (byte) 0;
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return readByte() & 0xff;
  }

  @Override
  public short readShort() throws IOException {
    readFully(tb, 0, 2);
    return (short) ((tb[0] << 8) | (tb[1] & 0xff));
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return readShort() & 0xffff;
  }

  @Override
  public char readChar() throws IOException {
    readFully(tb, 0, 2);
    return (char) ((tb[0] << 8) | (tb[1] & 0xff));
  }

  @Override
  public int readInt() throws IOException {
    readFully(tb, 0, 4);
    return (((tb[0] & 0xff) << 24) | ((tb[1] & 0xff) << 16) |
        ((tb[2] & 0xff) << 8) | (tb[3] & 0xff));
  }

  @Override
  public long readLong() throws IOException {
    readFully(tb, 0, 8);
    return (((long) (tb[0] & 0xff) << 56) |
        ((long) (tb[1] & 0xff) << 48) |
        ((long) (tb[2] & 0xff) << 40) |
        ((long) (tb[3] & 0xff) << 32) |
        ((long) (tb[4] & 0xff) << 24) |
        ((long) (tb[5] & 0xff) << 16) |
        ((long) (tb[6] & 0xff) << 8) |
        ((long) (tb[7] & 0xff)));
  }

  @Override
  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());
  }

  @Override
  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());
  }

  @Override
  public String readLine() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String readUTF() throws IOException {
    throw new UnsupportedOperationException();
  }

}
