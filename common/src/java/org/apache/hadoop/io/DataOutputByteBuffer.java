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

package org.apache.hadoop.io;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;

public class DataOutputByteBuffer extends DataOutputStream {

   static class Buffer extends OutputStream {

    final byte[] b = new byte[1];
    final boolean direct;
    final List<ByteBuffer> active = new ArrayList<ByteBuffer>();
    final List<ByteBuffer> inactive = new LinkedList<ByteBuffer>();
    int size;
    int length;
    ByteBuffer current;

    Buffer(int size, boolean direct) {
      this.direct = direct;
      this.size = size;
      current = direct
          ? ByteBuffer.allocateDirect(size)
          : ByteBuffer.allocate(size);
    }
    @Override
    public void write(int b) {
      this.b[0] = (byte)(b & 0xFF);
      write(this.b);
    }
    @Override
    public void write(byte[] b) {
      write(b, 0, b.length);
    }
    @Override
    public void write(byte[] b, int off, int len) {
      int rem = current.remaining();
      while (len > rem) {
        current.put(b, off, rem);
        length += rem;
        current.flip();
        active.add(current);
        off += rem;
        len -= rem;
        rem = getBuffer(len);
      }
      current.put(b, off, len);
      length += len;
    }
    int getBuffer(int newsize) {
      if (inactive.isEmpty()) {
        size = Math.max(size << 1, newsize);
        current = direct
            ? ByteBuffer.allocateDirect(size)
            : ByteBuffer.allocate(size);
      } else {
        current = inactive.remove(0);
      }
      return current.remaining();
    }
    ByteBuffer[] getData() {
      ByteBuffer[] ret = active.toArray(new ByteBuffer[active.size() + 1]);
      ByteBuffer tmp = current.duplicate();
      tmp.flip();
      ret[ret.length - 1] = tmp.slice();
      return ret;
    }
    int getLength() {
      return length;
    }
    void reset() {
      length = 0;
      current.rewind();
      inactive.add(0, current);
      for (int i = active.size() - 1; i >= 0; --i) {
        ByteBuffer b = active.remove(i);
        b.rewind();
        inactive.add(0, b);
      }
      current = inactive.remove(0);
    }
  }

  private final Buffer buffers;

  public DataOutputByteBuffer() {
    this(32);
  }

  public DataOutputByteBuffer(int size) {
    this(size, false);
  }

  public DataOutputByteBuffer(int size, boolean direct) {
    this(new Buffer(size, direct));
  }

  private DataOutputByteBuffer(Buffer buffers) {
    super(buffers);
    this.buffers = buffers;
  }

  public ByteBuffer[] getData() {
    return buffers.getData();
  }

  public int getLength() {
    return buffers.getLength();
  }

  public void reset() {
    this.written = 0;
    buffers.reset();
  }

}
