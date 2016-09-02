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

package org.apache.hadoop.ipc;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
/** generates byte-length framed buffers. */
public class ResponseBuffer extends DataOutputStream {

  public ResponseBuffer() {
    this(1024);
  }

  public ResponseBuffer(int capacity) {
    super(new FramedBuffer(capacity));
  }

  // update framing bytes based on bytes written to stream.
  private FramedBuffer getFramedBuffer() {
    FramedBuffer buf = (FramedBuffer)out;
    buf.setSize(written);
    return buf;
  }

  public void writeTo(OutputStream out) throws IOException {
    getFramedBuffer().writeTo(out);
  }

  byte[] toByteArray() {
    return getFramedBuffer().toByteArray();
  }

  int capacity() {
    return ((FramedBuffer)out).capacity();
  }

  void setCapacity(int capacity) {
    ((FramedBuffer)out).setCapacity(capacity);
  }

  void ensureCapacity(int capacity) {
    if (((FramedBuffer)out).capacity() < capacity) {
      ((FramedBuffer)out).setCapacity(capacity);
    }
  }

  ResponseBuffer reset() {
    written = 0;
    ((FramedBuffer)out).reset();
    return this;
  }

  private static class FramedBuffer extends ByteArrayOutputStream {
    private static final int FRAMING_BYTES = 4;
    FramedBuffer(int capacity) {
      super(capacity + FRAMING_BYTES);
      reset();
    }
    @Override
    public int size() {
      return count - FRAMING_BYTES;
    }
    void setSize(int size) {
      buf[0] = (byte)((size >>> 24) & 0xFF);
      buf[1] = (byte)((size >>> 16) & 0xFF);
      buf[2] = (byte)((size >>>  8) & 0xFF);
      buf[3] = (byte)((size >>>  0) & 0xFF);
    }
    int capacity() {
      return buf.length - FRAMING_BYTES;
    }
    void setCapacity(int capacity) {
      buf = Arrays.copyOf(buf, capacity + FRAMING_BYTES);
    }
    @Override
    public void reset() {
      count = FRAMING_BYTES;
      setSize(0);
    }
  };
}