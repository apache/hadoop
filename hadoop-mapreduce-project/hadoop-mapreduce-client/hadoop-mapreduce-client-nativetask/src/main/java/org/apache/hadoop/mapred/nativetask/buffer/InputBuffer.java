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

import org.apache.hadoop.util.DirectBufferPool;
import org.apache.hadoop.classification.InterfaceAudience;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@InterfaceAudience.Private
public class InputBuffer implements Closeable {

  static DirectBufferPool bufferPool = new DirectBufferPool();

  private ByteBuffer byteBuffer;
  private final BufferType type;

  public InputBuffer(BufferType type, int inputSize) throws IOException {

    final int capacity = inputSize;
    this.type = type;

    if (capacity > 0) {

      switch (type) {
      case DIRECT_BUFFER:
        this.byteBuffer = bufferPool.getBuffer(capacity);
        this.byteBuffer.order(ByteOrder.BIG_ENDIAN);
        break;
      case HEAP_BUFFER:
        this.byteBuffer = ByteBuffer.allocate(capacity);
        this.byteBuffer.order(ByteOrder.BIG_ENDIAN);
        break;
      }
      byteBuffer.position(0);
      byteBuffer.limit(0);
    }
  }

  public BufferType getType() {
    return this.type;
  }

  public InputBuffer(byte[] bytes) {
    this.type = BufferType.HEAP_BUFFER;
    if (bytes.length > 0) {
      this.byteBuffer = ByteBuffer.wrap(bytes);
      this.byteBuffer.order(ByteOrder.BIG_ENDIAN);
      byteBuffer.position(0);
      byteBuffer.limit(0);
    }
  }

  public ByteBuffer getByteBuffer() {
    return this.byteBuffer;
  }

  public int length() {
    if (null == byteBuffer) {
      return 0;
    }
    return byteBuffer.limit();
  }

  public void rewind(int startOffset, int length) {
    if (null == byteBuffer) {
      return;
    }
    byteBuffer.position(startOffset);
    byteBuffer.limit(length);
  }

  public int remaining() {
    if (null == byteBuffer) {
      return 0;
    }
    return byteBuffer.remaining();
  }

  public int position() {
    if (null == byteBuffer) {
      return 0;
    }
    return byteBuffer.position();
  }

  public int position(int pos) {
    if (null == byteBuffer) {
      return 0;
    }

    byteBuffer.position(pos);
    return pos;
  }

  public int capacity() {
    if (null == byteBuffer) {
      return 0;
    }
    return byteBuffer.capacity();
  }

  public byte[] array() {
    if (null == byteBuffer) {
      return null;
    }
    return byteBuffer.array();
  }

  @Override
  public void close() {
    if (byteBuffer != null && byteBuffer.isDirect()) {
      bufferPool.returnBuffer(byteBuffer);
      byteBuffer = null;
    }
  }
}
