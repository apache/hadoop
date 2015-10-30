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
package org.apache.hadoop.io.erasurecode;


import java.nio.ByteBuffer;

/**
 * An abstract buffer allocator used for test.
 */
public abstract class BufferAllocator {
  private boolean usingDirect = false;

  public BufferAllocator(boolean usingDirect) {
    this.usingDirect = usingDirect;
  }

  protected boolean isUsingDirect() {
    return usingDirect;
  }

  /**
   * Allocate and return a ByteBuffer of specified length.
   * @param bufferLen
   * @return
   */
  public abstract ByteBuffer allocate(int bufferLen);

  /**
   * A simple buffer allocator that just uses ByteBuffer's
   * allocate/allocateDirect API.
   */
  public static class SimpleBufferAllocator extends BufferAllocator {

    public SimpleBufferAllocator(boolean usingDirect) {
      super(usingDirect);
    }

    @Override
    public ByteBuffer allocate(int bufferLen) {
      return isUsingDirect() ? ByteBuffer.allocateDirect(bufferLen) :
          ByteBuffer.allocate(bufferLen);
    }
  }

  /**
   * A buffer allocator that allocates a buffer from an existing large buffer by
   * slice calling, but if no available space just degrades as
   * SimpleBufferAllocator. So please ensure enough space for it.
   */
  public static class SlicedBufferAllocator extends BufferAllocator {
    private ByteBuffer overallBuffer;

    public SlicedBufferAllocator(boolean usingDirect, int totalBufferLen) {
      super(usingDirect);
      overallBuffer = isUsingDirect() ?
          ByteBuffer.allocateDirect(totalBufferLen) :
          ByteBuffer.allocate(totalBufferLen);
    }

    @Override
    public ByteBuffer allocate(int bufferLen) {
      if (bufferLen > overallBuffer.capacity() - overallBuffer.position()) {
        // If no available space for the requested length, then allocate new
        return isUsingDirect() ? ByteBuffer.allocateDirect(bufferLen) :
            ByteBuffer.allocate(bufferLen);
      }

      overallBuffer.limit(overallBuffer.position() + bufferLen);
      ByteBuffer result = overallBuffer.slice();
      overallBuffer.position(overallBuffer.position() + bufferLen);
      return result;
    }
  }

}
