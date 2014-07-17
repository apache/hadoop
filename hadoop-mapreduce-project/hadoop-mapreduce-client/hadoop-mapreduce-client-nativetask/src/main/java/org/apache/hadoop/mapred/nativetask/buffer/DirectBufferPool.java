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
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * as direct buffer memory is not collected by GC, we keep a pool
 * to reuse direct buffers
 */
public class DirectBufferPool {
  
  private static DirectBufferPool directBufferPool = null;
  private static Log LOG = LogFactory.getLog(DirectBufferPool.class);
  private ConcurrentMap<Integer, Queue<WeakReference<ByteBuffer>>> bufferMap = new ConcurrentHashMap<Integer, Queue<WeakReference<ByteBuffer>>>();

  private DirectBufferPool() {
  }
  
  public static synchronized DirectBufferPool getInstance() {
    if (null == directBufferPool) {
      directBufferPool = new DirectBufferPool();
    }
    return directBufferPool;
  }
  
  public static void destoryInstance(){
    directBufferPool = null;
  }
  
  public synchronized ByteBuffer borrowBuffer(int capacity) throws IOException {
    Queue<WeakReference<ByteBuffer>> list = bufferMap.get(capacity);
    if (null == list) {
      return ByteBuffer.allocateDirect(capacity);
    }
    WeakReference<ByteBuffer> ref;
    while ((ref = list.poll()) != null) {
      ByteBuffer buf = ref.get();
      if (buf != null) {
        return buf;
      }
    }
    return ByteBuffer.allocateDirect(capacity);
  }
  
  public void returnBuffer(ByteBuffer buffer) throws IOException {
    if (null == buffer || !buffer.isDirect()) {
      throw new IOException("the buffer is null or the buffer returned is not direct buffer");
    }

    buffer.clear();
    int capacity = buffer.capacity();
    Queue<WeakReference<ByteBuffer>> list = bufferMap.get(capacity);
    if (null == list) {
      list = new ConcurrentLinkedQueue<WeakReference<ByteBuffer>>();
      Queue<WeakReference<ByteBuffer>> prev = bufferMap.putIfAbsent(capacity, list);
      if (prev != null) {
        list = prev;
      }
    }
    list.add(new WeakReference<ByteBuffer>(buffer));
  }

  int getBufCountsForCapacity(int capacity) {
    return bufferMap.get(capacity).size();
  }

}
