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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;

import org.junit.Test;

public class TestDirectBufferPool {

  @Test
  public void testGetInstance() throws Exception {
    final int num = 100;
    List<DirectBufferPool> pools = new ArrayList<DirectBufferPool>();
    Thread[] list = new Thread[num];
    for (int i = 0; i < num; i++)  {
      Thread t = getPoolThread(pools);
      t.start();
      list[i] = t;
    }
    for (int i = 0; i < num; i++) {
      try {
        list[i].join(10000);
      } catch (Exception e) {
        e.printStackTrace(); 
      }
    }
    DirectBufferPool p1 = pools.get(0);
    assertNotNull(p1);
    for (int i = 1; i < pools.size(); i++) {
      DirectBufferPool p2 = pools.get(i);
      assertNotNull(p2);
      assertSame(p1, p2);
    }
  }

  private Thread getPoolThread(final List<DirectBufferPool> pools) {
    Thread t = new Thread() {
      public void run() {
        pools.add(DirectBufferPool.getInstance());
      }
    };
    return t;
  }


  @Test
  public void testBufBorrow() throws IOException {
    final DirectBufferPool bufferPool = DirectBufferPool.getInstance();
    ByteBuffer b1 = bufferPool.borrowBuffer(100);
    assertTrue(b1.isDirect());
    assertEquals(0, b1.position());
    assertEquals(100, b1.capacity());
    bufferPool.returnBuffer(b1);
    ByteBuffer b2 = bufferPool.borrowBuffer(100);
    assertTrue(b2.isDirect());
    assertEquals(0, b2.position());
    assertEquals(100, b2.capacity());
    assertSame(b1, b2);

    ByteBuffer b3 =  bufferPool.borrowBuffer(100);
    assertTrue(b3.isDirect());
    assertEquals(0, b3.position());
    assertEquals(100, b3.capacity());
    assertNotSame(b2, b3);
    bufferPool.returnBuffer(b2);
    bufferPool.returnBuffer(b3);
  }

  @Test
  public void testBufReset() throws IOException {
    final DirectBufferPool bufferPool = DirectBufferPool.getInstance();
    ByteBuffer b1 = bufferPool.borrowBuffer(100);
    assertTrue(b1.isDirect());
    assertEquals(0, b1.position());
    assertEquals(100, b1.capacity());
    b1.putInt(1);
    assertEquals(4, b1.position());
    bufferPool.returnBuffer(b1);
    ByteBuffer b2 = bufferPool.borrowBuffer(100);
    assertSame(b1, b2);
    assertTrue(b2.isDirect());
    assertEquals(0, b2.position());
    assertEquals(100, b2.capacity());
  }

  @Test
  public void testBufReturn() throws IOException {
    final DirectBufferPool bufferPool = DirectBufferPool.getInstance();
    int numOfBufs = 100;
    int capacity = 100;
    final ByteBuffer[] bufs = new ByteBuffer[numOfBufs];
    for (int i = 0; i < numOfBufs; i++) {
      bufs[i] = bufferPool.borrowBuffer(capacity);
    }

    assertEquals(0, bufferPool.getBufCountsForCapacity(capacity));


    int numOfThreads = numOfBufs;
    Thread[] list = new Thread[numOfThreads];
    for (int i = 0; i < numOfThreads; i++) {
      Thread t = retBufThread(bufferPool, bufs, i);
      t.start();
      list[i] = t;
    }
    for (int i = 0; i < numOfThreads; i++) {
      try {
        list[i].join(10000);
      } catch (Exception e) {
       e.printStackTrace();
      }
    }

    assertEquals(numOfBufs, bufferPool.getBufCountsForCapacity(capacity));
  }

  private Thread retBufThread(final DirectBufferPool bufferPool, final ByteBuffer[] bufs, final int i) {
       Thread t = new Thread(new Runnable(){
        @Override
        public void run() {
          try {
          bufferPool.returnBuffer(bufs[i]);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    return t;
  }

  @Test
  public void testBufException() {
    final DirectBufferPool bufferPool = DirectBufferPool.getInstance();
    boolean thrown = false;
    try {
      bufferPool.returnBuffer(null);
    } catch (IOException e) {
      thrown = true;
    }
    assertEquals(true, thrown);

    thrown = false;
    ByteBuffer buf = ByteBuffer.allocate(100);
    try {
      bufferPool.returnBuffer(buf);
    } catch (IOException e) {
      thrown = true;
    }
    assertEquals(true, thrown);
  }

  @Test
  public void testBufWeakRefClear() throws IOException {
    final DirectBufferPool bufferPool = DirectBufferPool.getInstance();
    int numOfBufs = 100;
    int capacity = 100;
    ByteBuffer[] list = new ByteBuffer[capacity];
    for (int i = 0; i < numOfBufs; i++) {
      list[i] = bufferPool.borrowBuffer(capacity);
    }
    for (int i = 0; i < numOfBufs; i++) {
      bufferPool.returnBuffer(list[i]);
      list[i] = null;
    }

    assertEquals(numOfBufs, bufferPool.getBufCountsForCapacity(capacity));

    for (int i = 0; i < 3; i++) {
      System.gc();
    }

    ByteBuffer b = bufferPool.borrowBuffer(capacity);
    assertEquals(0, bufferPool.getBufCountsForCapacity(capacity));
  }
}
