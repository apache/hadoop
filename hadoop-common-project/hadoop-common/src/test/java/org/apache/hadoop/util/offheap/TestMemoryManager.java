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

package org.apache.hadoop.util.offheap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMemoryManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMemoryManager.class);

  @Test(timeout=60000)
  public void testAllocateAndFreeOnHeap() throws Exception {
    ByteArrayMemoryManager mman = new ByteArrayMemoryManager("test");
    testAllocateAndFree(mman);
    mman.close();
  }

  @Test(timeout=60000)
  public void testAllocateAndFreeOffHeap() throws Exception {
    Assume.assumeTrue(NativeMemoryManager.isAvailable());
    NativeMemoryManager mman = new NativeMemoryManager("test");
    testAllocateAndFree(mman);
    mman.close();
  }

  private void testAllocateAndFree(MemoryManager mman) throws Exception {
    long addr = mman.allocate(100);
    Assert.assertTrue("Expected addr to be non-zero.", addr != 0);
    mman.free(addr);
  }

  @Test(timeout=60000)
  public void testGetAndSetOnHeap() throws Exception {
    ByteArrayMemoryManager mman = new ByteArrayMemoryManager("test");
    testGetAndSet(mman);
    mman.close();
  }

  @Test(timeout=60000)
  public void testGetAndSetOffHeap() throws Exception {
    Assume.assumeTrue(NativeMemoryManager.isAvailable());
    NativeMemoryManager mman = new NativeMemoryManager("test");
    testGetAndSet(mman);
    mman.close();
  }

  private void testGetAndSet(MemoryManager mman) throws Exception {
    LOG.info("testingGetAndSet with " + mman.getClass().getCanonicalName());
    long byteAddr = mman.allocateZeroed(1);
    Assert.assertTrue("Expected addr to be non-zero.", byteAddr != 0);
    byte b = mman.getByte(byteAddr);
    Assert.assertEquals((byte)0, b);
    mman.putByte(byteAddr, (byte) 42);
    b = mman.getByte(byteAddr);
    Assert.assertEquals((byte)42, b);

    long intAddr = mman.allocateZeroed(4);
    Assert.assertTrue("Expected addr to be non-zero.", intAddr != 0);
    int i = mman.getInt(intAddr);
    Assert.assertEquals(0, i);
    mman.putInt(intAddr, 0xfea01234);
    i = mman.getInt(intAddr);
    Assert.assertEquals(0xfea01234, i);

    long shortAddr = mman.allocateZeroed(2);
    Assert.assertTrue("Expected addr to be non-zero.", shortAddr != 0);
    short s = mman.getShort(shortAddr);
    Assert.assertEquals(0, s);
    mman.putShort(shortAddr, (short) 0xeecc);
    s = mman.getShort(shortAddr);
    Assert.assertEquals((short)0xeecc, s);

    long longAddr = mman.allocateZeroed(8);
    Assert.assertTrue("Expected addr to be non-zero.", longAddr != 0);
    long l = mman.getLong(longAddr);
    Assert.assertEquals(0, l);
    long testVal = 0x3ea0123400112233L;
    LOG.info("longAddr = " + longAddr + ", testVal = "  + testVal);
    mman.putLong(longAddr, testVal);
    l = mman.getLong(longAddr);
    LOG.info("got back " + l + " from " + longAddr);
    Assert.assertEquals(testVal, l);

    mman.free(byteAddr);
    mman.free(intAddr);
    mman.free(shortAddr);
    mman.free(longAddr);
  }

  @Test(timeout=60000)
  public void testCatchInvalidPuts() throws Exception {
    ByteArrayMemoryManager mman = new ByteArrayMemoryManager("test");
    long addr = mman.allocate(1);
    mman.putByte(addr, (byte)1); // should succeed
    try {
      mman.putInt(addr, 0xdeadbeef);
      Assert.fail("expected to catch invalid put");
    } catch (RuntimeException e) {
    }
    try {
      mman.putByte(addr + 1, (byte) 1);
      Assert.fail("expected to catch invalid put");
    } catch (RuntimeException e) {
    }
    try {
      mman.putLong(addr, 11111111111L);
      Assert.fail("expected to catch invalid put");
    } catch (RuntimeException e) {
    }
    mman.free(addr);
    try {
      mman.putByte(addr, (byte)1);
      Assert.fail("expected to catch invalid put");
    } catch (RuntimeException e) {
    }
    try {
      mman.putShort(addr, (short) 101);
      Assert.fail("expected to catch invalid put");
    } catch (RuntimeException e) {
    }
    try {
      mman.putInt(addr + 1, 0xfaceface);
      Assert.fail("expected to catch invalid put");
    } catch (RuntimeException e) {
    }
    try {
      mman.putLong(addr, 0xf00L);
      Assert.fail("expected to catch invalid put");
    } catch (RuntimeException e) {
    }
    mman.close();
  }

  private void testMemoryManagerCreate(
      String className, String createdClassName) throws Exception {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_MEMORY_MANAGER_KEY, className);
    MemoryManager mman = MemoryManager.Factory.create("test", conf);
    Assert.assertNotNull(mman);
    Assert.assertEquals(createdClassName, mman.getClass().getCanonicalName());
    mman.close();
  }

  @Test(timeout=60000)
  public void testByteBufferMemoryManagerCreate() throws Exception {
    testMemoryManagerCreate(
        "org.apache.hadoop.util.offheap.ByteArrayMemoryManager",
        "org.apache.hadoop.util.offheap.ByteArrayMemoryManager");
  }

  @Test(timeout=60000)
  public void testNativeMemoryManagerCreate() throws Exception {
    Assume.assumeTrue(NativeMemoryManager.isAvailable());
    testMemoryManagerCreate(
        "org.apache.hadoop.util.offheap.NativeMemoryManager",
        "org.apache.hadoop.util.offheap.NativeMemoryManager");
  }

  @Test(timeout=60000)
  public void testDefaultMemoryManagerCreate() throws Exception {
    testMemoryManagerCreate(
        "org.apache.hadoop.util.offheap.NonExistentMemoryManager",
        "org.apache.hadoop.util.offheap.ByteArrayMemoryManager");
  }

  @Test(timeout=60000)
  public void testByteBufferMemoryDirtyClose() throws Exception {
    ByteArrayMemoryManager mman = new ByteArrayMemoryManager("test");
    long addr = mman.allocate(1);
    try {
      mman.close();
      Assert.fail("expected close to fail since we did not free all " +
          "allocations first.");
    } catch (RuntimeException e) {
      GenericTestUtils.assertExceptionContains("There are still unfreed " +
          "buffers", e);
    }
  }
}
