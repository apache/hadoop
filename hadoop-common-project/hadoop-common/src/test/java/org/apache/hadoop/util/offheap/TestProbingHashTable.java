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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

public class TestProbingHashTable {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestProbingHashTable.class);

  @Before
  public void before() {
    GenericTestUtils.setLogLevel(ProbingHashTable.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(NativeMemoryManager.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(ByteArrayMemoryManager.LOG, Level.ALL);
  }

  private static class TestBlockId implements ProbingHashTable.Key {
    private static final HashFunction hashFunction = Hashing.goodFastHash(64);

    private final long id;

    TestBlockId(long id) {
      this.id = id;
    }

    @Override
    public long longHash() {
      return hashFunction.newHasher().putLong(id).hash().asLong();
    }

    @Override
    public boolean equals(Object o) {
      if (o.getClass() != this.getClass()) {
        return false;
      }
      return id == ((TestBlockId)o).id;
    }

    @Override
    public String toString() {
      return "TestBlockId(0x" + Long.toHexString(id) + ")";
    }

    // Just for use with java.util.HashMap
    @Override
    public int hashCode() {
      return (int)(id & 0xfffffff) ^ (int)((id >> 32) & 0xffffffff);
    }
  }

  private static class TestBlockInfo
        implements  ProbingHashTable.Entry<TestBlockId>, Closeable {
    /**
     * The memory manager to use.
     */
    private MemoryManager mman;

    /**
     * The address of the block reference.
     */
    private long addr;

    private static final long BLOCK_ID_OFF = 0;

    private static final long TOTAL_LEN = 8;

    static TestBlockInfo allocZeroed(MemoryManager mman) {
      long addr = mman.allocateZeroed(TOTAL_LEN);
      return new TestBlockInfo(mman, addr);
    }

    TestBlockInfo(MemoryManager mman, long addr) {
      this.mman = mman;
      this.addr = addr;
    }

    public long getBlockId() {
      return mman.getLong(addr + BLOCK_ID_OFF);
    }

    public void setBlockId(long blockId) {
      mman.putLong(addr + BLOCK_ID_OFF, blockId);
    }

    public void close() throws IOException {
      mman.free(addr);
    }

    @Override
    public boolean equals(Object o) {
      if (o.getClass() != TestBlockInfo.class) {
        return false;
      }
      TestBlockInfo other = (TestBlockInfo)o;
      return (other.getBlockId() == getBlockId());
    }

    @Override
    public TestBlockId getKey() {
      return new TestBlockId(getBlockId());
    }
  }

  private static class TestBlockInfoAdaptor
        implements ProbingHashTable.Adaptor<TestBlockInfo> {
    private final MemoryManager mman;

    TestBlockInfoAdaptor(MemoryManager mman) {
      this.mman = mman;
    }

    @Override
    public int getSlotSize() {
      return 8;
    }

    @Override
    public TestBlockInfo load(long addr) {
      long infoAddr = mman.getLong(addr);
      if (infoAddr == 0) {
        return null;
      }
      return new TestBlockInfo(mman, infoAddr);
    }

    @Override
    public void store(TestBlockInfo info, long addr) {
      mman.putLong(addr, info.addr);
    }

    @Override
    public void clear(long addr) {
      mman.putLong(addr, 0L);
    }
  }

  private void testAllocateAndFree(MemoryManager mman) throws Exception {
    TestBlockInfoAdaptor adaptor = new TestBlockInfoAdaptor(mman);
    ProbingHashTable<TestBlockId, TestBlockInfo> htable =
        new ProbingHashTable<TestBlockId, TestBlockInfo>(
            "testAllocateAndFreeTable", mman, adaptor, 100, 0.5f);
    // should have been rounded up to 256
    Assert.assertEquals(256, htable.numSlots());
    htable.close();
  }

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

  private static TestBlockInfo[] createBlockInfos(MemoryManager mman,
                                    int initialBlockId, int numBlocks) {
    TestBlockInfo infos[] = new TestBlockInfo[numBlocks];
    boolean success = false;
    try {
      for (int i = 0; i < numBlocks; i++) {
        infos[i] = TestBlockInfo.allocZeroed(mman);
        infos[i].setBlockId(initialBlockId + i);
        LOG.info("allocated infos[{}] with id {}", i, infos[i].getBlockId());
      }
      success = true;
      return infos;
    } finally {
      if (!success) {
        freeBlockInfos(infos);
      }
    }
  }

  private static void freeBlockInfos(TestBlockInfo[] infos) {
    if (infos != null) {
      for (int i = 0; i < infos.length; i++) {
        if (infos[i] != null) {
          IOUtils.cleanup(null, infos[i]);
        }
      }
    }
  }

  private void testAddRemove(MemoryManager mman) throws Exception {
    TestBlockInfoAdaptor adaptor = new TestBlockInfoAdaptor(mman);
    ProbingHashTable<TestBlockId, TestBlockInfo> htable =
        new ProbingHashTable<TestBlockId, TestBlockInfo>(
            "testAddRemoveTable", mman, adaptor, 10, 0.5f);
    TestBlockInfo infos[] = null;
    Assert.assertEquals(32, htable.numSlots());
    Assert.assertTrue(htable.isEmpty());
    Assert.assertEquals(0, htable.size());
    infos = createBlockInfos(mman, 1, 6);
    for (int i = 0; i < infos.length; i++) {
      LOG.info("Putting {} into {}", infos[i].getKey(), htable);
      TestBlockInfo prev = htable.putIfAbsent(infos[i]);
      Assert.assertEquals(null, prev);
    }
    Assert.assertFalse(htable.isEmpty());
    Assert.assertEquals(infos.length, htable.size());

    // Test that we can iterate over all elements in the hash table.
    Iterator<TestBlockId> iter = htable.iterator();
    Assert.assertNotNull(iter);
    HashSet<TestBlockId> contents = new HashSet<TestBlockId>();
    for (TestBlockInfo info : infos) {
      contents.add(info.getKey());
    }
    for (int i = 0; i < infos.length; i++) {
      Assert.assertTrue(iter.hasNext());
      TestBlockId blockId = iter.next();
      Assert.assertTrue("Iterator returned " + blockId + ", which was " +
          "not inserted into the HashTable.", contents.remove(blockId));
    }
    Assert.assertFalse(iter.hasNext());
    Assert.assertEquals("Did not find " + contents.size() + " entries " +
        "from the hash table during iteration.", 0, contents.size());

    for (int i = 0; i < infos.length; i++) {
      LOG.info("Removing {} from {}", infos[i].getKey(), htable);
      TestBlockInfo prev = htable.remove(infos[i].getKey());
      Assert.assertNotNull("unable to remove " + infos[i].getKey() +
          " from the ProbingHashTable.", prev);
    }
    Assert.assertTrue(htable.isEmpty());
    freeBlockInfos(infos);
    htable.close();
  }

  @Test(timeout=60000)
  public void testAddRemoveOnHeap() throws Exception {
    ByteArrayMemoryManager mman = new ByteArrayMemoryManager("test");
    testAddRemove(mman);
    mman.close();
  }

  @Test(timeout=60000)
  public void testAddRemoveOffHeap() throws Exception {
    Assume.assumeTrue(NativeMemoryManager.isAvailable());
    NativeMemoryManager mman = new NativeMemoryManager("test");
    testAddRemove(mman);
    mman.close();
  }

  private void testEnlargeHashTable(MemoryManager mman) throws Exception {
    TestBlockInfoAdaptor adaptor = new TestBlockInfoAdaptor(mman);
    ProbingHashTable<TestBlockId, TestBlockInfo> htable =
        new ProbingHashTable<TestBlockId, TestBlockInfo>(
            "testEnlargeHashTable", mman, adaptor, 4, 0.5f);
    TestBlockInfo infos[] = null;
    Assert.assertEquals(8, htable.numSlots());
    Assert.assertTrue(htable.isEmpty());
    Assert.assertEquals(0, htable.size());
    infos = createBlockInfos(mman, 1, 33);
    for (int i = 0; i < 4; i++) {
      LOG.info("Putting {} into {}", infos[i].getKey(), htable);
      TestBlockInfo prev = htable.putIfAbsent(infos[i]);
      Assert.assertEquals(null, prev);
    }
    Assert.assertEquals(8, htable.numSlots());
    Assert.assertFalse(htable.isEmpty());
    Assert.assertEquals(4, htable.size());
    for (int i = 4; i < 8; i++) {
      LOG.info("Putting {} into {}", infos[i].getKey(), htable);
      TestBlockInfo prev = htable.putIfAbsent(infos[i]);
      Assert.assertEquals(null, prev);
    }
    Assert.assertEquals(16, htable.numSlots());
    Assert.assertFalse(htable.isEmpty());
    Assert.assertEquals(8, htable.size());

    for (int i = 8; i < 16; i++) {
      LOG.info("Putting {} into {}", infos[i].getKey(), htable);
      TestBlockInfo prev = htable.putIfAbsent(infos[i]);
      Assert.assertEquals(null, prev);
    }
    Assert.assertEquals(32, htable.numSlots());
    Assert.assertFalse(htable.isEmpty());
    Assert.assertEquals(16, htable.size());

    for (int i = 16; i < infos.length; i++) {
      LOG.info("Putting {} into {}", infos[i].getKey(), htable);
      TestBlockInfo prev = htable.putIfAbsent(infos[i]);
      Assert.assertEquals(null, prev);
    }
    Assert.assertEquals(64, htable.numSlots());
    Assert.assertFalse(htable.isEmpty());
    Assert.assertEquals(33, htable.size());

    // Delete every other element
    for (int i = 0; i < infos.length; i+=2) {
      LOG.info("Removing {} from {}", infos[i].getKey(), htable);
      TestBlockInfo prev = htable.remove(infos[i].getKey());
      Assert.assertNotNull("unable to remove " + infos[i].getKey() +
          " from the ProbingHashTable.", prev);
    }
    Assert.assertEquals(64, htable.numSlots());
    Assert.assertFalse(htable.isEmpty());
    Assert.assertEquals(16, htable.size());

    // Test that we can iterate over all remaining elements in the hash set.
    Iterator<TestBlockId> iter = htable.iterator();
    Assert.assertNotNull(iter);
    HashSet<TestBlockId> contents = new HashSet<TestBlockId>();
    for (int i = 1; i < infos.length; i+=2) {
      contents.add(infos[i].getKey());
    }
    for (int i = 1; i < infos.length; i+=2) {
      Assert.assertTrue(iter.hasNext());
      TestBlockId blockId = iter.next();
      Assert.assertTrue("Iterator returned " + blockId + ", which was " +
          "not inserted into the HashTable.", contents.remove(blockId));
    }
    Assert.assertFalse(iter.hasNext());
    Assert.assertEquals("Did not find " + contents.size() + " entries " +
        "from the hash table during iteration.", 0, contents.size());

    // Delete remaining elements
    for (int i = 1; i < infos.length; i+=2) {
      LOG.info("Removing {} from {}", infos[i].getKey(), htable);
      TestBlockInfo prev = htable.remove(infos[i].getKey());
      Assert.assertNotNull("unable to remove " + infos[i].getKey() +
          " from the ProbingHashTable.", prev);
    }
    Assert.assertEquals(64, htable.numSlots());
    Assert.assertTrue(htable.isEmpty());
    Assert.assertEquals(0, htable.size());

    iter = htable.iterator();
    Assert.assertNotNull(iter);
    Assert.assertFalse(iter.hasNext());

    freeBlockInfos(infos);
    htable.close();
  }

  @Test(timeout=60000)
  public void testEnlargeHashTableOnHeap() throws Exception {
    ByteArrayMemoryManager mman = new ByteArrayMemoryManager("test");
    testEnlargeHashTable(mman);
    mman.close();
  }

  @Test(timeout=60000)
  public void testEnlargeHashTableOffHeap() throws Exception {
    Assume.assumeTrue(NativeMemoryManager.isAvailable());
    NativeMemoryManager mman = new NativeMemoryManager("test");
    testEnlargeHashTable(mman);
    mman.close();
  }
}
