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
package org.apache.hadoop.util;

import java.util.Date;
import java.util.Iterator;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/** Testing {@link LightWeightCache} */
public class TestLightWeightCache {
  private static final long starttime = Time.now();
  private static final long seed = starttime;
  private static final Random ran = new Random(seed);
  static {
    println("Start time = " + new Date(starttime) + ", seed=" +  seed);
  }

  private static void print(Object s) {
    System.out.print(s);
    System.out.flush();
  }

  private static void println(Object s) {
    System.out.println(s);
  }

  @Test
  public void testLightWeightCache() {
    // test randomized creation expiration with zero access expiration 
    {
      final long creationExpiration = ran.nextInt(1024) + 1;
      check(1, creationExpiration, 0L, 1 << 10, 65537);
      check(17, creationExpiration, 0L, 1 << 16, 17);
      check(255, creationExpiration, 0L, 1 << 16, 65537);
    }

    // test randomized creation/access expiration periods
    for(int i = 0; i < 3; i++) {
      final long creationExpiration = ran.nextInt(1024) + 1;
      final long accessExpiration = ran.nextInt(1024) + 1;
      
      check(1, creationExpiration, accessExpiration, 1 << 10, 65537);
      check(17, creationExpiration, accessExpiration, 1 << 16, 17);
      check(255, creationExpiration, accessExpiration, 1 << 16, 65537);
    }
  
    // test size limit
    final int dataSize = 1 << 16;
    for(int i = 0; i < 10; i++) {
      final int modulus = ran.nextInt(1024) + 1;
      final int sizeLimit = ran.nextInt(modulus) + 1;
      checkSizeLimit(sizeLimit, dataSize, modulus);
    }
  }

  private static void checkSizeLimit(final int sizeLimit, final int datasize,
      final int modulus) {
    final LightWeightCacheTestCase test = new LightWeightCacheTestCase(
        sizeLimit, sizeLimit, 1L << 32, 1L << 32, datasize, modulus);

    // keep putting entries and check size limit
    print("  check size ................. ");
    for(int i = 0; i < test.data.size(); i++) {
      test.cache.put(test.data.get(i));
      Assert.assertTrue(test.cache.size() <= sizeLimit);
    }
    println("DONE " + test.stat());
  }
  
  /** 
   * Test various createionExpirationPeriod and accessExpirationPeriod.
   * It runs ~2 minutes. If you are changing the implementation,
   * please un-comment the following line in order to run the test.
   */
//  @Test
  public void testExpirationPeriods() {
    for(int k = -4; k < 10; k += 4) {
      final long accessExpirationPeriod = k < 0? 0L: (1L << k); 
      for(int j = 0; j < 10; j += 4) {
        final long creationExpirationPeriod = 1L << j; 
        runTests(1, creationExpirationPeriod, accessExpirationPeriod);
        for(int i = 1; i < Integer.SIZE - 1; i += 8) {
          runTests((1 << i) + 1, creationExpirationPeriod, accessExpirationPeriod);
        }
      }
    }
  }

  /** Run tests with various table lengths. */
  private static void runTests(final int modulus,
      final long creationExpirationPeriod,
      final long accessExpirationPeriod) {
    println("\n\n\n*** runTest: modulus=" + modulus
        + ", creationExpirationPeriod=" + creationExpirationPeriod
        + ", accessExpirationPeriod=" + accessExpirationPeriod);
    for(int i = 0; i <= 16; i += 4) {
      final int tablelength = (1 << i);

      final int upper = i + 2;
      final int steps = Math.max(1, upper/3);

      for(int j = upper; j > 0; j -= steps) {
        final int datasize = 1 << j;
        check(tablelength, creationExpirationPeriod, accessExpirationPeriod,
            datasize, modulus);
      }
    }
  }

  private static void check(int tablelength, long creationExpirationPeriod,
      long accessExpirationPeriod, int datasize, int modulus) {
    check(new LightWeightCacheTestCase(tablelength, -1,
        creationExpirationPeriod, accessExpirationPeriod, datasize, modulus));
  }

  /** 
   * check the following operations
   * (1) put
   * (2) remove & put
   * (3) remove
   * (4) remove & put again
   */
  private static void check(final LightWeightCacheTestCase test) {
    //check put
    print("  check put .................. ");
    for(int i = 0; i < test.data.size()/2; i++) {
      test.put(test.data.get(i));
    }
    for(int i = 0; i < test.data.size(); i++) {
      test.put(test.data.get(i));
    }
    println("DONE " + test.stat());

    //check remove and put
    print("  check remove & put ......... ");
    for(int j = 0; j < 10; j++) {
      for(int i = 0; i < test.data.size()/2; i++) {
        final int r = ran.nextInt(test.data.size());
        test.remove(test.data.get(r));
      }
      for(int i = 0; i < test.data.size()/2; i++) {
        final int r = ran.nextInt(test.data.size());
        test.put(test.data.get(r));
      }
    }
    println("DONE " + test.stat());

    //check remove
    print("  check remove ............... ");
    for(int i = 0; i < test.data.size(); i++) {
      test.remove(test.data.get(i));
    }
    Assert.assertEquals(0, test.cache.size());
    println("DONE " + test.stat());

    //check remove and put again
    print("  check remove & put again ... ");
    for(int j = 0; j < 10; j++) {
      for(int i = 0; i < test.data.size()/2; i++) {
        final int r = ran.nextInt(test.data.size());
        test.remove(test.data.get(r));
      }
      for(int i = 0; i < test.data.size()/2; i++) {
        final int r = ran.nextInt(test.data.size());
        test.put(test.data.get(r));
      }
    }
    println("DONE " + test.stat());

    final long s = (Time.now() - starttime)/1000L;
    println("total time elapsed=" + s + "s\n");
  }

  /**
   * The test case contains two data structures, a cache and a hashMap.
   * The hashMap is used to verify the correctness of the cache.  Note that
   * no automatic eviction is performed in the hashMap.  Thus, we have
   * (1) If an entry exists in cache, it MUST exist in the hashMap.
   * (2) If an entry does not exist in the cache, it may or may not exist in the
   *     hashMap.  If it exists, it must be expired.
   */
  private static class LightWeightCacheTestCase implements GSet<IntEntry, IntEntry> {
    /** hashMap will not evict entries automatically. */
    final GSet<IntEntry, IntEntry> hashMap
        = new GSetByHashMap<IntEntry, IntEntry>(1024, 0.75f);

    final LightWeightCache<IntEntry, IntEntry> cache;
    final IntData data;

    final String info;
    final long starttime = Time.now();
    /** Determine the probability in {@link #check()}. */
    final int denominator;
    int iterate_count = 0;
    int contain_count = 0;

    private FakeTimer fakeTimer = new FakeTimer();

    LightWeightCacheTestCase(int tablelength, int sizeLimit,
        long creationExpirationPeriod, long accessExpirationPeriod,
        int datasize, int modulus) {
      denominator = Math.min((datasize >> 7) + 1, 1 << 16);
      info = getClass().getSimpleName() + "(" + new Date(starttime)
          + "): tablelength=" + tablelength
          + ", creationExpirationPeriod=" + creationExpirationPeriod
          + ", accessExpirationPeriod=" + accessExpirationPeriod
          + ", datasize=" + datasize
          + ", modulus=" + modulus
          + ", denominator=" + denominator;
      println(info);

      data = new IntData(datasize, modulus);
      cache = new LightWeightCache<IntEntry, IntEntry>(tablelength, sizeLimit,
          creationExpirationPeriod, 0, fakeTimer);

      Assert.assertEquals(0, cache.size());
    }

    private boolean containsTest(IntEntry key) {
      final boolean c = cache.contains(key);
      if (c) {
        Assert.assertTrue(hashMap.contains(key));
      } else {
        final IntEntry h = hashMap.remove(key);
        if (h != null) {
          Assert.assertTrue(cache.isExpired(h, fakeTimer.monotonicNowNanos()));
        }
      }
      return c;
    }
    @Override
    public boolean contains(IntEntry key) {
      final boolean e = containsTest(key);
      check();
      return e;
    }

    private IntEntry getTest(IntEntry key) {
      final IntEntry c = cache.get(key);
      if (c != null) {
        Assert.assertEquals(hashMap.get(key).id, c.id);
      } else {
        final IntEntry h = hashMap.remove(key);
        if (h != null) {
          Assert.assertTrue(cache.isExpired(h, fakeTimer.monotonicNowNanos()));
        }
      }
      return c;
    }
    @Override
    public IntEntry get(IntEntry key) {
      final IntEntry e = getTest(key);
      check();
      return e;
    }

    private IntEntry putTest(IntEntry entry) {
      final IntEntry c = cache.put(entry);
      if (c != null) {
        Assert.assertEquals(hashMap.put(entry).id, c.id);
      } else {
        final IntEntry h = hashMap.put(entry);
        if (h != null && h != entry) {
          // if h == entry, its expiration time is already updated
          Assert.assertTrue(cache.isExpired(h, fakeTimer.monotonicNowNanos()));
        }
      }
      return c;
    }
    @Override
    public IntEntry put(IntEntry entry) {
      final IntEntry e = putTest(entry);
      check();
      return e;
    }

    private IntEntry removeTest(IntEntry key) {
      final IntEntry c = cache.remove(key);
      if (c != null) {
        Assert.assertEquals(c.id, hashMap.remove(key).id);
      } else {
        final IntEntry h = hashMap.remove(key);
        if (h != null) {
          Assert.assertTrue(cache.isExpired(h, fakeTimer.monotonicNowNanos()));
        }
      }
      return c;
    }
    @Override
    public IntEntry remove(IntEntry key) {
      final IntEntry e = removeTest(key);
      check();
      return e;
    }

    private int sizeTest() {
      final int c = cache.size();
      Assert.assertTrue(hashMap.size() >= c);
      return c;
    }
    @Override
    public int size() {
      final int s = sizeTest();
      check();
      return s;
    }

    @Override
    public Iterator<IntEntry> iterator() {
      throw new UnsupportedOperationException();
    }

    boolean tossCoin() {
      return ran.nextInt(denominator) == 0;
    }

    void check() {
      fakeTimer.advanceNanos(ran.nextInt() & 0x3);

      //test size
      sizeTest();

      if (tossCoin()) {
        //test get(..), check content and test iterator
        iterate_count++;
        for(IntEntry i : cache) {
          getTest(i);
        }
      }

      if (tossCoin()) {
        //test contains(..)
        contain_count++;
        final int count = Math.min(data.size(), 1000);
        if (count == data.size()) {
          for(IntEntry i : data.integers) {
            containsTest(i);
          }
        } else {
          for(int j = 0; j < count; j++) {
            containsTest(data.get(ran.nextInt(data.size())));
          }
        }
      }
    }

    String stat() {
      final long t = Time.now() - starttime;
      return String.format(" iterate=%5d, contain=%5d, time elapsed=%5d.%03ds",
          iterate_count, contain_count, t/1000, t%1000);
    }

    @Override
    public void clear() {
      hashMap.clear();
      cache.clear();
      Assert.assertEquals(0, size());
    }
  }

  private static class IntData {
    final IntEntry[] integers;

    IntData(int size, int modulus) {
      integers = new IntEntry[size];
      for(int i = 0; i < integers.length; i++) {
        integers[i] = new IntEntry(i, ran.nextInt(modulus));
      }
    }

    IntEntry get(int i) {
      return integers[i];
    }

    int size() {
      return integers.length;
    }
  }

  /** Entries of {@link LightWeightCache} in this test */
  private static class IntEntry implements LightWeightCache.Entry,
      Comparable<IntEntry> {
    private LightWeightGSet.LinkedElement next;
    final int id;
    final int value;
    private long expirationTime = 0;

    IntEntry(int id, int value) {
      this.id = id;
      this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
      return obj != null && obj instanceof IntEntry
          && value == ((IntEntry)obj).value;
    }

    @Override
    public int hashCode() {
      return value;
    }

    @Override
    public int compareTo(IntEntry that) {
      return value - that.value;
    }

    @Override
    public String toString() {
      return id + "#" + value + ",expirationTime=" + expirationTime;
    }

    @Override
    public LightWeightGSet.LinkedElement getNext() {
      return next;
    }

    @Override
    public void setNext(LightWeightGSet.LinkedElement e) {
      next = e;
    }

    @Override
    public void setExpirationTime(long timeNano) {
      this.expirationTime = timeNano;
    }

    @Override
    public long getExpirationTime() {
      return expirationTime;
    }
  }
}
