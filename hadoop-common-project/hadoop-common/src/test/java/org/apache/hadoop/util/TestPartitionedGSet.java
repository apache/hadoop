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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.util.LightWeightGSet.LinkedElement;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Testing {@link PartitionedGSet} */
public class TestPartitionedGSet {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestPartitionedGSet.class);
  private static final int ELEMENT_NUM = 100;

  /**
   * Generate positive random numbers for testing. We want to use only positive
   * numbers because the smallest partition used in testing is 0.
   *
   * @param length
   *    number of random numbers to be generated.
   *
   * @param randomSeed
   *    seed to be used for random number generator.
   *
   * @return
   *    An array of Integers
   */
  private static ArrayList<Integer> getRandomList(int length, int randomSeed) {
    Random random = new Random(randomSeed);
    ArrayList<Integer> list = new ArrayList<Integer>(length);
    for (int i = 0; i < length; i++) {
      list.add(random.nextInt(Integer.MAX_VALUE));
    }
    return list;
  }

  private static class TestElement implements LinkedElement {
    private final int val;
    private LinkedElement next;

    TestElement(int val) {
      this.val = val;
      this.next = null;
    }

    public int getVal() {
      return val;
    }

    @Override
    public void setNext(LinkedElement next) {
      this.next = next;
    }

    @Override
    public LinkedElement getNext() {
      return next;
    }
  }

  private static class TestElementComparator implements Comparator<TestElement>
  {
    @Override
    public int compare(TestElement e1, TestElement e2) {
      if (e1 == null || e2 == null) {
        throw new NullPointerException("Cannot compare null elements");
      }

      return e1.getVal() - e2.getVal();
    }
  }

  protected ReentrantReadWriteLock topLock =
      new ReentrantReadWriteLock(false);
  /**
   * We are NOT testing any concurrent access to a PartitionedGSet here.
   */
  private class NoOpLock extends LatchLock<ReentrantReadWriteLock> {
    private ReentrantReadWriteLock childLock;

    public NoOpLock() {
      childLock = new ReentrantReadWriteLock(false);
    }

    @Override
    protected boolean isReadTopLocked() {
      return topLock.getReadLockCount() > 0 || isWriteTopLocked();
    }

    @Override
    protected boolean isWriteTopLocked() {
      return topLock.isWriteLocked();
    }

    @Override
    protected void readTopUnlock() {
      topLock.readLock().unlock();
    }

    @Override
    protected void writeTopUnlock() {
      topLock.writeLock().unlock();
    }

    @Override
    protected boolean hasReadChildLock() {
      return childLock.getReadLockCount() > 0 || hasWriteChildLock();
    }

    @Override
    protected void readChildLock() {
      childLock.readLock().lock();
    }

    @Override
    protected void readChildUnlock() {
      childLock.readLock().unlock();
    }

    @Override
    protected boolean hasWriteChildLock() {
      return childLock.isWriteLockedByCurrentThread();
    }

    @Override
    protected void writeChildLock() {
      childLock.writeLock().lock();
    }

    @Override
    protected void writeChildUnlock() {
      childLock.writeLock().unlock();
    }

    @Override
    protected LatchLock<ReentrantReadWriteLock> clone() {
      return new NoOpLock();
    }
  }

  /**
   * Test iterator for a PartitionedGSet with no partitions.
   */
  @Test(timeout=60000)
  public void testIteratorForNoPartition() {
    PartitionedGSet<TestElement, TestElement> set =
        new PartitionedGSet<TestElement, TestElement>(
            16, new TestElementComparator(), new NoOpLock());

    topLock.readLock().lock();
    int count = 0;
    Iterator<TestElement> iter = set.iterator();
    while( iter.hasNext() ) {
      iter.next();
      count ++;
    }
    topLock.readLock().unlock();
    Assert.assertEquals(0, count);
  }

  /**
   * Test iterator for a PartitionedGSet with empty partitions.
   */
  @Test(timeout=60000)
  public void testIteratorForEmptyPartitions() {
    PartitionedGSet<TestElement, TestElement> set =
        new PartitionedGSet<TestElement, TestElement>(
            16, new TestElementComparator(), new NoOpLock());

    set.addNewPartition(new TestElement(0));
    set.addNewPartition(new TestElement(1000));
    set.addNewPartition(new TestElement(2000));

    topLock.readLock().lock();
    int count = 0;
    Iterator<TestElement> iter = set.iterator();
    while( iter.hasNext() ) {
      iter.next();
      count ++;
    }
    topLock.readLock().unlock();
    Assert.assertEquals(0, count);
  }

  /**
   * Test whether the iterator can return the same number of elements as stored
   * into the PartitionedGSet.
   */
  @Test(timeout=60000)
  public void testIteratorCountElements() {
    ArrayList<Integer> list = getRandomList(ELEMENT_NUM, 123);
    PartitionedGSet<TestElement, TestElement> set =
        new PartitionedGSet<TestElement, TestElement>(
            16, new TestElementComparator(), new NoOpLock());

    set.addNewPartition(new TestElement(0));
    set.addNewPartition(new TestElement(1000));
    set.addNewPartition(new TestElement(2000));

    topLock.writeLock().lock();
    for (Integer i : list) {
      set.put(new TestElement(i));
    }
    topLock.writeLock().unlock();

    topLock.readLock().lock();
    int count = 0;
    Iterator<TestElement> iter = set.iterator();
    while( iter.hasNext() ) {
      iter.next();
      count ++;
    }
    topLock.readLock().unlock();
    Assert.assertEquals(ELEMENT_NUM, count);
  }

  /**
   * Test iterator when it is created before partitions/elements are
   * added to the PartitionedGSet.
   */
  @Test(timeout=60000)
  public void testIteratorAddElementsAfterIteratorCreation() {
    PartitionedGSet<TestElement, TestElement> set =
        new PartitionedGSet<TestElement, TestElement>(
            16, new TestElementComparator(), new NoOpLock());

    // Create the iterator before partitions are added.
    Iterator<TestElement> iter = set.iterator();

    set.addNewPartition(new TestElement(0));
    set.addNewPartition(new TestElement(1000));
    set.addNewPartition(new TestElement(2000));

    // Added one element
    topLock.writeLock().lock();
    set.put(new TestElement(2500));
    topLock.writeLock().unlock();

    topLock.readLock().lock();
    int count = 0;
    while( iter.hasNext() ) {
      iter.next();
      count ++;
    }
    topLock.readLock().unlock();
    Assert.assertEquals(1, count);
  }
}
