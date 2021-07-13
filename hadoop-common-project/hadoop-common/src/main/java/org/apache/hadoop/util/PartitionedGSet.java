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

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.NoSuchElementException;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.LightWeightGSet.LinkedElement;

/**
 * An implementation of {@link GSet}, which splits a collection of elements
 * into partitions each corresponding to a range of keys.
 *
 * This class does not support null element.
 *
 * This class is backed up by LatchLock for hierarchical synchronization.
 *
 * @param <K> Key type for looking up the elements
 * @param <E> Element type, which must be
 *       (1) a subclass of K, and
 *       (2) implementing {@link LinkedElement} interface.
 */
@InterfaceAudience.Private
public class PartitionedGSet<K, E extends K> implements GSet<K, E> {

  private static final int DEFAULT_PARTITION_CAPACITY = 65536; // 4096; // 5120; // 2048; // 1027;
  private static final float DEFAULT_PARTITION_OVERFLOW = 1.8f;

  /**
   * An ordered map of contiguous segments of elements.
   * Each key in the map represent the smallest key in the mapped segment,
   * so that all elements in this segment are >= the mapping key,
   * but are smaller then the next key in the map.
   * Elements within a partition do not need to be ordered.
   */
  private final NavigableMap<K, PartitionEntry> partitions;
  private LatchLock<?> latchLock;

  /**
   * The number of elements in the set.
   */
  protected volatile int size;

  /**
   * A single partition of the {@link PartitionedGSet}.
   * Consists of a hash table {@link LightWeightGSet} and a lock, which
   * controls access to this partition independently on the other ones.
   */
  private class PartitionEntry extends LightWeightGSet<K, E> {
    private final LatchLock<?> partLock;

    PartitionEntry(int defaultPartitionCapacity) {
      super(defaultPartitionCapacity);
      this.partLock = latchLock.clone();
    }
  }

  public PartitionedGSet(final int capacity,
      final Comparator<? super K> comparator,
      final LatchLock<?> latchLock) {
    this.partitions = new TreeMap<K, PartitionEntry>(comparator);
    this.latchLock = latchLock;
    // addNewPartition(rootKey).put(rootKey);
    // this.size = 1;
    this.size = 0;
    LOG.info("Partition capacity = {}", DEFAULT_PARTITION_CAPACITY);
    LOG.info("Partition overflow factor = {}", DEFAULT_PARTITION_OVERFLOW);
  }

  /**
   * Creates new empty partition.
   * @param key
   * @return
   */
  public PartitionEntry addNewPartition(final K key) {
    Entry<K, PartitionEntry> lastEntry = partitions.lastEntry();
    PartitionEntry lastPart = null;
    if(lastEntry != null)
      lastPart = lastEntry.getValue();

    PartitionEntry newPart =
        new PartitionEntry(DEFAULT_PARTITION_CAPACITY);
    // assert size == 0 || newPart.partLock.isWriteTopLocked() :
    //      "Must hold write Lock: key = " + key;
    PartitionEntry oldPart = partitions.put(key, newPart);
    assert oldPart == null :
      "RangeMap already has a partition associated with " + key;

    LOG.debug("Total GSet size = {}", size);
    LOG.debug("Number of partitions = {}", partitions.size());
    LOG.debug("Previous partition size = {}",
        lastPart == null ? 0 : lastPart.size());

    return newPart;
  }

  @Override
  public int size() {
    return size;
  }

  protected PartitionEntry getPartition(final K key) {
    Entry<K, PartitionEntry> partEntry = partitions.floorEntry(key);
    if(partEntry == null) {
      return null;
    }
    PartitionEntry part = partEntry.getValue();
    if(part == null) {
      throw new IllegalStateException("Null partition for key: " + key);
    }
    assert size == 0 || part.partLock.isReadTopLocked() ||
        part.partLock.hasReadChildLock() : "Must hold read Lock: key = " + key;
    return part;
  }

  @Override
  public boolean contains(final K key) {
    PartitionEntry part = getPartition(key);
    if(part == null) {
      return false;
    }
    return part.contains(key);
  }

  @Override
  public E get(final K key) {
    PartitionEntry part = getPartition(key);
    if(part == null) {
      return null;
    }
    LOG.debug("get key: {}", key);
    // part.partLock.readLock();
    return part.get(key);
  }

  @Override
  public E put(final E element) {
    K key = element;
    PartitionEntry part = getPartition(key);
    if(part == null) {
      throw new HadoopIllegalArgumentException("Illegal key: " + key);
    }
    assert size == 0 || part.partLock.isWriteTopLocked() ||
        part.partLock.hasWriteChildLock() :
          "Must hold write Lock: key = " + key;
    LOG.debug("put key: {}", key);
    PartitionEntry newPart = addNewPartitionIfNeeded(part, key);
    if(newPart != part) {
      newPart.partLock.writeChildLock();
      part = newPart;
    }
    E result = part.put(element);
    if(result == null) {  // new element
      size++;
    }
    return result;
  }

  private PartitionEntry addNewPartitionIfNeeded(
      PartitionEntry curPart, K key) {
    if(curPart.size() < DEFAULT_PARTITION_CAPACITY * DEFAULT_PARTITION_OVERFLOW
        || curPart.contains(key)) {
      return curPart;
    }
    return addNewPartition(key);
  }

  @Override
  public E remove(final K key) {
    PartitionEntry part = getPartition(key);
    if(part == null) {
      return null;
    }
    E result = part.remove(key);
    if(result != null) {
      size--;
    }
    return result;
  }

  @Override
  public void clear() {
    LOG.error("Total GSet size = {}", size);
    LOG.error("Number of partitions = {}", partitions.size());
    printStats();
    // assert latchLock.hasWriteTopLock() : "Must hold write topLock";
    // SHV May need to clear all partitions?
    partitions.clear();
    size = 0;
  }

  private void printStats() {
    int partSizeMin = Integer.MAX_VALUE, partSizeAvg = 0, partSizeMax = 0;
    long totalSize = 0;
    int numEmptyPartitions = 0, numFullPartitions = 0;
    Collection<PartitionEntry> parts = partitions.values();
    Set<Entry<K, PartitionEntry>> entries = partitions.entrySet();
    int i = 0;
    for(Entry<K, PartitionEntry> e : entries) {
      PartitionEntry part = e.getValue();
      int s = part.size;
      if(s == 0) numEmptyPartitions++;
      if(s > DEFAULT_PARTITION_CAPACITY) numFullPartitions++;
      totalSize += s;
      partSizeMin = (s < partSizeMin ? s : partSizeMin);
      partSizeMax = (partSizeMax < s ? s : partSizeMax);
      Class<?> inodeClass = e.getKey().getClass();
      try {
        long[] key = (long[]) inodeClass.
            getMethod("getNamespaceKey", int.class).invoke(e.getKey(), 2);
        long[] firstKey = new long[0];
        if(part.iterator().hasNext()) {
          Object first = part.iterator().next();
          firstKey = (long[]) inodeClass.getMethod(
            "getNamespaceKey", int.class).invoke(first, 2);
          Object parent = inodeClass.
              getMethod("getParent").invoke(first);
          long parentId = (parent == null ? 0L :
            (long) inodeClass.getMethod("getId").invoke(parent));
          firstKey[0] = parentId;
        }
        LOG.error("Partition #{}\t key: {}\t size: {}\t first: {}",
            i++, key, s, firstKey);  // SHV should be info
      } catch (Exception ex) {
        LOG.error("Cannot find Method getNamespaceKey() in {}", inodeClass);
      }
    }
    partSizeAvg = (int) (totalSize / parts.size());
    LOG.error("Partition sizes: min = {}, avg = {}, max = {}, sum = {}",
        partSizeMin, partSizeAvg, partSizeMax, totalSize);
    LOG.error("Number of partitions: empty = {}, full = {}",
        numEmptyPartitions, numFullPartitions);
  }

  @Override
  public Collection<E> values() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterator<E> iterator() {
    return new EntryIterator();
  }

  /**
   * Iterator over the elements in the set.
   * Iterates first by keys, then inside the partition
   * corresponding to the key.
   *
   * Modifications are tracked by the underlying collections. We allow
   * modifying other partitions, while iterating through the current one.
   */
  private class EntryIterator implements Iterator<E> {
    private Iterator<K> keyIterator;
    private Iterator<E> partitionIterator;

    public EntryIterator() {
      keyIterator = partitions.keySet().iterator();
 
      if (!keyIterator.hasNext()) {
        partitionIterator = null;
        return;
      }

      K firstKey = keyIterator.next();
      partitionIterator = partitions.get(firstKey).iterator();
    }

    @Override
    public boolean hasNext() {

      // Special case: an iterator was created for an empty PartitionedGSet.
      // Check whether new partitions have been added since then.
      if (partitionIterator == null) {
        if (partitions.size() == 0)
          return false;
        else {
          keyIterator = partitions.keySet().iterator();
          K nextKey = keyIterator.next();
          partitionIterator = partitions.get(nextKey).iterator();
        }
      }

      while(!partitionIterator.hasNext()) {
        if(!keyIterator.hasNext()) {
          return false;
        }
        K curKey = keyIterator.next();
        partitionIterator = getPartition(curKey).iterator();
      }
      return partitionIterator.hasNext();
    }

    @Override
    public E next() {
      if (!hasNext())
        throw new NoSuchElementException("No more elements in this set.");
      return partitionIterator.next();
    }
  }

  public void latchWriteLock(K[] keys) {
    // getPartition(parent).partLock.writeChildLock();
    LatchLock<?> pLock = null;
    for(K key : keys) {
      pLock = getPartition(key).partLock;
      pLock.writeChildLock();
    }
    assert pLock != null : "pLock is null";
    pLock.writeTopUnlock();
  }
}
