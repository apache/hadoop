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
import java.util.TreeMap;

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

  private static final int DEFAULT_PARTITION_CAPACITY = 2027;

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
      final LatchLock<?> latchLock,
      final E rootKey) {
    this.partitions = new TreeMap<K, PartitionEntry>(comparator);
    this.latchLock = latchLock;
    addNewPartition(rootKey).put(rootKey);
    this.size = 1;
  }

  /**
   * Creates new empty partition.
   * @param key
   * @return
   */
  private PartitionEntry addNewPartition(final K key) {
    PartitionEntry lastPart = null;
    if(size > 0)
      lastPart = partitions.lastEntry().getValue();

    PartitionEntry newPart =
        new PartitionEntry(DEFAULT_PARTITION_CAPACITY);
    // assert size == 0 || newPart.partLock.isWriteTopLocked() :
    //      "Must hold write Lock: key = " + key;
    partitions.put(key, newPart);

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
    if(curPart.size() < DEFAULT_PARTITION_CAPACITY * 1.1
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
    // assert latchLock.hasWriteTopLock() : "Must hold write topLock";
    // SHV May need to clear all partitions?
    partitions.clear();
    size = 0;
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
    private final Iterator<K> keyIterator;
    private Iterator<E> partitionIterator;

    public EntryIterator() {
      keyIterator = partitions.keySet().iterator();
      K curKey = partitions.firstKey();
      partitionIterator = getPartition(curKey).iterator();
    }

    @Override
    public boolean hasNext() {
      if(partitionIterator.hasNext()) {
        return true;
      }
      return keyIterator.hasNext();
    }

    @Override
    public E next() {
      if(!partitionIterator.hasNext()) {
        K curKey = keyIterator.next();
        partitionIterator = getPartition(curKey).iterator();
      }
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
