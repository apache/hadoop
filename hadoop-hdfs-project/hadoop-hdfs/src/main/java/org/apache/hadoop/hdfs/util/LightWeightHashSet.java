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
package org.apache.hadoop.hdfs.util;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A low memory linked hash set implementation, which uses an array for storing
 * the elements and linked lists for collision resolution. This class does not
 * support null element.
 *
 * This class is not thread safe.
 *
 */
public class LightWeightHashSet<T> implements Collection<T> {
  /**
   * Elements of {@link LightWeightLinkedSet}.
   */
  static class LinkedElement<T> {
    protected final T element;

    // reference to the next entry within a bucket linked list
    protected LinkedElement<T> next;

    //hashCode of the element
    protected final int hashCode;

    public LinkedElement(T elem, int hash) {
      this.element = elem;
      this.next = null;
      this.hashCode = hash;
    }

    @Override
    public String toString() {
      return element.toString();
    }
  }

  protected static final float DEFAULT_MAX_LOAD_FACTOR = 0.75f;
  protected static final float DEFAUT_MIN_LOAD_FACTOR = 0.2f;
  protected static final int MINIMUM_CAPACITY = 16;

  static final int MAXIMUM_CAPACITY = 1 << 30;
  private static final Log LOG = LogFactory.getLog(LightWeightHashSet.class);

  /**
   * An internal array of entries, which are the rows of the hash table. The
   * size must be a power of two.
   */
  protected LinkedElement<T>[] entries;
  /** Size of the entry table. */
  private int capacity;
  /** The size of the set (not the entry array). */
  protected int size = 0;
  /** Hashmask used for determining the bucket index **/
  private int hash_mask;
  /** Capacity at initialization time **/
  private final int initialCapacity;

  /**
   * Modification version for fail-fast.
   *
   * @see ConcurrentModificationException
   */
  protected int modification = 0;

  private float maxLoadFactor;
  private float minLoadFactor;
  private final int expandMultiplier = 2;

  private int expandThreshold;
  private int shrinkThreshold;

  /**
   * @param initCapacity
   *          Recommended size of the internal array.
   * @param maxLoadFactor
   *          used to determine when to expand the internal array
   * @param minLoadFactor
   *          used to determine when to shrink the internal array
   */
  @SuppressWarnings("unchecked")
  public LightWeightHashSet(int initCapacity, float maxLoadFactor,
      float minLoadFactor) {

    if (maxLoadFactor <= 0 || maxLoadFactor > 1.0f)
      throw new IllegalArgumentException("Illegal maxload factor: "
          + maxLoadFactor);

    if (minLoadFactor <= 0 || minLoadFactor > maxLoadFactor)
      throw new IllegalArgumentException("Illegal minload factor: "
          + minLoadFactor);

    this.initialCapacity = computeCapacity(initCapacity);
    this.capacity = this.initialCapacity;
    this.hash_mask = capacity - 1;

    this.maxLoadFactor = maxLoadFactor;
    this.expandThreshold = (int) (capacity * maxLoadFactor);
    this.minLoadFactor = minLoadFactor;
    this.shrinkThreshold = (int) (capacity * minLoadFactor);

    entries = new LinkedElement[capacity];
    LOG.debug("initial capacity=" + initialCapacity + ", max load factor= "
        + maxLoadFactor + ", min load factor= " + minLoadFactor);
  }

  public LightWeightHashSet() {
    this(MINIMUM_CAPACITY, DEFAULT_MAX_LOAD_FACTOR, DEFAUT_MIN_LOAD_FACTOR);
  }

  public LightWeightHashSet(int minCapacity) {
    this(minCapacity, DEFAULT_MAX_LOAD_FACTOR, DEFAUT_MIN_LOAD_FACTOR);
  }

  /**
   * Check if the set is empty.
   *
   * @return true is set empty, false otherwise
   */
  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Return the current capacity (for testing).
   */
  public int getCapacity() {
    return capacity;
  }

  /**
   * Return the number of stored elements.
   */
  @Override
  public int size() {
    return size;
  }

  /**
   * Get index in the internal table for a given hash.
   */
  protected int getIndex(int hashCode) {
    return hashCode & hash_mask;
  }

  /**
   * Check if the set contains given element
   *
   * @return true if element present, false otherwise.
   */
  @SuppressWarnings("unchecked")
  @Override
  public boolean contains(final Object key) {
    return getElement((T)key) != null;
  }
  
  /**
   * Return the element in this set which is equal to
   * the given key, if such an element exists.
   * Otherwise returns null.
   */
  public T getElement(final T key) {
    // validate key
    if (key == null) {
      throw new IllegalArgumentException("Null element is not supported.");
    }
    // find element
    final int hashCode = key.hashCode();
    final int index = getIndex(hashCode);
    return getContainedElem(index, key, hashCode);
  }

  /**
   * Check if the set contains given element at given index. If it
   * does, return that element.
   *
   * @return the element, or null, if no element matches
   */
  protected T getContainedElem(int index, final T key, int hashCode) {
    for (LinkedElement<T> e = entries[index]; e != null; e = e.next) {
      // element found
      if (hashCode == e.hashCode && e.element.equals(key)) {
        return e.element;
      }
    }
    // element not found
    return null;
  }

  /**
   * All all elements in the collection. Expand if necessary.
   *
   * @param toAdd - elements to add.
   * @return true if the set has changed, false otherwise
   */
  @Override
  public boolean addAll(Collection<? extends T> toAdd) {
    boolean changed = false;
    for (T elem : toAdd) {
      changed |= addElem(elem);
    }
    expandIfNecessary();
    return changed;
  }

  /**
   * Add given element to the hash table. Expand table if necessary.
   *
   * @return true if the element was not present in the table, false otherwise
   */
  @Override
  public boolean add(final T element) {
    boolean added = addElem(element);
    expandIfNecessary();
    return added;
  }

  /**
   * Add given element to the hash table
   *
   * @return true if the element was not present in the table, false otherwise
   */
  protected boolean addElem(final T element) {
    // validate element
    if (element == null) {
      throw new IllegalArgumentException("Null element is not supported.");
    }
    // find hashCode & index
    final int hashCode = element.hashCode();
    final int index = getIndex(hashCode);
    // return false if already present
    if (getContainedElem(index, element, hashCode) != null) {
      return false;
    }

    modification++;
    size++;

    // update bucket linked list
    LinkedElement<T> le = new LinkedElement<T>(element, hashCode);
    le.next = entries[index];
    entries[index] = le;
    return true;
  }

  /**
   * Remove the element corresponding to the key.
   *
   * @return If such element exists, return true. Otherwise, return false.
   */
  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(final Object key) {
    // validate key
    if (key == null) {
      throw new IllegalArgumentException("Null element is not supported.");
    }
    LinkedElement<T> removed = removeElem((T) key);
    shrinkIfNecessary();
    return removed == null ? false : true;
  }

  /**
   * Remove the element corresponding to the key, given key.hashCode() == index.
   *
   * @return If such element exists, return true. Otherwise, return false.
   */
  protected LinkedElement<T> removeElem(final T key) {
    LinkedElement<T> found = null;
    final int hashCode = key.hashCode();
    final int index = getIndex(hashCode);
    if (entries[index] == null) {
      return null;
    } else if (hashCode == entries[index].hashCode &&
            entries[index].element.equals(key)) {
      // remove the head of the bucket linked list
      modification++;
      size--;
      found = entries[index];
      entries[index] = found.next;
    } else {
      // head != null and key is not equal to head
      // search the element
      LinkedElement<T> prev = entries[index];
      for (found = prev.next; found != null;) {
        if (hashCode == found.hashCode &&
                found.element.equals(key)) {
          // found the element, remove it
          modification++;
          size--;
          prev.next = found.next;
          found.next = null;
          break;
        } else {
          prev = found;
          found = found.next;
        }
      }
    }
    return found;
  }

  /**
   * Remove and return n elements from the hashtable.
   * The order in which entries are removed is unspecified, and
   * and may not correspond to the order in which they were inserted.
   *
   * @return first element
   */
  public List<T> pollN(int n) {
    if (n >= size) {
      return pollAll();
    }
    List<T> retList = new ArrayList<T>(n);
    if (n == 0) {
      return retList;
    }
    boolean done = false;
    int currentBucketIndex = 0;

    while (!done) {
      LinkedElement<T> current = entries[currentBucketIndex];
      while (current != null) {
        retList.add(current.element);
        current = current.next;
        entries[currentBucketIndex] = current;
        size--;
        modification++;
        if (--n == 0) {
          done = true;
          break;
        }
      }
      currentBucketIndex++;
    }
    shrinkIfNecessary();
    return retList;
  }

  /**
   * Remove all elements from the set and return them. Clear the entries.
   */
  public List<T> pollAll() {
    List<T> retList = new ArrayList<T>(size);
    for (int i = 0; i < entries.length; i++) {
      LinkedElement<T> current = entries[i];
      while (current != null) {
        retList.add(current.element);
        current = current.next;
      }
    }
    this.clear();
    return retList;
  }

  /**
   * Get array.length elements from the set, and put them into the array.
   */
  @SuppressWarnings("unchecked")
  public T[] pollToArray(T[] array) {
    int currentIndex = 0;
    LinkedElement<T> current = null;

    if (array.length == 0) {
      return array;
    }
    if (array.length > size) {
      array = (T[]) java.lang.reflect.Array.newInstance(array.getClass()
          .getComponentType(), size);
    }
    // do fast polling if the entire set needs to be fetched
    if (array.length == size) {
      for (int i = 0; i < entries.length; i++) {
        current = entries[i];
        while (current != null) {
          array[currentIndex++] = current.element;
          current = current.next;
        }
      }
      this.clear();
      return array;
    }

    boolean done = false;
    int currentBucketIndex = 0;

    while (!done) {
      current = entries[currentBucketIndex];
      while (current != null) {
        array[currentIndex++] = current.element;
        current = current.next;
        entries[currentBucketIndex] = current;
        size--;
        modification++;
        if (currentIndex == array.length) {
          done = true;
          break;
        }
      }
      currentBucketIndex++;
    }
    shrinkIfNecessary();
    return array;
  }

  /**
   * Compute capacity given initial capacity.
   *
   * @return final capacity, either MIN_CAPACITY, MAX_CAPACITY, or power of 2
   *         closest to the requested capacity.
   */
  private int computeCapacity(int initial) {
    if (initial < MINIMUM_CAPACITY) {
      return MINIMUM_CAPACITY;
    }
    if (initial > MAXIMUM_CAPACITY) {
      return MAXIMUM_CAPACITY;
    }
    int capacity = 1;
    while (capacity < initial) {
      capacity <<= 1;
    }
    return capacity;
  }

  /**
   * Resize the internal table to given capacity.
   */
  @SuppressWarnings("unchecked")
  private void resize(int cap) {
    int newCapacity = computeCapacity(cap);
    if (newCapacity == this.capacity) {
      return;
    }
    this.capacity = newCapacity;
    this.expandThreshold = (int) (capacity * maxLoadFactor);
    this.shrinkThreshold = (int) (capacity * minLoadFactor);
    this.hash_mask = capacity - 1;
    LinkedElement<T>[] temp = entries;
    entries = new LinkedElement[capacity];
    for (int i = 0; i < temp.length; i++) {
      LinkedElement<T> curr = temp[i];
      while (curr != null) {
        LinkedElement<T> next = curr.next;
        int index = getIndex(curr.hashCode);
        curr.next = entries[index];
        entries[index] = curr;
        curr = next;
      }
    }
  }

  /**
   * Checks if we need to shrink, and shrinks if necessary.
   */
  protected void shrinkIfNecessary() {
    if (size < this.shrinkThreshold && capacity > initialCapacity) {
      resize(capacity / expandMultiplier);
    }
  }

  /**
   * Checks if we need to expand, and expands if necessary.
   */
  protected void expandIfNecessary() {
    if (size > this.expandThreshold && capacity < MAXIMUM_CAPACITY) {
      resize(capacity * expandMultiplier);
    }
  }

  @Override
  public Iterator<T> iterator() {
    return new LinkedSetIterator();
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName());
    b.append("(size=").append(size).append(", modification=")
        .append(modification).append(", entries.length=")
        .append(entries.length).append(")");
    return b.toString();
  }

  /** Print detailed information of this object. */
  public void printDetails(final PrintStream out) {
    out.print(this + ", entries = [");
    for (int i = 0; i < entries.length; i++) {
      if (entries[i] != null) {
        LinkedElement<T> e = entries[i];
        out.print("\n  " + i + ": " + e);
        for (e = e.next; e != null; e = e.next) {
          out.print(" -> " + e);
        }
      }
    }
    out.println("\n]");
  }

  private class LinkedSetIterator implements Iterator<T> {
    /** The starting modification for fail-fast. */
    private final int startModification = modification;
    /** The current index of the entry array. */
    private int index = -1;
    /** The next element to return. */
    private LinkedElement<T> next = nextNonemptyEntry();

    private LinkedElement<T> nextNonemptyEntry() {
      for (index++; index < entries.length && entries[index] == null; index++);
      return index < entries.length ? entries[index] : null;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public T next() {
      if (modification != startModification) {
        throw new ConcurrentModificationException("modification="
            + modification + " != startModification = " + startModification);
      }
      if (next == null) {
        throw new NoSuchElementException();
      }
      final T e = next.element;
      // find the next element
      final LinkedElement<T> n = next.next;
      next = n != null ? n : nextNonemptyEntry();
      return e;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported.");
    }
  }

  /**
   * Clear the set. Resize it to the original capacity.
   */
  @Override
  @SuppressWarnings("unchecked")
  public void clear() {
    this.capacity = this.initialCapacity;
    this.hash_mask = capacity - 1;

    this.expandThreshold = (int) (capacity * maxLoadFactor);
    this.shrinkThreshold = (int) (capacity * minLoadFactor);

    entries = new LinkedElement[capacity];
    size = 0;
    modification++;
  }

  @Override
  public Object[] toArray() {
    Object[] result = new Object[size];
    return toArray(result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <U> U[] toArray(U[] a) {
    if (a == null) {
      throw new NullPointerException("Input array can not be null");
    }
    if (a.length < size) {
      a = (U[]) java.lang.reflect.Array.newInstance(a.getClass()
          .getComponentType(), size);
    }
    int currentIndex = 0;
    for (int i = 0; i < entries.length; i++) {
      LinkedElement<T> current = entries[i];
      while (current != null) {
        a[currentIndex++] = (U) current.element;
        current = current.next;
      }
    }
    return a;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    Iterator<?> iter = c.iterator();
    while (iter.hasNext()) {
      if (!contains(iter.next())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean changed = false;
    Iterator<?> iter = c.iterator();
    while (iter.hasNext()) {
      changed |= remove(iter.next());
    }
    return changed;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("retainAll is not supported.");
  }
}
