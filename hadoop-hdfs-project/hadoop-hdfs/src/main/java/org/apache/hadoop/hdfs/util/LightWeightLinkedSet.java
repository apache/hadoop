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

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A low memory linked hash set implementation, which uses an array for storing
 * the elements and linked lists for collision resolution. In addition it stores
 * elements in a linked list to ensure ordered traversal. This class does not
 * support null element.
 *
 * This class is not thread safe.
 *
 */
public class LightWeightLinkedSet<T> extends LightWeightHashSet<T> {
  /**
   * Elements of {@link LightWeightLinkedSet}.
   */
  static class DoubleLinkedElement<T> extends LinkedElement<T> {
    // references to elements within all-element linked list
    private DoubleLinkedElement<T> before;
    private DoubleLinkedElement<T> after;

    public DoubleLinkedElement(T elem, int hashCode) {
      super(elem, hashCode);
      this.before = null;
      this.after = null;
    }

    public String toString() {
      return super.toString();
    }
  }

  private DoubleLinkedElement<T> head;
  private DoubleLinkedElement<T> tail;

  /**
   * @param initCapacity
   *          Recommended size of the internal array.
   * @param maxLoadFactor
   *          used to determine when to expand the internal array
   * @param minLoadFactor
   *          used to determine when to shrink the internal array
   */
  public LightWeightLinkedSet(int initCapacity, float maxLoadFactor,
      float minLoadFactor) {
    super(initCapacity, maxLoadFactor, minLoadFactor);
    head = null;
    tail = null;
  }

  public LightWeightLinkedSet() {
    this(MINIMUM_CAPACITY, DEFAULT_MAX_LOAD_FACTOR, DEFAUT_MIN_LOAD_FACTOR);
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
    if (containsElem(index, element, hashCode)) {
      return false;
    }

    modification++;
    size++;

    // update bucket linked list
    DoubleLinkedElement<T> le = new DoubleLinkedElement<T>(element, hashCode);
    le.next = entries[index];
    entries[index] = le;

    // insert to the end of the all-element linked list
    le.after = null;
    le.before = tail;
    if (tail != null) {
      tail.after = le;
    }
    tail = le;
    if (head == null) {
      head = le;
    }
    return true;
  }

  /**
   * Remove the element corresponding to the key, given key.hashCode() == index.
   *
   * @return Return the entry with the element if exists. Otherwise return null.
   */
  protected DoubleLinkedElement<T> removeElem(final T key) {
    DoubleLinkedElement<T> found = (DoubleLinkedElement<T>) (super
        .removeElem(key));
    if (found == null) {
      return null;
    }

    // update linked list
    if (found.after != null) {
      found.after.before = found.before;
    }
    if (found.before != null) {
      found.before.after = found.after;
    }
    if (head == found) {
      head = head.after;
    }
    if (tail == found) {
      tail = tail.before;
    }
    return found;
  }

  /**
   * Remove and return first element on the linked list of all elements.
   *
   * @return first element
   */
  public T pollFirst() {
    if (head == null) {
      return null;
    }
    T first = head.element;
    this.remove(first);
    return first;
  }

  /**
   * Remove and return n elements from the hashtable.
   * The order in which entries are removed is corresponds 
   * to the order in which they were inserted.
   *
   * @return first element
   */
  public List<T> pollN(int n) {
    if (n >= size) {
      // if we need to remove all elements then do fast polling
      return pollAll();
    }
    List<T> retList = new ArrayList<T>(n);
    while (n-- > 0 && head != null) {
      T curr = head.element;
      this.removeElem(curr);
      retList.add(curr);
    }
    shrinkIfNecessary();
    return retList;
  }

  /**
   * Remove all elements from the set and return them in order. Traverse the
   * link list, don't worry about hashtable - faster version of the parent
   * method.
   */
  public List<T> pollAll() {
    List<T> retList = new ArrayList<T>(size);
    while (head != null) {
      retList.add(head.element);
      head = head.after;
    }
    this.clear();
    return retList;
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
    DoubleLinkedElement<T> current = head;
    while (current != null) {
      T curr = current.element;
      a[currentIndex++] = (U) curr;
      current = current.after;
    }
    return a;
  }

  public Iterator<T> iterator() {
    return new LinkedSetIterator();
  }

  private class LinkedSetIterator implements Iterator<T> {
    /** The starting modification for fail-fast. */
    private final int startModification = modification;
    /** The next element to return. */
    private DoubleLinkedElement<T> next = head;

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
      next = next.after;
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
  public void clear() {
    super.clear();
    this.head = null;
    this.tail = null;
  }
}