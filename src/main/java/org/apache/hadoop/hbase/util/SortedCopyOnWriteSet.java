/*
 * Copyright 2011 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Simple {@link java.util.SortedSet} implementation that uses an internal
 * {@link java.util.TreeSet} to provide ordering. All mutation operations
 * create a new copy of the <code>TreeSet</code> instance, so are very
 * expensive.  This class is only intended for use on small, very rarely
 * written collections that expect highly concurrent reads. Read operations
 * are performed on a reference to the internal <code>TreeSet</code> at the
 * time of invocation, so will not see any mutations to the collection during
 * their operation.
 *
 * <p>Note that due to the use of a {@link java.util.TreeSet} internally,
 * a {@link java.util.Comparator} instance must be provided, or collection
 * elements must implement {@link java.lang.Comparable}.
 * </p>
 * @param <E> A class implementing {@link java.lang.Comparable} or able to be
 * compared by a provided comparator.
 */
public class SortedCopyOnWriteSet<E> implements SortedSet<E> {
  private SortedSet<E> internalSet;

  public SortedCopyOnWriteSet() {
    this.internalSet = new TreeSet<E>();
  }

  public SortedCopyOnWriteSet(Collection<? extends E> c) {
    this.internalSet = new TreeSet<E>(c);
  }

  public SortedCopyOnWriteSet(Comparator<? super E> comparator) {
    this.internalSet = new TreeSet<E>(comparator);
  }

  @Override
  public int size() {
    return internalSet.size();
  }

  @Override
  public boolean isEmpty() {
    return internalSet.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return internalSet.contains(o);
  }

  @Override
  public Iterator<E> iterator() {
    return internalSet.iterator();
  }

  @Override
  public Object[] toArray() {
    return internalSet.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return internalSet.toArray(a);
  }

  @Override
  public synchronized boolean add(E e) {
    SortedSet<E> newSet = new TreeSet<E>(internalSet);
    boolean added = newSet.add(e);
    internalSet = newSet;
    return added;
  }

  @Override
  public synchronized boolean remove(Object o) {
    SortedSet<E> newSet = new TreeSet<E>(internalSet);
    boolean removed = newSet.remove(o);
    internalSet = newSet;
    return removed;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return internalSet.containsAll(c);
  }

  @Override
  public synchronized boolean addAll(Collection<? extends E> c) {
    SortedSet<E> newSet = new TreeSet<E>(internalSet);
    boolean changed = newSet.addAll(c);
    internalSet = newSet;
    return changed;
  }

  @Override
  public synchronized boolean retainAll(Collection<?> c) {
    SortedSet<E> newSet = new TreeSet<E>(internalSet);
    boolean changed = newSet.retainAll(c);
    internalSet = newSet;
    return changed;
  }

  @Override
  public synchronized boolean removeAll(Collection<?> c) {
    SortedSet<E> newSet = new TreeSet<E>(internalSet);
    boolean changed = newSet.removeAll(c);
    internalSet = newSet;
    return changed;
  }

  @Override
  public synchronized void clear() {
    Comparator<? super E> comparator = internalSet.comparator();
    if (comparator != null) {
      internalSet = new TreeSet<E>(comparator);
    } else {
      internalSet = new TreeSet<E>();
    }
  }

  @Override
  public Comparator<? super E> comparator() {
    return internalSet.comparator();
  }

  @Override
  public SortedSet<E> subSet(E fromElement, E toElement) {
    return internalSet.subSet(fromElement, toElement);
  }

  @Override
  public SortedSet<E> headSet(E toElement) {
    return internalSet.headSet(toElement);
  }

  @Override
  public SortedSet<E> tailSet(E fromElement) {
    return internalSet.tailSet(fromElement);
  }

  @Override
  public E first() {
    return internalSet.first();
  }

  @Override
  public E last() {
    return internalSet.last();
  }
}
