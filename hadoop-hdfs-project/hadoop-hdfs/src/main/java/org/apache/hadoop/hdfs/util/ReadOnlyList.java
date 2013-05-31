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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A {@link ReadOnlyList} is a unmodifiable list,
 * which supports read-only operations.
 * 
 * @param <E> The type of the list elements.
 */
@InterfaceAudience.Private
public interface ReadOnlyList<E> extends Iterable<E> {
  /**
   * Is this an empty list?
   */
  boolean isEmpty();

  /**
   * @return the size of this list.
   */
  int size();

  /**
   * @return the i-th element.
   */
  E get(int i);
  
  /**
   * Utilities for {@link ReadOnlyList}
   */
  public static class Util {
    /** @return an empty list. */
    public static <E> ReadOnlyList<E> emptyList() {
      return ReadOnlyList.Util.asReadOnlyList(Collections.<E>emptyList());
    }

    /**
     * The same as {@link Collections#binarySearch(List, Object)}
     * except that the list is a {@link ReadOnlyList}.
     *
     * @return the insertion point defined
     *         in {@link Collections#binarySearch(List, Object)}.
     */
    public static <K, E extends Comparable<K>> int binarySearch(
        final ReadOnlyList<E> list, final K key) {
      int lower = 0;
      for(int upper = list.size() - 1; lower <= upper; ) {
        final int mid = (upper + lower) >>> 1;

        final int d = list.get(mid).compareTo(key);
        if (d == 0) {
          return mid;
        } else if (d > 0) {
          upper = mid - 1;
        } else {
          lower = mid + 1;
        }
      }
      return -(lower + 1);
    }

    /**
     * @return a {@link ReadOnlyList} view of the given list.
     */
    public static <E> ReadOnlyList<E> asReadOnlyList(final List<E> list) {
      return new ReadOnlyList<E>() {
        @Override
        public Iterator<E> iterator() {
          return list.iterator();
        }

        @Override
        public boolean isEmpty() {
          return list.isEmpty();
        }

        @Override
        public int size() {
          return list.size();
        }

        @Override
        public E get(int i) {
          return list.get(i);
        }
      };
    }

    /**
     * @return a {@link List} view of the given list.
     */
    public static <E> List<E> asList(final ReadOnlyList<E> list) {
      return new List<E>() {
        @Override
        public Iterator<E> iterator() {
          return list.iterator();
        }

        @Override
        public boolean isEmpty() {
          return list.isEmpty();
        }

        @Override
        public int size() {
          return list.size();
        }

        @Override
        public E get(int i) {
          return list.get(i);
        }

        @Override
        public Object[] toArray() {
          final Object[] a = new Object[size()];
          for(int i = 0; i < a.length; i++) {
            a[i] = get(i);
          }
          return a;
        }

        //All methods below are not supported.

        @Override
        public boolean add(E e) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void add(int index, E element) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(int index, Collection<? extends E> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean contains(Object o) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection<?> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public int indexOf(Object o) {
          throw new UnsupportedOperationException();
        }

        @Override
        public int lastIndexOf(Object o) {
          throw new UnsupportedOperationException();
        }

        @Override
        public ListIterator<E> listIterator() {
          throw new UnsupportedOperationException();
        }

        @Override
        public ListIterator<E> listIterator(int index) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
          throw new UnsupportedOperationException();
        }

        @Override
        public E remove(int index) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public E set(int index, E element) {
          throw new UnsupportedOperationException();
        }

        @Override
        public List<E> subList(int fromIndex, int toIndex) {
          throw new UnsupportedOperationException();
        }

        @Override
        public <T> T[] toArray(T[] a) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
