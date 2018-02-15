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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.util.Collections;
import java.util.Iterator;

/**
 * This interface defines the methods used to store and manage InodeDiffs.
 * @param <T> Type of the object in this list.
 */
public interface DiffList<T extends Comparable<Integer>> extends Iterable<T> {
  DiffList EMPTY_LIST = new DiffListByArrayList(Collections.emptyList());

  /**
   * Returns an empty DiffList.
   */
  static <T extends Comparable<Integer>> DiffList<T> emptyList() {
    return EMPTY_LIST;
  }

  /**
   * Returns an unmodifiable diffList.
   * @param diffs DiffList
   * @param <T> Type of the object in the the diffList
   * @return Unmodifiable diffList
   */
  static <T extends Comparable<Integer>> DiffList<T> unmodifiableList(
      DiffList<T> diffs) {
    return new DiffList<T>() {
      @Override
      public T get(int i) {
        return diffs.get(i);
      }

      @Override
      public boolean isEmpty() {
        return diffs.isEmpty();
      }

      @Override
      public int size() {
        return diffs.size();
      }

      @Override
      public T remove(int i) {
        throw new UnsupportedOperationException("This list is unmodifiable.");
      }

      @Override
      public boolean addLast(T t) {
        throw new UnsupportedOperationException("This list is unmodifiable.");
      }

      @Override
      public void addFirst(T t) {
        throw new UnsupportedOperationException("This list is unmodifiable.");
      }

      @Override
      public int binarySearch(int i) {
        return diffs.binarySearch(i);
      }

      @Override
      public Iterator<T> iterator() {
        return diffs.iterator();
      }
    };
  }

  /**
   * Returns the element at the specified position in this list.
   *
   * @param index index of the element to return
   * @return the element at the specified position in this list
   * @throws IndexOutOfBoundsException if the index is out of range
   *         (<tt>index &lt; 0 || index &gt;= size()</tt>)
   */
  T get(int index);

  /**
   * Returns true if this list contains no elements.
   *
   * @return true if this list contains no elements
   */
  boolean isEmpty();

  /**
   * Returns the number of elements in this list.
   * @return the number of elements in this list.
   */
  int size();

  /**
   * Removes the element at the specified position in this list.
   * @param index the index of the element to be removed
   * @return the element previously at the specified position
   */
  T remove(int index);

  /**
   * Adds an element at the end of the list.
   * @param t element to be appended to this list
   * @return true, if insertion is successful
   */
  boolean addLast(T t);

  /**
   * Adds an element at the beginning of the list.
   * @param t element to be added to this list
   */
  void addFirst(T t);

  /**
   * Searches the list for the specified object using the binary
   * search algorithm.
   * @param key key to be searched for
   * @return the index of the search key, if it is contained in the list
   *         otherwise, (-insertion point - 1).
   */
  int binarySearch(int key);

}
