/*
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

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Static utility methods pertaining to {@link List} instances.
 * This class is Hadoop's internal use alternative to Guava's Lists
 * utility class.
 * Javadocs for majority of APIs in this class are taken from Guava's Lists
 * class from Guava release version 27.0-jre.
 */
@InterfaceAudience.Private
public final class Lists {

  private Lists() {
    // empty
  }

  /**
   * Creates a <i>mutable</i>, empty {@code ArrayList} instance.
   *
   * @param <E> Generics Type E.
   * @return ArrayList Generics Type E.
   */
  public static <E> ArrayList<E> newArrayList() {
    return new ArrayList<>();
  }

  /**
   * Creates a <i>mutable</i> {@code ArrayList} instance containing the given
   * elements.
   *
   * <p>Note that even when you do need the ability to add or remove,
   * this method provides only a tiny bit of syntactic sugar for
   * {@code newArrayList(}
   * {@link Arrays#asList asList}
   * {@code (...))}, or for creating an empty list then calling
   * {@link Collections#addAll}.
   *
   * @param <E> Generics Type E.
   * @param elements elements.
   * @return ArrayList Generics Type E.
   */
  @SafeVarargs
  public static <E> ArrayList<E> newArrayList(E... elements) {
    if (elements == null) {
      throw new NullPointerException();
    }
    // Avoid integer overflow when a large array is passed in
    int capacity = computeArrayListCapacity(elements.length);
    ArrayList<E> list = new ArrayList<>(capacity);
    Collections.addAll(list, elements);
    return list;
  }

  /**
   * Creates a <i>mutable</i> {@code ArrayList} instance containing the
   * given elements; a very thin shortcut for creating an empty list then
   * calling Iterables#addAll.
   *
   * @param <E> Generics Type E.
   * @param elements elements.
   * @return ArrayList Generics Type E.
   */
  public static <E> ArrayList<E> newArrayList(Iterable<? extends E> elements) {
    if (elements == null) {
      throw new NullPointerException();
    }
    return (elements instanceof Collection)
        ? new ArrayList<>(cast(elements))
        : newArrayList(elements.iterator());
  }

  /**
   * Creates a <i>mutable</i> {@code ArrayList} instance containing the
   * given elements; a very thin shortcut for creating an empty list
   * and then calling Iterators#addAll.
   *
   * @param <E> Generics Type E.
   * @param elements elements.
   * @return ArrayList Generics Type E.
   */
  public static <E> ArrayList<E> newArrayList(Iterator<? extends E> elements) {
    ArrayList<E> list = newArrayList();
    addAll(list, elements);
    return list;
  }

  /**
   * Creates an {@code ArrayList} instance backed by an array with the
   * specified initial size;
   * simply delegates to {@link ArrayList#ArrayList(int)}.
   *
   * @param <E> Generics Type E.
   * @param initialArraySize the exact size of the initial backing array for
   *     the returned array list
   *     ({@code ArrayList} documentation calls this value the "capacity").
   * @return a new, empty {@code ArrayList} which is guaranteed not to
   *     resize itself unless its size reaches {@code initialArraySize + 1}.
   * @throws IllegalArgumentException if {@code initialArraySize} is negative.
   */
  public static <E> ArrayList<E> newArrayListWithCapacity(
      int initialArraySize) {
    checkNonnegative(initialArraySize, "initialArraySize");
    return new ArrayList<>(initialArraySize);
  }

  /**
   * Creates an {@code ArrayList} instance to hold {@code estimatedSize}
   * elements, <i>plus</i> an unspecified amount of padding;
   * you almost certainly mean to call {@link
   * #newArrayListWithCapacity} (see that method for further advice on usage).
   *
   * @param estimatedSize an estimate of the eventual {@link List#size()}
   *     of the new list.
   * @return a new, empty {@code ArrayList}, sized appropriately to hold the
   *     estimated number of elements.
   * @throws IllegalArgumentException if {@code estimatedSize} is negative.
   *
   * @param <E> Generics Type E.
   */
  public static <E> ArrayList<E> newArrayListWithExpectedSize(
      int estimatedSize) {
    return new ArrayList<>(computeArrayListCapacity(estimatedSize));
  }

  /**
   * Creates a <i>mutable</i>, empty {@code LinkedList} instance.
   *
   * <p><b>Performance note:</b> {@link ArrayList} and
   * {@link java.util.ArrayDeque} consistently
   * outperform {@code LinkedList} except in certain rare and specific
   * situations. Unless you have
   * spent a lot of time benchmarking your specific needs, use one of those
   * instead.</p>
   *
   * @param <E> Generics Type E.
   * @return Generics Type E List.
   */
  public static <E> LinkedList<E> newLinkedList() {
    return new LinkedList<>();
  }

  /**
   * Creates a <i>mutable</i> {@code LinkedList} instance containing the given
   * elements; a very thin shortcut for creating an empty list then calling
   * Iterables#addAll.
   *
   * <p><b>Performance note:</b> {@link ArrayList} and
   * {@link java.util.ArrayDeque} consistently
   * outperform {@code LinkedList} except in certain rare and specific
   * situations. Unless you have spent a lot of time benchmarking your
   * specific needs, use one of those instead.</p>
   *
   * @param elements elements.
   * @param <E> Generics Type E.
   * @return Generics Type E List.
   */
  public static <E> LinkedList<E> newLinkedList(
      Iterable<? extends E> elements) {
    LinkedList<E> list = newLinkedList();
    addAll(list, elements);
    return list;
  }

  private static int computeArrayListCapacity(int arraySize) {
    checkNonnegative(arraySize, "arraySize");
    return saturatedCast(5L + arraySize + (arraySize / 10));
  }

  private static int checkNonnegative(int value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException(name + " cannot be negative but was: "
          + value);
    }
    return value;
  }

  /**
   * Returns the {@code int} nearest in value to {@code value}.
   *
   * @param value any {@code long} value.
   * @return the same value cast to {@code int} if it is in the range of the
   *     {@code int} type, {@link Integer#MAX_VALUE} if it is too large,
   *     or {@link Integer#MIN_VALUE} if it is too small.
   */
  private static int saturatedCast(long value) {
    if (value > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    if (value < Integer.MIN_VALUE) {
      return Integer.MIN_VALUE;
    }
    return (int) value;
  }

  private static <T> boolean addAll(Collection<T> addTo,
      Iterator<? extends T> iterator) {
    if (addTo == null) {
      throw new NullPointerException();
    }
    if (iterator == null) {
      throw new NullPointerException();
    }
    boolean wasModified = false;
    while (iterator.hasNext()) {
      wasModified |= addTo.add(iterator.next());
    }
    return wasModified;
  }

  private static <T> Collection<T> cast(Iterable<T> iterable) {
    return (Collection<T>) iterable;
  }

  /**
   * Adds all elements in {@code iterable} to {@code collection}.
   *
   * @return {@code true} if {@code collection} was modified as a result of
   *     this operation.
   */
  private static <T> boolean addAll(Collection<T> addTo,
      Iterable<? extends T> elementsToAdd) {
    if (elementsToAdd instanceof Collection) {
      Collection<? extends T> c = cast(elementsToAdd);
      return addTo.addAll(c);
    }
    if (elementsToAdd == null) {
      throw new NullPointerException();
    }
    return addAll(addTo, elementsToAdd.iterator());
  }

  /**
   * Returns consecutive sub-lists of a list, each of the same size
   * (the final list may be smaller).
   * @param originalList original big list.
   * @param pageSize desired size of each sublist ( last one
   *                 may be smaller)
   * @param <T> Generics Type.
   * @return a list of sub lists.
   */
  public static <T> List<List<T>> partition(List<T> originalList, int pageSize) {

    Preconditions.checkArgument(originalList != null && originalList.size() > 0,
            "Invalid original list");
    Preconditions.checkArgument(pageSize > 0, "Page size should " +
            "be greater than 0 for performing partition");

    List<List<T>> result = new ArrayList<>();
    int i=0;
    while (i < originalList.size()) {
      result.add(originalList.subList(i,
              Math.min(i + pageSize, originalList.size())));
      i = i + pageSize;
    }
    return result;
  }
}
