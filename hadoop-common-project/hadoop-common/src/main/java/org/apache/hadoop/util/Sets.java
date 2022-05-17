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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Static utility methods pertaining to {@link Set} instances.
 * This class is Hadoop's internal use alternative to Guava's Sets
 * utility class.
 * Javadocs for majority of APIs in this class are taken from Guava's Sets
 * class from Guava release version 27.0-jre.
 */
@InterfaceAudience.Private
public final class Sets {

  private static final int MAX_POWER_OF_TWO = 1 << (Integer.SIZE - 2);

  private Sets() {
    // empty
  }

  /**
   * Creates a <i>mutable</i>, initially empty {@code HashSet} instance.
   *
   * <p><b>Note:</b> if mutability is not required, use ImmutableSet#of()
   * instead. If {@code E} is an {@link Enum} type, use {@link EnumSet#noneOf}
   * instead. Otherwise, strongly consider using a {@code LinkedHashSet}
   * instead, at the cost of increased memory footprint, to get
   * deterministic iteration behavior.</p>
   *
   * @param <E> Generics Type E.
   * @return a new, empty {@code TreeSet}
   */
  public static <E> HashSet<E> newHashSet() {
    return new HashSet<E>();
  }

  /**
   * Creates a <i>mutable</i>, empty {@code TreeSet} instance sorted by the
   * natural sort ordering of its elements.
   *
   * <p><b>Note:</b> if mutability is not required, use ImmutableSortedSet#of()
   * instead.</p>
   *
   * @param <E> Generics Type E
   * @return a new, empty {@code TreeSet}
   */
  public static <E extends Comparable> TreeSet<E> newTreeSet() {
    return new TreeSet<E>();
  }

  /**
   * Creates a <i>mutable</i> {@code HashSet} instance initially containing
   * the given elements.
   *
   * <p><b>Note:</b> if elements are non-null and won't be added or removed
   * after this point, use ImmutableSet#of() or ImmutableSet#copyOf(Object[])
   * instead. If {@code E} is an {@link Enum} type, use
   * {@link EnumSet#of(Enum, Enum[])} instead. Otherwise, strongly consider
   * using a {@code LinkedHashSet} instead, at the cost of increased memory
   * footprint, to get deterministic iteration behavior.</p>
   *
   * <p>This method is just a small convenience, either for
   * {@code newHashSet(}{@link Arrays#asList}{@code (...))}, or for creating an
   * empty set then calling {@link Collections#addAll}.</p>
   *
   * @param <E> Generics Type E.
   * @param elements the elements that the set should contain.
   * @return a new, empty thread-safe {@code Set}
   */
  @SafeVarargs
  public static <E> HashSet<E> newHashSet(E... elements) {
    HashSet<E> set = newHashSetWithExpectedSize(elements.length);
    Collections.addAll(set, elements);
    return set;
  }

  /**
   * Creates a <i>mutable</i> {@code HashSet} instance containing the given
   * elements. A very thin convenience for creating an empty set then calling
   * {@link Collection#addAll} or Iterables#addAll.
   *
   * <p><b>Note:</b> if mutability is not required and the elements are
   * non-null, use ImmutableSet#copyOf(Iterable) instead. (Or, change
   * {@code elements} to be a FluentIterable and call {@code elements.toSet()}.)</p>
   *
   * <p><b>Note:</b> if {@code E} is an {@link Enum} type, use
   * newEnumSet(Iterable, Class) instead.</p>
   *
   * @param <E> Generics Type E.
   * @param elements the elements that the set should contain.
   * @return a new, empty thread-safe {@code Set}.
   */
  public static <E> HashSet<E> newHashSet(Iterable<? extends E> elements) {
    return (elements instanceof Collection)
        ? new HashSet<E>(cast(elements))
        : newHashSet(elements.iterator());
  }

  /**
   * Creates a <i>mutable</i> {@code TreeSet} instance containing the given
   * elements sorted by their natural ordering.
   *
   * <p><b>Note:</b> if mutability is not required, use
   * ImmutableSortedSet#copyOf(Iterable) instead.
   *
   * <p><b>Note:</b> If {@code elements} is a {@code SortedSet} with an
   * explicit comparator, this method has different behavior than
   * {@link TreeSet#TreeSet(SortedSet)}, which returns a {@code TreeSet}
   * with that comparator.
   *
   * <p><b>Note for Java 7 and later:</b> this method is now unnecessary and
   * should be treated as deprecated. Instead, use the {@code TreeSet}
   * constructor directly, taking advantage of the new
   * <a href="http://goo.gl/iz2Wi">"diamond" syntax</a>.
   *
   * <p>This method is just a small convenience for creating an empty set and
   * then calling Iterables#addAll. This method is not very useful and will
   * likely be deprecated in the future.
   *
   * @param <E> Generics Type E.
   * @param elements the elements that the set should contain
   * @return a new {@code TreeSet} containing those elements (minus duplicates)
   */
  public static <E extends Comparable> TreeSet<E> newTreeSet(
      Iterable<? extends E> elements) {
    TreeSet<E> set = newTreeSet();
    addAll(set, elements);
    return set;
  }

  private static <E extends Comparable> boolean addAll(TreeSet<E> addTo,
      Iterable<? extends E> elementsToAdd) {
    if (elementsToAdd instanceof Collection) {
      Collection<? extends E> c = cast(elementsToAdd);
      return addTo.addAll(c);
    }
    if (elementsToAdd == null) {
      throw new NullPointerException();
    }
    return addAll(addTo, elementsToAdd.iterator());
  }

  /**
   * Creates a <i>mutable</i> {@code HashSet} instance containing the given
   * elements. A very thin convenience for creating an empty set and then
   * calling Iterators#addAll.
   *
   * <p><b>Note:</b> if mutability is not required and the elements are
   * non-null, use ImmutableSet#copyOf(Iterator) instead.</p>
   *
   * <p><b>Note:</b> if {@code E} is an {@link Enum} type, you should create
   * an {@link EnumSet} instead.</p>
   *
   * <p>Overall, this method is not very useful and will likely be deprecated
   * in the future.</p>
   *
   * @param <E> Generics Type E.
   * @param elements elements.
   * @return a new, empty thread-safe {@code Set}.
   */
  public static <E> HashSet<E> newHashSet(Iterator<? extends E> elements) {
    HashSet<E> set = newHashSet();
    addAll(set, elements);
    return set;
  }

  /**
   * Returns a new hash set using the smallest initial table size that can hold
   * {@code expectedSize} elements without resizing. Note that this is not what
   * {@link HashSet#HashSet(int)} does, but it is what most users want and
   * expect it to do.
   *
   * <p>This behavior can't be broadly guaranteed, but has been tested with
   * OpenJDK 1.7 and 1.8.</p>
   *
   * @param expectedSize the number of elements you expect to add to the
   *     returned set
   * @param <E> Generics Type E.
   * @return a new, empty hash set with enough capacity to hold
   *     {@code expectedSize} elements without resizing
   * @throws IllegalArgumentException if {@code expectedSize} is negative
   */
  public static <E> HashSet<E> newHashSetWithExpectedSize(int expectedSize) {
    return new HashSet<E>(capacity(expectedSize));
  }

  private static <E> Collection<E> cast(Iterable<E> iterable) {
    return (Collection<E>) iterable;
  }

  private static <E> boolean addAll(Collection<E> addTo,
      Iterator<? extends E> iterator) {
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

  /**
   * Returns the intersection of two sets as an unmodifiable set.
   * The returned set contains all elements that are contained by both backing
   * sets.
   *
   * <p>Results are undefined if {@code set1} and {@code set2} are sets based
   * on different equivalence relations (as {@code HashSet}, {@code TreeSet},
   * and the keySet of an {@code IdentityHashMap} all are).
   *
   * @param set1 set1.
   * @param set2 set2.
   * @param <E> Generics Type E.
   * @return a new, empty thread-safe {@code Set}.
   */
  public static <E> Set<E> intersection(final Set<E> set1,
      final Set<E> set2) {
    if (set1 == null) {
      throw new NullPointerException("set1");
    }
    if (set2 == null) {
      throw new NullPointerException("set2");
    }
    Set<E> newSet = new HashSet<>(set1);
    newSet.retainAll(set2);
    return Collections.unmodifiableSet(newSet);
  }

  /**
   * Returns the union of two sets as an unmodifiable set.
   * The returned set contains all elements that are contained in either
   * backing set.
   *
   * <p>Results are undefined if {@code set1} and {@code set2} are sets
   * based on different equivalence relations (as {@link HashSet},
   * {@link TreeSet}, and the {@link Map#keySet} of an
   * {@code IdentityHashMap} all are).
   *
   * @param set1 set1.
   * @param set2 set2.
   * @param <E> Generics Type E.
   * @return a new, empty thread-safe {@code Set}.
   */
  public static <E> Set<E> union(
      final Set<E> set1, final Set<E> set2) {
    if (set1 == null) {
      throw new NullPointerException("set1");
    }
    if (set2 == null) {
      throw new NullPointerException("set2");
    }
    Set<E> newSet = new HashSet<>(set1);
    newSet.addAll(set2);
    return Collections.unmodifiableSet(newSet);
  }

  /**
   * Returns the difference of two sets as an unmodifiable set.
   * The returned set contains all elements that are contained by {@code set1}
   * and not contained by {@code set2}.
   *
   * <p>Results are undefined if {@code set1} and {@code set2} are sets based
   * on different equivalence relations (as {@code HashSet}, {@code TreeSet},
   * and the keySet of an {@code IdentityHashMap} all are).
   *
   * This method is used to find difference for HashSets. For TreeSets with
   * strict order requirement, recommended method is
   * {@link #differenceInTreeSets(Set, Set)}.
   *
   * @param set1 set1.
   * @param set2 set2.
   * @param <E> Generics Type E.
   * @return a new, empty thread-safe {@code Set}.
   */
  public static <E> Set<E> difference(
      final Set<E> set1, final Set<E> set2) {
    if (set1 == null) {
      throw new NullPointerException("set1");
    }
    if (set2 == null) {
      throw new NullPointerException("set2");
    }
    Set<E> newSet = new HashSet<>(set1);
    newSet.removeAll(set2);
    return Collections.unmodifiableSet(newSet);
  }

  /**
   * Returns the difference of two sets as an unmodifiable set.
   * The returned set contains all elements that are contained by {@code set1}
   * and not contained by {@code set2}.
   *
   * <p>Results are undefined if {@code set1} and {@code set2} are sets based
   * on different equivalence relations (as {@code HashSet}, {@code TreeSet},
   * and the keySet of an {@code IdentityHashMap} all are).
   *
   * This method is used to find difference for TreeSets. For HashSets,
   * recommended method is {@link #difference(Set, Set)}.
   *
   * @param <E> Generics Type E.
   * @param set1 set1.
   * @param set2 set2.
   * @return a new, empty thread-safe {@code Set}.
   */
  public static <E> Set<E> differenceInTreeSets(
      final Set<E> set1, final Set<E> set2) {
    if (set1 == null) {
      throw new NullPointerException("set1");
    }
    if (set2 == null) {
      throw new NullPointerException("set2");
    }
    Set<E> newSet = new TreeSet<>(set1);
    newSet.removeAll(set2);
    return Collections.unmodifiableSet(newSet);
  }

  /**
   * Returns the symmetric difference of two sets as an unmodifiable set.
   * The returned set contains all elements that are contained in either
   * {@code set1} or {@code set2} but not in both. The iteration order of the
   * returned set is undefined.
   *
   * <p>Results are undefined if {@code set1} and {@code set2} are sets based
   * on different equivalence relations (as {@code HashSet}, {@code TreeSet},
   * and the keySet of an {@code IdentityHashMap} all are).
   *
   * @param set1 set1.
   * @param set2 set2.
   * @param <E> Generics Type E.
   * @return a new, empty thread-safe {@code Set}.
   */
  public static <E> Set<E> symmetricDifference(
      final Set<E> set1, final Set<E> set2) {
    if (set1 == null) {
      throw new NullPointerException("set1");
    }
    if (set2 == null) {
      throw new NullPointerException("set2");
    }
    Set<E> intersection = new HashSet<>(set1);
    intersection.retainAll(set2);
    Set<E> symmetricDifference = new HashSet<>(set1);
    symmetricDifference.addAll(set2);
    symmetricDifference.removeAll(intersection);
    return Collections.unmodifiableSet(symmetricDifference);
  }

  /**
   * Creates a thread-safe set backed by a hash map. The set is backed by a
   * {@link ConcurrentHashMap} instance, and thus carries the same concurrency
   * guarantees.
   *
   * <p>Unlike {@code HashSet}, this class does NOT allow {@code null} to be
   * used as an element. The set is serializable.
   *
   * @param <E> Generics Type.
   * @return a new, empty thread-safe {@code Set}
   */
  public static <E> Set<E> newConcurrentHashSet() {
    return Collections.newSetFromMap(new ConcurrentHashMap<E, Boolean>());
  }

  /**
   * Returns a capacity that is sufficient to keep the map from being resized
   * as long as it grows no larger than expectedSize and the load factor
   * is ≥ its default (0.75).
   * The implementation of this method is adapted from Guava version 27.0-jre.
   */
  private static int capacity(int expectedSize) {
    if (expectedSize < 3) {
      if (expectedSize < 0) {
        throw new IllegalArgumentException(
            "expectedSize cannot be negative but was: " + expectedSize);
      }
      return expectedSize + 1;
    }
    if (expectedSize < MAX_POWER_OF_TWO) {
      // This is the calculation used in JDK8 to resize when a putAll
      // happens; it seems to be the most conservative calculation we
      // can make.  0.75 is the default load factor.
      return (int) ((float) expectedSize / 0.75F + 1.0F);
    }
    return Integer.MAX_VALUE; // any large value
  }

}
