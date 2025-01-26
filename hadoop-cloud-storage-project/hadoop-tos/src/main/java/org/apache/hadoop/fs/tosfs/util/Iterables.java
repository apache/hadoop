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

package org.apache.hadoop.fs.tosfs.util;

import org.apache.hadoop.util.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

public class Iterables {

  private Iterables() {}

  public static <F, T> Iterable<T> transform(final Iterable<F> fromIterable,
      final Function<? super F, ? extends T> function) {
    Preconditions.checkNotNull(fromIterable);
    Preconditions.checkNotNull(function);
    return () -> new Iterator<T>() {
      private Iterator<F> iterator = fromIterable.iterator();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        return function.apply(iterator.next());
      }
    };
  }

  public static <T> Iterable<T> filter(final Iterable<T> unfiltered,
      final Predicate<? super T> predicate) {
    Preconditions.checkNotNull(unfiltered);
    Preconditions.checkNotNull(predicate);
    return () -> new Iterator<T>() {
      private Iterator<T> iterator = unfiltered.iterator();
      private boolean advance = true;
      private T value;

      @Override
      public boolean hasNext() {
        if (!advance) {
          return true;
        }

        while (iterator.hasNext()) {
          value = iterator.next();
          if (predicate.test(value)) {
            advance = false;
            return true;
          }
        }

        return false;
      }

      @Override
      public T next() {
        if (hasNext()) {
          advance = true;
          return value;
        }
        throw new NoSuchElementException("No more items in iterator.");
      }
    };
  }

  public static <T extends @Nullable Object> Iterable<T> concat(
      Iterable<? extends Iterable<? extends T>> inputs) {
    return () -> new ConcatenatedIterator<>(inputs.iterator());
  }

  private static class ConcatenatedIterator<T> implements Iterator<T> {
    // Iterators is the iterator of iterables.
    private final Iterator<? extends Iterable<? extends T>> iterators;
    private Iterator<? extends T> curIter;

    ConcatenatedIterator(Iterator<? extends Iterable<? extends T>> iterators) {
      Preconditions.checkNotNull(iterators, "Iterators should not be null.");
      this.iterators = iterators;
    }

    @Override
    public boolean hasNext() {
      while (curIter == null || !curIter.hasNext()) {
        if (curIter != null) {
          curIter = null;
        }

        if (!iterators.hasNext()) {
          return false;
        }

        curIter = iterators.next().iterator();
      }
      return true;
    }

    @Override
    public T next() {
      if (hasNext()) {
        return curIter.next();
      }
      throw new NoSuchElementException("No more elements");
    }
  }
}
