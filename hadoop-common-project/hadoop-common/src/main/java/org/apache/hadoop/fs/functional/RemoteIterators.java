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

package org.apache.hadoop.fs.functional;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.logIOStatisticsAtDebug;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;

/**
 * A set of remote iterators supporting transformation and filtering,
 * with IOStatisticsSource passthrough, and of conversions of
 * the iterators to lists/arrays and of performing actions
 * on the values.
 * <p></p>
 * This aims to make it straightforward to use lambda-expressions to
 * transform the results of an iterator, without losing the statistics
 * in the process, and to chain the operations together.
 * <p></p>
 * The closeable operation will be passed through RemoteIterators which
 * wrap other RemoteIterators. This is to support any iterator which
 * can be closed to release held connections, file handles etc.
 * Unless client code is written to assume that RemoteIterator instances
 * may be closed, this is not likely to be broadly used. It is added
 * to make it possible to adopt this feature in a managed way.
 * <p></p>
 * One notable feature is that the
 * {@link #foreach(RemoteIterator, ConsumerRaisingIOE)} method will
 * LOG at debug any IOStatistics provided by the iterator, if such
 * statistics are provided. There's no attempt at retrieval and logging
 * if the LOG is not set to debug, so it is a zero cost feature unless
 * the logger {@code org.apache.hadoop.fs.functional.RemoteIterators}
 * is at DEBUG.
 * <p></p>
 * Based on the S3A Listing code, and some some work on moving other code
 * to using iterative listings so as to pick up the statistics.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class RemoteIterators {

  /**
   * Log used for logging any statistics in
   * {@link #foreach(RemoteIterator, ConsumerRaisingIOE)}
   * at DEBUG.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      RemoteIterators.class);

  private RemoteIterators() {
  }

  /**
   * Create an iterator from a singleton.
   * @param singleton instance
   * @param <T> type
   * @return a remote iterator
   */
  public static <T> RemoteIterator<T> toRemoteIterator(@Nullable T singleton) {
    return new SingletonIterator<>(singleton);
  }

  /**
   * Create a remote iterator from a java.util.Iterator.
   * @param <T> type
   * @return a remote iterator
   */
  public static <T> RemoteIterator<T> toRemoteIterator(Iterator<T> iterator) {
    return new FromIterable<>(iterator);
  }

  /**
   * Create a remote iterator from a java.util.Iterable -e.g. a list
   * or other collection.
   * @param <T> type
   * @return a remote iterator
   */
  public static <T> RemoteIterator<T> toRemoteIterator(Iterable<T> iterable) {
    return new FromIterable<>(iterable);
  }

  /**
   * Create a remote iterator from an array.
   * @param <T> type
   * @return a remote iterator
   */
  public static <T> RemoteIterator<T> toRemoteIterator(T[] array) {
    return new FromIterable<>(Arrays.stream(array).iterator());
  }

  /**
   * Build a list from a RemoteIterator.
   * @param <T> type
   * @return a list of the values.
   * @throws IOException if the source RemoteIterator raises it.
   */
  public static <T> List<T> toList(RemoteIterator<T> source)
      throws IOException {
    List<T> l = new ArrayList<>();
    foreach(source, l::add);
    return l;
  }

  /**
   * Build an array from a RemoteIterator.
   * @param <T> type
   * @return an array of the values.
   * @throws IOException if the source RemoteIterator raises it.
   */
  public static <T> T[] toArray(RemoteIterator<T> source) throws IOException {
    return (T[]) toList(source).toArray();
  }

  /**
   * Apply an operation to all values of a RemoteIterator.
   * <p></p>
   * If the iterator is an IOStatisticsSource returning a non-null
   * set of statistics, <i>and</i> this classes log is set to DEBUG,
   * then the statistics of the operation are evaluated and logged at
   * debug.
   * <p></p>
   * The number of entries processed is returned, as it is useful to
   * know this, especially during tests or when reporting values
   * to users.
   * @param source iterator source
   * @param consumer consumer of the values.
   * @return the number of elements processed
   * @param <T> type of source
   * @throws IOException if the source RemoteIterator or the consumer raise one.
   */
  public static <T> long foreach(
      RemoteIterator<T> source,
      ConsumerRaisingIOE<? super T> consumer) throws IOException {
    long count = 0;

    while (source.hasNext()) {
      count++;
      consumer.accept(source.next());
    }

    // maybe log the results
    logIOStatisticsAtDebug(LOG, "RemoteIterator Statistics: {}", source);
    return count;
  }

  /**
   * Create an iterator from an iterator and a transformation function.
   * @param <S> source type
   * @param <T> result type
   * @param iterator source
   * @param mapper transformation
   * @return a remote iterator
   */
  public static <S, T> RemoteIterator<T> mappingRemoteIterator(
      RemoteIterator<S> iterator,
      FunctionRaisingIOE<? super S, T> mapper) {
    return new MappingRemoteIterator<>(iterator, mapper);
  }

  /**
   * Create an iterator from an iterator and a filter.
   * <p></p>
   * Elements are filtered in the hasNext() method; if not used
   * the filtering will be done on demand in the {@code next()}
   * call.
   * @param <S> type
   * @param iterator source
   * @param filter filter
   * @return a remote iterator
   */
  public static <S> RemoteIterator<S> filteringRemoteIterator(
      RemoteIterator<S> iterator,
      FunctionRaisingIOE<? super S, Boolean> filter) {
    return new FilteringRemoteIterator<>(iterator, filter);
  }

  /**
   * A remote iterator from a singleton. It has a single next()
   * value, after which hasNext() returns false and next() fails.
   * <p></p>
   * If it is a source of
   * remote statistics, these are returned.
   * @param <T> type.
   */
  private static final class SingletonIterator<T>
      implements RemoteIterator<T>, IOStatisticsSource {

    /**
     * Single entry.
     */
    private T singleton;

    /**
     * Instantiate.
     * @param singleton single value...may be null
     */
    private SingletonIterator(@Nullable T singleton) {
      this.singleton = singleton;
    }

    @Override
    public boolean hasNext() throws IOException {
      return singleton != null;
    }

    @SuppressWarnings("NewExceptionWithoutArguments")
    @Override
    public T next() throws IOException {
      if (hasNext()) {
        T r = singleton;
        singleton = null;
        return r;
      } else {
        throw new NoSuchElementException();
      }
    }

    @Override
    public IOStatistics getIOStatistics() {
      return retrieveIOStatistics(singleton);
    }

    @Override
    public String toString() {
      return "SingletonIterator{"
          + (singleton != null ? singleton : "")
          + '}';
    }
  }

  /**
   * Create a remote iterator from a simple java.util.Iterator, or
   * an iterable.
   * <p> </p>
   * If the iterator is a source of statistics that is passed through.
   * <p></p>
   * The {@link #close()} will close the source iterator if it is
   * Closeable; it will also do the same if the origin was an iterable
   * and it is closeable. That is needed to support
   * java.nio directory listings.
   * @param <T> iterator type.
   */
  private static final class FromIterable<T>
      implements RemoteIterator<T>, IOStatisticsSource, Closeable {

    private final Iterator<? extends T> source;
    private final Closeable origin;

    private FromIterable(Iterator<? extends T> source) {
      this.source = requireNonNull(source);
      this.origin = null;
    }

    /**
     * Construct from an iterable.
     * If the origin is closeable it will be closed in
     * the close() call.
     * @param origin origin.
     */
    private FromIterable(final Iterable<T> origin) {
      this.source = origin.iterator();
      this.origin = origin instanceof Closeable
          ? (Closeable) origin
          : null;
    }

    @Override
    public boolean hasNext() {
      return source.hasNext();
    }

    @Override
    public T next() {
      return source.next();
    }

    @Override
    public IOStatistics getIOStatistics() {
      return retrieveIOStatistics(source);
    }

    @Override
    public String toString() {
      return "FromIterator{" + source + '}';
    }

    @Override
    public void close() throws IOException {
      try {
        if (source instanceof Closeable) {
          ((Closeable) source).close();
        }
      } finally {
        if (origin != null) {
          origin.close();
        }
      }
    }
  }

  /**
   * Wrapper of another remote iterator; IOStatistics
   * and Closeable methods are passed down if implemented.
   * @param <S> source type
   * @param <T> type of returned value
   */
  private static abstract class WrappingRemoteIterator<S, T>
      implements RemoteIterator<T>, IOStatisticsSource, Closeable {

    /**
     * Source iterator.
     */
    private final RemoteIterator<S> source;

    protected WrappingRemoteIterator(final RemoteIterator<S> source) {
      this.source = requireNonNull(source);
    }

    protected RemoteIterator<S> getSource() {
      return source;
    }

    @Override
    public IOStatistics getIOStatistics() {
      return retrieveIOStatistics(source);
    }

    @Override
    public void close() throws IOException {
      if (source instanceof Closeable) {
        ((Closeable) source).close();
      }
    }
  }

  /**
   * Iterator taking a source and a transformational function.
   * @param <S> source type
   * @param <T> final output type.There
   */
  private static final class MappingRemoteIterator<S, T>
      extends WrappingRemoteIterator<S, T> {

    private final FunctionRaisingIOE<? super S, T> mapper;

    private MappingRemoteIterator(
        RemoteIterator<S> source,
        FunctionRaisingIOE<? super S, T> mapper) {
      super(source);
      this.mapper = requireNonNull(mapper);
    }

    @Override
    public boolean hasNext() throws IOException {
      return getSource().hasNext();
    }

    @Override
    public T next() throws IOException {
      return mapper.apply(getSource().next());
    }

    @Override
    public String toString() {
      return "FunctionRemoteIterator{" + getSource() + '}';
    }
  }

  /**
   * Extend the wrapped iterator by filtering source values out.
   * Only those values for which the filter predicate returns true
   * will be returned.
   * @param <S> type of iterator.
   */
  @SuppressWarnings("NewExceptionWithoutArguments")
  private static final class FilteringRemoteIterator<S>
      extends WrappingRemoteIterator<S, S> {

    /**
     * Filter Predicate.
     * Takes the input type or any superclass.
     */
    private final FunctionRaisingIOE<? super S, Boolean>
        filter;

    /**
     * Next value; will be null if none has been evaluated, or the
     * last one was already returned by next().
     */
    private S next;

    /**
     * An iterator which combines filtering with transformation.
     * All source elements for which filter = true are returned,
     * transformed via the mapper.
     * @param source source iterator.
     * @param filter filter predicate.
     */
    private FilteringRemoteIterator(
        RemoteIterator<S> source,
        FunctionRaisingIOE<? super S, Boolean> filter) {
      super(source);

      this.filter = requireNonNull(filter);
    }

    /**
     * Fetch: retrieve the next value.
     * @return true if a new value was found after filtering.
     * @throws IOException failure in retrieval from source or mapping
     */
    private boolean fetch() throws IOException {
      while (next == null && getSource().hasNext()) {
        S candidate = getSource().next();
        if (filter.apply(candidate)) {
          next = candidate;
          return true;
        }
      }
      return false;
    }

    /**
     * Trigger a fetch if an entry is needed.
     * @return true if there was already an entry return,
     * or there was not but one could then be retrieved.set
     * @throws IOException failure in fetch operation
     */
    @Override
    public boolean hasNext() throws IOException {
      if (next != null) {
        return true;
      }
      return fetch();
    }

    /**
     * Return the next value.
     * Will retrieve the next elements if needed.
     * This is where the mapper takes place.
     * @return true if there is another data element.
     * @throws IOException failure in fetch operation or the transformation.
     * @throws NoSuchElementException no more data
     */
    @Override
    public S next() throws IOException {
      if (hasNext()) {
        S result = next;
        next = null;
        return result;
      }
      throw new NoSuchElementException();
    }

    @Override
    public String toString() {
      return "FilteringRemoteIterator{" + getSource() + '}';
    }
  }
}
