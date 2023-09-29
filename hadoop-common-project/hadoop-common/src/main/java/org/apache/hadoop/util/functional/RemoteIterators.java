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

package org.apache.hadoop.util.functional;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.io.IOUtils;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.logIOStatisticsAtDebug;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;

/**
 * A set of remote iterators supporting transformation and filtering,
 * with IOStatisticsSource passthrough, and of conversions of
 * the iterators to lists/arrays and of performing actions
 * on the values.
 * <p>
 * This aims to make it straightforward to use lambda-expressions to
 * transform the results of an iterator, without losing the statistics
 * in the process, and to chain the operations together.
 * </p>
 * The closeable operation will be passed through RemoteIterators which
 * wrap other RemoteIterators. This is to support any iterator which
 * can be closed to release held connections, file handles etc.
 * Unless client code is written to assume that RemoteIterator instances
 * may be closed, this is not likely to be broadly used. It is added
 * to make it possible to adopt this feature in a managed way.
 * <p>
 * One notable feature is that the
 * {@link #foreach(RemoteIterator, ConsumerRaisingIOE)} method will
 * LOG at debug any IOStatistics provided by the iterator, if such
 * statistics are provided. There's no attempt at retrieval and logging
 * if the LOG is not set to debug, so it is a zero cost feature unless
 * the logger {@code org.apache.hadoop.fs.functional.RemoteIterators}
 * is at DEBUG.
 * </p>
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
  public static <T> RemoteIterator<T> remoteIteratorFromSingleton(
      @Nullable T singleton) {
    return new SingletonIterator<>(singleton);
  }

  /**
   * Create a remote iterator from a java.util.Iterator.
   * @param <T> type
   * @param iterator iterator.
   * @return a remote iterator
   */
  public static <T> RemoteIterator<T> remoteIteratorFromIterator(
      Iterator<T> iterator) {
    return new WrappedJavaIterator<>(iterator);
  }

  /**
   * Create a remote iterator from a java.util.Iterable -e.g. a list
   * or other collection.
   * @param <T> type
   * @param iterable iterable.
   * @return a remote iterator
   */
  public static <T> RemoteIterator<T> remoteIteratorFromIterable(
      Iterable<T> iterable) {
    return new WrappedJavaIterator<>(iterable.iterator());
  }

  /**
   * Create a remote iterator from an array.
   * @param <T> type
   * @param array array.
   * @return a remote iterator
   */
  public static <T> RemoteIterator<T> remoteIteratorFromArray(T[] array) {
    return new WrappedJavaIterator<>(Arrays.stream(array).iterator());
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
   * Create a RemoteIterator from a RemoteIterator, casting the
   * type in the process. This is to help with filesystem API
   * calls where overloading causes confusion (e.g. listStatusIterator())
   * @param <S> source type
   * @param <T> result type
   * @param iterator source
   * @return a remote iterator
   */
  public static <S, T> RemoteIterator<T> typeCastingRemoteIterator(
      RemoteIterator<S> iterator) {
    return new TypeCastingRemoteIterator<>(iterator);
  }

  /**
   * Create a RemoteIterator from a RemoteIterator and a filter
   * function which returns true for every element to be passed
   * through.
   * <p>
   * Elements are filtered in the hasNext() method; if not used
   * the filtering will be done on demand in the {@code next()}
   * call.
   * </p>
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
   * This adds an extra close operation alongside the passthrough
   * to any Closeable.close() method supported by the source iterator.
   * @param iterator source
   * @param toClose extra object to close.
   * @param <S> source type.
   * @return a new iterator
   */
  public static <S> RemoteIterator<S> closingRemoteIterator(
      RemoteIterator<S> iterator,
      Closeable toClose) {
    return new CloseRemoteIterator<>(iterator, toClose);
  }

  /**
   * Wrap an iterator with one which adds a continuation probe.
   * This allows work to exit fast without complicated breakout logic
   * @param iterator source
   * @param continueWork predicate which will trigger a fast halt if it returns false.
   * @param <S> source type.
   * @return a new iterator
   */
  public static <S> RemoteIterator<S> haltableRemoteIterator(
      final RemoteIterator<S> iterator,
      final CallableRaisingIOE<Boolean> continueWork) {
    return new HaltableRemoteIterator<>(iterator, continueWork);
  }

  /**
   * A remote iterator which simply counts up, stopping once the
   * value is greater than the value of {@code excludedFinish}.
   * This is primarily for tests or when submitting work into a TaskPool.
   * equivalent to
   * <pre>
   *   for(long l = start, l &lt; excludedFinish; l++) yield l;
   * </pre>
   * @param start start value
   * @param excludedFinish excluded finish
   * @return an iterator which returns longs from [start, finish)
   */
  public static RemoteIterator<Long> rangeExcludingIterator(
      final long start, final long excludedFinish) {
    return new RangeExcludingLongIterator(start, excludedFinish);
  }

  /**
   * Build a list from a RemoteIterator.
   * @param source source iterator
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
   * @param source source iterator
   * @param a destination array; if too small a new array
   * of the same type is created
   * @param <T> type
   * @return an array of the values.
   * @throws IOException if the source RemoteIterator raises it.
   */
  public static <T> T[] toArray(RemoteIterator<T> source,
      T[] a) throws IOException {
    List<T> list = toList(source);
    return list.toArray(a);
  }

  /**
   * Apply an operation to all values of a RemoteIterator.
   *
   * If the iterator is an IOStatisticsSource returning a non-null
   * set of statistics, <i>and</i> this classes log is set to DEBUG,
   * then the statistics of the operation are evaluated and logged at
   * debug.
   * <p>
   * The number of entries processed is returned, as it is useful to
   * know this, especially during tests or when reporting values
   * to users.
   * </p>
   * This does not close the iterator afterwards.
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

    try {
      while (source.hasNext()) {
        count++;
        consumer.accept(source.next());
      }

    } finally {
      cleanupRemoteIterator(source);
    }
    return count;
  }

  /**
   * Clean up after an iteration.
   * If the log is at debug, calculate and log the IOStatistics.
   * If the iterator is closeable, cast and then cleanup the iterator
   * @param source iterator source
   * @param <T> type of source
   */
  public static <T> void cleanupRemoteIterator(RemoteIterator<T> source) {
    // maybe log the results
    logIOStatisticsAtDebug(LOG, "RemoteIterator Statistics: {}", source);
    if (source instanceof Closeable) {
      /* source is closeable, so close.*/
      IOUtils.cleanupWithLogger(LOG, (Closeable) source);
    }
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
    private final T singleton;

    /** Has the entry been processed?  */
    private boolean processed;

    /**
     * Instantiate.
     * @param singleton single value...may be null
     */
    private SingletonIterator(@Nullable T singleton) {
      this.singleton = singleton;
      // if the entry is null, consider it processed.
      this.processed = singleton == null;
    }

    @Override
    public boolean hasNext() throws IOException {
      return !processed;
    }

    @SuppressWarnings("NewExceptionWithoutArguments")
    @Override
    public T next() throws IOException {
      if (hasNext()) {
        processed = true;
        return singleton;
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
   * Closeable;
   * @param <T> iterator type.
   */
  private static final class WrappedJavaIterator<T>
      implements RemoteIterator<T>, IOStatisticsSource, Closeable {

    /**
     * inner iterator..
     */
    private final Iterator<? extends T> source;

    private final Closeable sourceToClose;


    /**
     * Construct from an interator.
     * @param source source iterator.
     */
    private WrappedJavaIterator(Iterator<? extends T> source) {
      this.source = requireNonNull(source);
      sourceToClose = new MaybeClose(source);
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
      sourceToClose.close();

    }
  }

  /**
   * Wrapper of another remote iterator; IOStatistics
   * and Closeable methods are passed down if implemented.
   * This class may be subclassed within the hadoop codebase
   * if custom iterators are needed.
   * @param <S> source type
   * @param <T> type of returned value
   */
  public static abstract class WrappingRemoteIterator<S, T>
      implements RemoteIterator<T>, IOStatisticsSource, Closeable {

    /**
     * Source iterator.
     */
    private final RemoteIterator<S> source;

    private final Closeable sourceToClose;

    protected WrappingRemoteIterator(final RemoteIterator<S> source) {
      this.source = requireNonNull(source);
      sourceToClose = new MaybeClose(source);
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
      sourceToClose.close();
    }

    /**
     * Check for the source having a next element.
     * If it does not, this object's close() method
     * is called and false returned
     * @return true if there is a new value
     * @throws IOException failure to retrieve next value
     */
    protected boolean sourceHasNext() throws IOException {
      boolean hasNext;
      try {
        hasNext = getSource().hasNext();
      } catch (IOException e) {
        IOUtils.cleanupWithLogger(LOG, this);
        throw e;
      }
      if (!hasNext) {
        // there is nothing less so automatically close.
        close();
      }
      return hasNext;
    }

    /**
     * Get the next source value.
     * This calls {@link #sourceHasNext()} first to verify
     * that there is data.
     * @return the next value
     * @throws IOException failure
     * @throws NoSuchElementException no more data
     */
    protected S sourceNext() throws IOException {
      try {
        if (!sourceHasNext()) {
          throw new NoSuchElementException();
        }
        return getSource().next();
      } catch (NoSuchElementException | IOException e) {
        IOUtils.cleanupWithLogger(LOG, this);
        throw e;
      }
    }

    @Override
    public String toString() {
      return source.toString();
    }

  }

  /**
   * Iterator taking a source and a transformational function.
   * @param <S> source type
   * @param <T> final output type.There
   */
  private static final class MappingRemoteIterator<S, T>
      extends WrappingRemoteIterator<S, T> {

    /**
     * Mapper to invoke.
     */
    private final FunctionRaisingIOE<? super S, T> mapper;

    private MappingRemoteIterator(
        RemoteIterator<S> source,
        FunctionRaisingIOE<? super S, T> mapper) {
      super(source);
      this.mapper = requireNonNull(mapper);
    }

    @Override
    public boolean hasNext() throws IOException {
      return sourceHasNext();
    }

    @Override
    public T next() throws IOException {
      return mapper.apply(sourceNext());
    }

    @Override
    public String toString() {
      return "FunctionRemoteIterator{" + getSource() + '}';
    }
  }

  /**
   * RemoteIterator which can change the type of the input.
   * This is useful in some situations.
   * @param <S> source type
   * @param <T> final output type.
   */
  private static final class TypeCastingRemoteIterator<S, T>
      extends WrappingRemoteIterator<S, T> {

    private TypeCastingRemoteIterator(
        RemoteIterator<S> source) {
      super(source);
    }

    @Override
    public boolean hasNext() throws IOException {
      return sourceHasNext();
    }

    @Override
    public T next() throws IOException {
      return (T)sourceNext();
    }

    @Override
    public String toString() {
      return getSource().toString();
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
      while (next == null && sourceHasNext()) {
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

  /**
   * A wrapping remote iterator which adds another entry to
   * close. This is to assist cleanup.
   * @param <S> type
   */
  private static final class CloseRemoteIterator<S>
      extends WrappingRemoteIterator<S, S> {

    private final MaybeClose toClose;
    private boolean closed;

    private CloseRemoteIterator(
        final RemoteIterator<S> source,
        final Closeable toClose) {
      super(source);
      this.toClose = new MaybeClose(Objects.requireNonNull(toClose));
    }

    @Override
    public boolean hasNext() throws IOException {
      return sourceHasNext();
    }

    @Override
    public S next() throws IOException {

      return sourceNext();
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      LOG.debug("Closing {}", this);
      try {
        super.close();
      } finally {
        toClose.close();
      }
    }
  }

  /**
   * Class to help with Closeable logic, where sources may/may not
   * be closeable, only one invocation is allowed.
   * On the second and later call of close(), it is a no-op.
   */
  private static final class MaybeClose implements Closeable {

    private Closeable toClose;

    /**
     * Construct.
     * @param o object to close.
     */
    private MaybeClose(Object o) {
      this(o, true);
    }

    /**
     * Construct -close the object if it is closeable and close==true.
     * @param o object to close.
     * @param close should close?
     */
    private MaybeClose(Object o, boolean close) {
      if (close && o instanceof Closeable) {
        this.toClose = (Closeable) o;
      } else {
        this.toClose = null;
      }
    }

    @Override
    public void close() throws IOException {
      if (toClose != null) {
        try {
          toClose.close();
        } finally {
          toClose = null;
        }
      }
    }
  }

  /**
   * An iterator which allows for a fast exit predicate.
   * @param <S> source type
   */
  private static final class HaltableRemoteIterator<S>
      extends WrappingRemoteIterator<S, S> {

    /**
     * Probe as to whether work should continue.
     */
    private final CallableRaisingIOE<Boolean> continueWork;

    /**
     * Wrap an iterator with one which adds a continuation probe.
     * The probe will be called in the {@link #hasNext()} method, before
     * the source iterator is itself checked and in {@link #next()}
     * before retrieval.
     * That is: it may be called multiple times per iteration.
     * @param source source iterator.
     * @param continueWork predicate which will trigger a fast halt if it returns false.
     */
    private HaltableRemoteIterator(
        final RemoteIterator<S> source,
        final CallableRaisingIOE<Boolean> continueWork) {
      super(source);
      this.continueWork = continueWork;
    }

    @Override
    public boolean hasNext() throws IOException {
      return sourceHasNext();
    }

    @Override
    public S next() throws IOException {
      return sourceNext();
    }

    @Override
    protected boolean sourceHasNext() throws IOException {
      return continueWork.apply() && super.sourceHasNext();
    }
  }

  /**
   * A remote iterator which simply counts up, stopping once the
   * value is greater than the finish.
   * This is primarily for tests or when submitting work into a TaskPool.
   */
  private static final class RangeExcludingLongIterator implements RemoteIterator<Long> {

    /**
     * Current value.
     */
    private long current;

    /**
     * End value.
     */
    private final long excludedFinish;

    /**
     * Construct.
     * @param start start value.
     * @param excludedFinish halt the iterator once the current value is equal
     *          to or greater than this.
     */
    private RangeExcludingLongIterator(final long start, final long excludedFinish) {
      this.current = start;
      this.excludedFinish = excludedFinish;
    }

    @Override
    public boolean hasNext() throws IOException {
      return current < excludedFinish;
    }

    @Override
    public Long next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final long s = current;
      current++;
      return s;
    }
  }

}
