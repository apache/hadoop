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
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;

/**
 * A set of remote iterators supporting transformation and filtering,
 * with IOStatisticsSource passthrough.
 * <p></p>
 * This aims to make it straightforward to use lambda-expressions to
 * transform the results of an iterator, without losing the statistics
 * in the process, and to chain the operations together.
 * <p></p>
 * Based on the S3A Listing code, and some some work on moving other code
 * to using iterative listings so as to pick up the statistics.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class RemoteIterators {

  private RemoteIterators() {
  }

  /**
   * Create an iterator from a singleton.
   * @param instance instance
   * @param <T> type
   * @return an iterator
   */
  public static <T> RemoteIterator<T> toRemoteIterator(@Nullable T instance) {
    return new SingletonIterator<>(instance);
  }

  /**
   * Create a remote iterator from a java.util.Iterator.
   * @param <T> type
   * @return an iterator
   */
  public static <T> RemoteIterator<T> toRemoteIterator(Iterator<T> iterator) {
    return new FromIterator<>(iterator);
  }

  /**
   * Create an iterator from an iterator and a transformation function.
   * @param <S> source type
   * @param <T> result type
   * @param iterator source
   * @param mapper transformation
   * @return an iterator
   */
  public static <S, T> RemoteIterator<T> mappingRemoteIterator(
      RemoteIterator<S> iterator,
      FunctionsRaisingIOE.FunctionRaisingIOE<S, T> mapper) {
    return new MappingRemoteIterator<>(iterator, mapper);
  }

  /**
   * Create an iterator from an iterator and a filter.
   * Elements are filtered during the hasNext phase.
   * @param <S> type
   * @param iterator source
   * @param filter filter
   * @return an iterator
   */
  public static <S> RemoteIterator<S> filteringRemoteIterator(
      RemoteIterator<S> iterator,
      FunctionsRaisingIOE.FunctionRaisingIOE<S, Boolean> filter) {
    return new FilteringMappingRemoteIterator<>(iterator, filter, self -> self);
  }

  /**
   * Create an iterator from an iterator, a filter and a
   * transformation function.
   * Elements are filtered during the hasNext phase.
   * The transform operation is applied during next(), and
   * only to elements which the filter accepted.
   * @param <S> source type
   * @param <T> result type
   * @param iterator source
   * @param mapper transformation
   * @return an iterator
   */
  public static <S, T> RemoteIterator<T> filteringMappingRemoteIterator(
      RemoteIterator<S> iterator,
      FunctionsRaisingIOE.FunctionRaisingIOE<S, Boolean> filter,
      FunctionsRaisingIOE.FunctionRaisingIOE<S, T> mapper) {
    return new FilteringMappingRemoteIterator<>(iterator, filter, mapper);
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

    private SingletonIterator(@Nullable T singleton) {
      this.singleton = singleton;
    }

    @Override
    public boolean hasNext() throws IOException {
      return singleton != null;
    }

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
   * Create a remote iterator from a simple java.util.Iterator.
   * <p> </p>
   * if the iterator is a source of statistics that is passed through.
   * @param <T> iterator type.
   */
  private static final class FromIterator<T>
      implements RemoteIterator<T>, IOStatisticsSource {

    private final Iterator<? extends T> source;

    private FromIterator(Iterator<? extends T> source) {
      this.source = requireNonNull(source);
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
  }

  /**
   * Iterator taking a source and a transformational function.
   * @param <S> source type
   * @param <T> final output type.There
   */
  private static final class MappingRemoteIterator<S, T>
      implements RemoteIterator<T>, IOStatisticsSource {

    private final RemoteIterator<S> source;

    private final FunctionsRaisingIOE.FunctionRaisingIOE<S, T> mapper;

    private MappingRemoteIterator(
        RemoteIterator<S> source,
        FunctionsRaisingIOE.FunctionRaisingIOE<S, T> mapper) {
      this.source = requireNonNull(source);
      this.mapper = requireNonNull(mapper);
    }

    @Override
    public boolean hasNext() throws IOException {
      return source.hasNext();
    }

    @Override
    public T next() throws IOException {
      return mapper.apply(source.next());
    }

    @Override
    public IOStatistics getIOStatistics() {
      return retrieveIOStatistics(source);
    }

    @Override
    public String toString() {
      return "FunctionRemoteIterator{" + source + '}';
    }
  }

  @SuppressWarnings("NewExceptionWithoutArguments")
  private static final class FilteringMappingRemoteIterator<S, T>
      implements RemoteIterator<T>, IOStatisticsSource {

    /**
     * Source iterator.
     */
    private final RemoteIterator<S> source;

    private final FunctionsRaisingIOE.FunctionRaisingIOE<S, Boolean> filter;

    private final FunctionsRaisingIOE.FunctionRaisingIOE<S, T> mapper;

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
     * @param mapper mapper function.
     */
    private FilteringMappingRemoteIterator(
        RemoteIterator<S> source,
        FunctionsRaisingIOE.FunctionRaisingIOE<S, Boolean> filter,
        FunctionsRaisingIOE.FunctionRaisingIOE<S, T> mapper) {
      this.source = requireNonNull(source);
      this.filter = requireNonNull(filter);
      this.mapper = requireNonNull(mapper);
    }

    /**
     * Fetch: retrieve the next value.
     * @return true if a new value was found after filtering.
     * @throws IOException failure in retrieval from source or mapping
     */
    private boolean fetch() throws IOException {
      while (next == null && source.hasNext()) {
        S candidate = source.next();
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
    public T next() throws IOException {
      if (hasNext()) {
        T result = mapper.apply(next);
        next = null;
        return result;
      }
      throw new NoSuchElementException();
    }

    @Override
    public IOStatistics getIOStatistics() {
      return retrieveIOStatistics(source);
    }

    @Override
    public String toString() {
      return "FilteringFunctionIterator{" + source + '}';
    }
  }
}
