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

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.thirdparty.com.google.common.base.MoreObjects;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class RemoteIterators {

  private RemoteIterators() {
  }

  /**
   * Create an iterator from a singleton.
   *
   * @param singleton instance
   * @param <T>       type
   * @return a remote iterator
   */
  public static <T> RemoteIterator<T> fromSingleton(@Nullable T singleton) {
    return new SingletonIterator<>(singleton);
  }

  /**
   * Create an iterator from an iterable and a transformation function.
   *
   * @param <S>      source type
   * @param <T>      result type
   * @param iterator source
   * @param mapper   transformation
   * @return a remote iterator
   */
  public static <S, T> RemoteIterator<T> fromIterable(Iterable<S> iterator,
      FunctionRaisingIOE<S, T> mapper) {
    return new IterableRemoteIterator<>(iterator, mapper);
  }

  public interface FunctionRaisingIOE<S, T> {

    /**
     * Apply the function.
     *
     * @param s argument 1
     * @return result
     * @throws IOException Any IO failure
     */
    T apply(S s) throws IOException;
  }

  private static final class IterableRemoteIterator<S, T> implements RemoteIterator<T> {
    private final Iterator<S> sourceIterator;
    private final FunctionRaisingIOE<S, T> mapper;

    private IterableRemoteIterator(Iterable<S> source, FunctionRaisingIOE<S, T> mapper) {
      this.sourceIterator = source.iterator();
      this.mapper = mapper;
    }

    @Override
    public boolean hasNext() {
      return sourceIterator.hasNext();
    }

    @Override
    public T next() throws IOException {
      return mapper.apply(sourceIterator.next());
    }
  }

  private static final class SingletonIterator<T> implements RemoteIterator<T> {
    private final T singleton;

    private boolean processed;

    private SingletonIterator(@Nullable T singleton) {
      this.singleton = singleton;
      this.processed = singleton == null;
    }

    @Override
    public boolean hasNext() {
      return !processed;
    }

    @Override
    public T next() {
      if (hasNext()) {
        processed = true;
        return singleton;
      } else {
        throw new NoSuchElementException();
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("singleton", singleton)
          .toString();
    }
  }
}
