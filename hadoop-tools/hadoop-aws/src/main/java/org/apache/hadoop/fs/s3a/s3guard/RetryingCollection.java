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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.impl.WrappedIOException;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;

/**
 * A collection which wraps the result of a query or scan
 * with retries; the {@link #scanThrottleEvents} count is
 * then updated.
 * Important: iterate through this only once; the outcome
 * of repeating an iteration is "undefined"
 * @param <T> type of outcome.
 */
class RetryingCollection<T>
    implements Iterable<T> {

  /**
   * Source iterable.
   */
  private final Iterable<T> source;

  /**
   * Invoker for retries.
   */
  private Invoker invoker;

  RetryingCollection(
      final Invoker invoker,
      final Iterable<T> source) {
    this.source = source;
    this.invoker = invoker;
  }


  @Override
  public Iterator<T> iterator() {
    return new RetryingIterator<>(invoker, source.iterator());
  }

  /**
   * An iterator which wraps a non-retrying iterator of scan results
   * (i.e {@code S3GuardTableAccess.DDBPathMetadataIterator}.
   */
  private static final class RetryingIterator<T> implements Iterator<T> {

    private final Iterator<T> source;

    private Invoker scanOp;

    private RetryingIterator(final Invoker scanOp,
        final Iterator<T> source) {
      this.source = source;
      this.scanOp = scanOp;
    }

    /**
     * {@inheritDoc}.
     * @throws WrappedIOException for IO failure, including throttling.
     */
    @Override
    @Retries.RetryTranslated
    public boolean hasNext() {
      try {
        return scanOp.retry(
            "Scan Dynamo",
            null,
            true,
            source::hasNext);
      } catch (IOException e) {
        throw new WrappedIOException(e);
      }
    }

    /**
     * {@inheritDoc}.
     * @throws WrappedIOException for IO failure, including throttling.
     */
    @Override
    @Retries.RetryTranslated
    public T next() {
      try {
        return scanOp.retry(
            "Scan Dynamo",
            null,
            true,
            source::next);
      } catch (IOException e) {
        throw new WrappedIOException(e);
      }
    }
  }

}
