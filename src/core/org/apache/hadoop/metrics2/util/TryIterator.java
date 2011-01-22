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

package org.apache.hadoop.metrics2.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A base class for unmodifiable iterators (throws on remove)
 *
 * This class also makes writing filtering iterators easier, where the only
 * way to discover the end of data is by trying to read it. The same applies
 * to writing iterator wrappers around stream read calls.
 *
 * One only needs to implement the tryNext() method and call done() when done.
 *
 * @param <T> the type of the iterator
 */
public abstract class TryIterator<T> implements Iterator<T> {

  enum State {
    PENDING,  // Ready to tryNext().
    GOT_NEXT, // Got the next element from tryNext() and yet to return it.
    DONE,     // Done/finished.
    FAILED,   // An exception occurred in the last op.
  }

  private State state = State.PENDING;
  private T next;

  /**
   * Return the next element. Must call {@link #done()} when done, otherwise
   * infinite loop could occur. If this method throws an exception, any
   * further attempts to use the iterator would result in an
   * {@link IllegalStateException}.
   *
   * @return the next element if there is one or return {@link #done()}
   */
  protected abstract T tryNext();

  /**
   * Implementations of {@link #tryNext} <b>must</b> call this method
   * when there are no more elements left in the iteration.
   *
   * @return  null as a convenience to implement {@link #tryNext()}
   */
  protected final T done() {
    state = State.DONE;
    return null;
  }

  /**
   * @return  true if we have a next element or false otherwise.
   */
  public final boolean hasNext() {
    if (state == State.FAILED)
      throw new IllegalStateException();

    switch (state) {
      case DONE:      return false;
      case GOT_NEXT:  return true;
      default:
    }

    // handle tryNext
    state = State.FAILED; // just in case
    next = tryNext();

    if (state != State.DONE) {
      state = State.GOT_NEXT;
      return true;
    }
    return false;
  }

  /**
   * @return  the next element if we have one.
   */
  public final T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    state = State.PENDING;
    return next;
  }

  /**
   * @return the current element without advancing the iterator
   */
  public final T current() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return next;
  }

  /**
   * Guaranteed to throw UnsupportedOperationException
   */
  public final void remove() {
    throw new UnsupportedOperationException("Not allowed.");
  }

}
