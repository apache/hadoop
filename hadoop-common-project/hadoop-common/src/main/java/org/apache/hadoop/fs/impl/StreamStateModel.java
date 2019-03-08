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

package org.apache.hadoop.fs.impl;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;

import static org.apache.hadoop.fs.FSExceptionMessages.STREAM_IS_CLOSED;

/**
 * Models a stream's state and can be used for checking this state before
 * any operation.
 *
 * The model has three states: Open, Error, and Closed,
 *
 * <pre>
 *   Open: caller can interact with the stream.
 *   Error: all operations will raise the previously recorded exception.
 *   Closed: operations will be rejected.
 * </pre>
 */
public class StreamStateModel {

  /**
   * States of the stream.
   */
  public enum State {

    /**
     * Stream is open.
     */
    Open,

    /**
     * Stream is in an error state.
     * It is not expected to recover from this.
     */
    Error,

    /**
     * Stream is now closed. Operations will fail.
     */
    Closed
  }

  /**
   * Path; if not empty then a {@link PathIOException} will be raised
   * containing this path.
   */
  private final String path;

  /** Lock. Not considering an InstrumentedWriteLock, but it is an option. */
  private final Lock lock = new ReentrantLock();

  /**
   * Initial state: open.
   * This is volatile: it can be queried without encountering any locks.
   * However, to guarantee the state is constant through the life of an
   * operation, updates must be through the synchronized methods.
   */
  private volatile State state = State.Open;

  /** Any exception to raise on the next checkOpen call. */
  private IOException exception;

  public StreamStateModel(final Path path) {
    this.path = path.toString();
  }

  public StreamStateModel(final String path) {
    this.path = path;
  }

  /**
   * Get the current state.
   * Not synchronized; lock if you want consistency across calls.
   * @return the current state.
   */
  public State getState() {
    return state;
  }

  /**
   * Change state to closed. No-op if the state was in closed or error
   * @return true if the state changed.
   */
  public synchronized boolean enterClosedState() {
    if (state == State.Open) {
      state = State.Closed;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Change state to error and stores first error so it can be re-thrown.
   * If already in error: return previous exception.
   * @param ex the exception to record
   * @return the exception set when the error state was entered.
   */
  public synchronized IOException enterErrorState(final IOException ex) {
    Preconditions.checkArgument(ex != null, "Null exception");
    switch (state) {
      // a stream can go into the error state when open or closed
    case Open:
    case Closed:
      exception = ex;
      state = State.Error;
      break;
    case Error:
      // already in this state; retain the previous exception.
      break;
    }
    return exception;
  }

  /**
   * Check a stream is open.
   * If in an error state: rethrow that exception. If closed,
   * throw an exception about that.
   * @throws IOException if the stream is not open.
   */
  public synchronized void checkOpen() throws IOException {
    switch (state) {
    case Open:
      return;

    case Error:
      throw exception;

    case Closed:
      if (StringUtils.isNotEmpty(path)) {
        throw new PathIOException(path, STREAM_IS_CLOSED);
      } else {
        throw new IOException(STREAM_IS_CLOSED);
      }
    }
  }

  /**
   * Acquire an exclusive lock.
   * @param checkOpen must the stream be open?
   * @throws IOException if the stream is in error state or checkOpen==true
   * and the stream is closed.
   */
  public void acquireLock(boolean checkOpen) throws IOException {
    // fail fast if the stream is required to be open and it is not
    if (checkOpen) {
      checkOpen();
    }

    // acquire the lock; this may suspend the thread
    lock.lock();

    // now verify that the stream is still open.
    if (checkOpen) {
      checkOpen();
    }
  }

  /**
   * Release the lock.
   */
  public void releaseLock() {
    lock.unlock();
  }

  /**
   * Check for a stream being in a specific state.
   * The check is synchronized, but not locked; if the caller does
   * not hold a lock then the state may change before any subsequent
   * operation.
   * @param expected expected state
   * @return return true iff the steam was in the state at the time
   * of checking.
   */
  public synchronized boolean isInState(State expected) {
    return state == expected;
  }

}
