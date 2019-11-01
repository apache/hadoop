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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;

import static org.apache.hadoop.fs.FSExceptionMessages.STREAM_IS_CLOSED;

/**
 * Models a stream's state and can be used for checking this state before
 * any operation, and for using a lock to manage exclusive access to operations
 * within the stream.
 *
 * The model designed to ensure that a stream knows when it is closed,
 * and, once it has entered a failed state, that the failure
 * is not forgotten.
 *
 * This state model is based on the design of
 * {@code org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream} and
 * {@code org.apache.hadoop.fs.azure.PageBlobOutputStream} which both
 * store and rethrow any previously raised error.
 *
 *
 * The model has three states: Open, Error, and Closed:
 *
 * <pre>
 *   {@link State#Open}: caller can interact with the stream.
 *   {@link State#Error}: all operations will raise the previously recorded exception.
 *   {@link State#Closed}: operations will be rejected.
 * </pre>

 * When an instance of the model is created, it is in {@link State#Open};
 * a call to {@link #enterClosedState()} will move it from Open to Closed;
 *
 *
 * <p>
 * The lock/unlock operation relays to {@code java.util.concurrent.locks.Lock}
 * and has a similar usage pattern. A key difference is that a check for the
 * stream being open can be integrated with the lock acquisition.
 * If {@link #acquireLock(boolean)} is called with checkOpen == true, then
 * the caller will know that after the lock is granted then the stream
 * is open.
 * <pre>
 *   model.acquireLock(true);
 *   try {
 *     (do some work)
 *   } catch (IOException e) {
 *     model.enterErrorState(e);
 *   } finally {
 *     model.releaseLock();
 *   }
 * </pre>
 *
 */
@InterfaceAudience.LimitedPrivate("Filesystems")
@InterfaceStability.Unstable
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
   */
  private final AtomicReference<State> state =
      new AtomicReference<>(State.Open);

  /** Any exception to raise on the next checkOpen call. */
  private IOException exception;

  /**
   * Create for a path. 
   * @param path optional path for exception messages.
   */
  public StreamStateModel(@Nullable Path path) {
    this.path = path != null ? path.toString() : "";
  }

  /**
   * Create for a path. 
   * @param path optional path for exception messages.
   */  
  public StreamStateModel(@Nullable final String path) {
    this.path = path;
  }

  /**
   * Get the current state.
   * Not synchronized; lock if you want consistency across calls.
   * @return the current state.
   */
  public State getState() {
    return state.get();
  }

  /**
   * Change state to closed. No-op if the state was in closed or error.
   * @return true if the state changed.
   */
  public synchronized boolean enterClosedState() {
    return state.compareAndSet(State.Open, State.Closed);
  }

  /**
   * Change state to error and stores first error so it can be re-thrown.
   * If already in {@link State#Error}: return the previous exception.
   * @param ex the exception to record
   * @return the exception set when the error state was entered.
   */
  public synchronized IOException enterErrorState(final IOException ex) {
    Preconditions.checkArgument(ex != null, "Null exception");
    switch (state.get()) {
      // a stream can go into the error state when open or closed
    case Open:
    case Closed:
      exception = ex;
      state.set(State.Error);
      break;
    case Error:
      // already in this state; retain the previous exception.
      break;
    }
    return exception;
  }

  /**
   * Verify that a stream is open, throwing an IOException if
   * not.
   * If in an error state: rethrow that exception.
   * If closed, throw an IOException with
   * {@link FSExceptionMessages#STREAM_IS_CLOSED} in the message.
   * @throws IOException if the stream is not in {@link State#Open}.
   * @throws PathIOException if the stream was not open and a path was given
   * in the constructor.
   */
  public synchronized void checkOpen() throws IOException {
    switch (state.get()) {
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
   * Probe for the model being in a specific state.
   * @param probe state to probe for
   * @return true if at the time of the check, the service
   * was in the given state.
   */
  public boolean isInState(State probe) {
    return state.get().equals(probe);
  }

  /**
   * If the model is in {@link State#Error} throw the exception.
   * @throws IOException if one was caught earlier.
   */
  public synchronized void throwAnyExceptionRaised() throws IOException {
    if (exception != null) {
      throw exception;
    }
  }

  /**
   * Get any exception. Non-null iff the model is in the error state.
   * @return any exception set on a transition to the error state.
   */
  public synchronized IOException getException() {
    return exception;
  }

  /**
   * Acquire an exclusive lock.
   * @param checkOpen must the stream be open?
   * @throws IOException if the stream is in error state or
   * {@code checkOpen == true} and the stream is closed.
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

}
