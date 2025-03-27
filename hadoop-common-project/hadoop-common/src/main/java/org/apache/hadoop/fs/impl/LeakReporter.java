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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.functional.RunnableRaisingIOE;

import static java.util.Objects.requireNonNull;

/**
 * A class to report leaks of streams.
 * <p>
 * It is created during object creation, and closed during finalization.
 * Predicates should be supplied for the {@link #isOpen} probe check if the
 * resource is still open, and an operation to actually close the
 * target.
 */
public class LeakReporter implements Closeable {

  /**
   * Name of logger used to report leaks: {@value}.
   */
  public static final String RESOURCE_LEAKS_LOG_NAME = "org.apache.hadoop.fs.resource.leaks";

  /**
   * Special log for leaked streams.
   */
  private static final Logger LEAK_LOG =
      LoggerFactory.getLogger(RESOURCE_LEAKS_LOG_NAME);

  /**
   * Format string used to build the thread information: {@value}.
   */
  @VisibleForTesting
  static final String THREAD_FORMAT =
      "; thread: %s; id: %d";

  /**
   * Re-entrancy check.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Predicate to check if the resource is open.
   */
  private final BooleanSupplier isOpen;

  /**
   * Action to close the resource.
   */
  private final RunnableRaisingIOE closeAction;

  /**
   * Stack trace of object creation; used to
   * report of unclosed streams in finalize().
   */
  private final IOException leakException;

  /**
   * Constructor.
   * <p>
   * Validates the parameters and builds the stack;
   * append "; thread: " + thread name.
   * @param message error message
   * @param isOpen open predicate
   * @param closeAction action to close
   */
  public LeakReporter(
      final String message,
      final BooleanSupplier isOpen,
      final RunnableRaisingIOE closeAction) {
    this.isOpen = requireNonNull(isOpen);
    this.closeAction = requireNonNull(closeAction);
    // build the warning thread.
    // This includes the error string to print, so as to avoid
    // constructing objects in finalize().
    this.leakException = new IOException(message
        + String.format(THREAD_FORMAT,
        Thread.currentThread().getName(),
        Thread.currentThread().getId()));
  }

  /**
   * Close the resource.
   */
  @Override
  public void close() {
    try {
      if (!closed.getAndSet(true) && isOpen.getAsBoolean()) {
        // log a warning with the creation stack
        LEAK_LOG.warn(leakException.getMessage());
        // The creation stack is logged at INFO, so that
        // it is possible to configure logging to print
        // the name of files left open, without printing
        // the stacks. This is better for production use.

        LEAK_LOG.info("stack", leakException);
        closeAction.apply();
      }
    } catch (Exception e) {
      LEAK_LOG.info("executing leak cleanup actions", e);
    }
  }

  public IOException getLeakException() {
    return leakException;
  }

  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public String toString() {
    return "LeakReporter{" +
        "closed=" + closed.get() +
        '}';
  }
}
