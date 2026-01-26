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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.apache.hadoop.util.Preconditions.checkState;

/**
 * A subclass of {@link LazyAtomicReference} which
 * holds an {@code AutoCloseable} reference and calls {@code close()}
 * when it itself is closed.
 * @param <T> type of reference.
 */
public class LazyAutoCloseableReference<T extends AutoCloseable>
    extends LazyAtomicReference<T> implements AutoCloseable {

  /** Closed flag. */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Constructor for this instance.
   * @param constructor method to invoke to actually construct the inner object.
   */
  public LazyAutoCloseableReference(final CallableRaisingIOE<? extends T> constructor) {
    super(constructor);
  }

  /**
   * {@inheritDoc}
   * @throws IllegalStateException if the reference is closed.
   */
  @Override
  public synchronized T eval() throws IOException {
    checkState(!closed.get(), "Reference is closed");
    return super.eval();
  }

  /**
   * Is the reference closed?
   * @return true if the reference is closed.
   */
  public boolean isClosed() {
    return closed.get();
  }

  /**
   * Close the reference value if it is non-null.
   * Sets the reference to null afterwards, even on
   * a failure.
   * @throws Exception failure to close.
   */
  @Override
  public synchronized void close() throws Exception {
    if (closed.getAndSet(true)) {
      // already closed
      return;
    }
    final T v = getReference().get();
    // check the state.
    // A null reference means it has not yet been evaluated,
    if (v != null) {
      try {
        v.close();
      } finally {
        // set the reference to null, even on a failure.
        getReference().set(null);
      }
    }
  }


  /**
   * Create from a supplier.
   * This is not a constructor to avoid ambiguity when a lambda-expression is
   * passed in.
   * @param supplier supplier implementation.
   * @return a lazy reference.
   * @param <T> type of reference
   */
  public static <T extends AutoCloseable> LazyAutoCloseableReference<T> lazyAutoCloseablefromSupplier(Supplier<T> supplier) {
    return new LazyAutoCloseableReference<>(supplier::get);
  }
}
