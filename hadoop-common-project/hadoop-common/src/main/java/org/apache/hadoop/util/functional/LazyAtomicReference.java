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
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.util.functional.FunctionalIO.uncheckIOExceptions;

/**
 * A lazily constructed reference, whose reference
 * constructor is a {@link CallableRaisingIOE} so
 * may raise IOExceptions.
 * <p>
 * This {@code constructor}  is only invoked on demand
 * when the reference is first needed,
 * after which the same value is returned.
 * This value MUST NOT be null.
 * <p>
 * Implements {@link CallableRaisingIOE} and {@code java.util.function.Supplier}.
 * An instance of this can therefore  be used in a functional IO chain.
 * As such, it can act as a delayed and caching invocator of a function:
 * the supplier passed in is only ever invoked once, and only when requested.
 * @param <T> type of reference
 */
public class LazyAtomicReference<T>
    implements CallableRaisingIOE<T>, Supplier<T> {

  /**
   * Underlying reference.
   */
  private final AtomicReference<T> reference = new AtomicReference<>();

  /**
   * Constructor for lazy creation.
   */
  private final CallableRaisingIOE<? extends T> constructor;

  /**
   * Constructor for this instance.
   * @param constructor method to invoke to actually construct the inner object.
   */
  public LazyAtomicReference(final CallableRaisingIOE<? extends T> constructor) {
    this.constructor = requireNonNull(constructor);
  }

  /**
   * Getter for the constructor.
   * @return the constructor class
   */
  protected CallableRaisingIOE<? extends T> getConstructor() {
    return constructor;
  }

  /**
   * Get the reference.
   * Subclasses working with this need to be careful working with this.
   * @return the reference.
   */
  protected AtomicReference<T> getReference() {
    return reference;
  }

  /**
   * Get the value, constructing it if needed.
   * @return the value
   * @throws IOException on any evaluation failure
   * @throws NullPointerException if the evaluated function returned null.
   */
  public synchronized T eval() throws IOException {
    final T v = reference.get();
    if (v != null) {
      return v;
    }
    reference.set(requireNonNull(constructor.apply()));
    return reference.get();
  }

  /**
   * Implementation of {@code CallableRaisingIOE.apply()}.
   * Invoke {@link #eval()}.
   * @return the value
   * @throws IOException on any evaluation failure
   */
  @Override
  public final T apply() throws IOException {
    return eval();
  }

  /**
   * Implementation of {@code Supplier.get()}.
   * <p>
   * Invoke {@link #eval()} and convert IOEs to
   * UncheckedIOException.
   * <p>
   * This is the {@code Supplier.get()} implementation, which allows
   * this class to passed into anything taking a supplier.
   * @return the value
   * @throws UncheckedIOException if the constructor raised an IOException.
   */
  @Override
  public final T get() throws UncheckedIOException {
    return uncheckIOExceptions(this::eval);
  }

  /**
   * Is the reference set?
   * @return true if the reference has been set.
   */
  public final boolean isSet() {
    return reference.get() != null;
  }

  @Override
  public String toString() {
    return "LazyAtomicReference{" +
        "reference=" + reference + '}';
  }


  /**
   * Create from a supplier.
   * This is not a constructor to avoid ambiguity when a lambda-expression is
   * passed in.
   * @param supplier supplier implementation.
   * @return a lazy reference.
   * @param <T> type of reference
   */
  public static <T> LazyAtomicReference<T> lazyAtomicReferenceFromSupplier(
      Supplier<T> supplier) {
    return new LazyAtomicReference<>(supplier::get);
  }
}
