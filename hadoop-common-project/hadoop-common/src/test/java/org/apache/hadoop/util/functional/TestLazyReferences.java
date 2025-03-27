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
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.verifyCause;
import static org.apache.hadoop.util.Preconditions.checkState;

/**
 * Test {@link LazyAtomicReference} and {@link LazyAutoCloseableReference}.
 */
public class TestLazyReferences extends AbstractHadoopTestBase {

  /**
   * Format of exceptions to raise.
   */
  private static final String GENERATED = "generated[%d]";

  /**
   * Invocation counter, can be asserted on in {@link #assertCounterValue(int)}.
   */
  private final AtomicInteger counter = new AtomicInteger();

  /**
   * Assert that {@link #counter} has a specific value.
   * @param val expected value
   */
  private void assertCounterValue(final int val) {
    assertAtomicIntValue(counter, val);
  }

  /**
   * Assert an atomic integer has a specific value.
   * @param ai atomic integer
   * @param val expected value
   */
  private static void assertAtomicIntValue(
      final AtomicInteger ai, final int val) {
    Assertions.assertThat(ai.get())
        .describedAs("value of atomic integer %s", ai)
        .isEqualTo(val);
  }


  /**
   * Test the underlying {@link LazyAtomicReference} integration with java
   * Supplier API.
   */
  @Test
  public void testLazyAtomicReference() throws Throwable {

    LazyAtomicReference<Integer> ref = new LazyAtomicReference<>(counter::incrementAndGet);

    // constructor does not invoke the supplier
    assertCounterValue(0);

    assertSetState(ref, false);

    // second invocation does not
    Assertions.assertThat(ref.eval())
        .describedAs("first eval()")
        .isEqualTo(1);
    assertCounterValue(1);
    assertSetState(ref, true);


    // Callable.apply() returns the same value
    Assertions.assertThat(ref.apply())
        .describedAs("second get of %s", ref)
        .isEqualTo(1);
    // no new counter increment
    assertCounterValue(1);
  }

  /**
   * Assert that {@link LazyAtomicReference#isSet()} is in the expected state.
   * @param ref reference
   * @param expected expected value
   */
  private static <T> void assertSetState(final LazyAtomicReference<T> ref,
      final boolean expected) {
    Assertions.assertThat(ref.isSet())
        .describedAs("isSet() of %s", ref)
        .isEqualTo(expected);
  }

  /**
   * Test the underlying {@link LazyAtomicReference} integration with java
   * Supplier API.
   */
  @Test
  public void testSupplierIntegration() throws Throwable {

    LazyAtomicReference<Integer> ref = LazyAtomicReference.lazyAtomicReferenceFromSupplier(counter::incrementAndGet);

    // constructor does not invoke the supplier
    assertCounterValue(0);
    assertSetState(ref, false);

    // second invocation does not
    Assertions.assertThat(ref.get())
        .describedAs("first get()")
        .isEqualTo(1);
    assertCounterValue(1);

    // Callable.apply() returns the same value
    Assertions.assertThat(ref.apply())
        .describedAs("second get of %s", ref)
        .isEqualTo(1);
    // no new counter increment
    assertCounterValue(1);
  }

  /**
   * Test failure handling. through the supplier API.
   */
  @Test
  public void testSupplierIntegrationFailureHandling() throws Throwable {

    LazyAtomicReference<Integer> ref = new LazyAtomicReference<>(() -> {
      throw new UnknownHostException(String.format(GENERATED, counter.incrementAndGet()));
    });

    // the get() call will wrap the raised exception, which can be extracted
    // and type checked.
    verifyCause(UnknownHostException.class,
        intercept(UncheckedIOException.class, "[1]", ref::get));

    assertSetState(ref, false);

    // counter goes up
    intercept(UncheckedIOException.class, "[2]", ref::get);
  }

  @Test
  public void testAutoCloseable() throws Throwable {
    final LazyAutoCloseableReference<CloseableClass> ref =
        LazyAutoCloseableReference.lazyAutoCloseablefromSupplier(CloseableClass::new);

    assertSetState(ref, false);
    ref.eval();
    final CloseableClass closeable = ref.get();
    Assertions.assertThat(closeable.isClosed())
        .describedAs("closed flag of %s", closeable)
        .isFalse();

    // first close will close the class.
    ref.close();
    Assertions.assertThat(ref.isClosed())
        .describedAs("closed flag of %s", ref)
        .isTrue();
    Assertions.assertThat(closeable.isClosed())
        .describedAs("closed flag of %s", closeable)
        .isTrue();

    // second close will not raise an exception
    ref.close();

    // you cannot eval() a closed reference
    intercept(IllegalStateException.class, "Reference is closed", ref::eval);
    intercept(IllegalStateException.class, "Reference is closed", ref::get);
    intercept(IllegalStateException.class, "Reference is closed", ref::apply);

    Assertions.assertThat(ref.getReference().get())
        .describedAs("inner referece of %s", ref)
        .isNull();
  }

  /**
   * Not an error to close a reference which was never evaluated.
   */
  @Test
  public void testCloseableUnevaluated() throws Throwable {
    final LazyAutoCloseableReference<CloseableRaisingException> ref =
        new LazyAutoCloseableReference<>(CloseableRaisingException::new);
    ref.close();
    ref.close();
  }

  /**
   * If the close() call fails, that only raises an exception on the first attempt,
   * and the reference is set to null.
   */
  @Test
  public void testAutoCloseableFailureHandling() throws Throwable {
    final LazyAutoCloseableReference<CloseableRaisingException> ref =
        new LazyAutoCloseableReference<>(CloseableRaisingException::new);
    ref.eval();

    // close reports the failure.
    intercept(IOException.class, "raised", ref::close);

    // but the reference is set to null
    assertSetState(ref, false);
    // second attept does nothing, so will not raise an exception.p
    ref.close();
  }

  /**
   * Closeable which sets the closed flag on close().
   */
  private static final class CloseableClass implements AutoCloseable {

    /** closed flag. */
    private boolean closed;

    /**
     * Close the resource.
     * @throws IllegalArgumentException if already closed.
     */
    @Override
    public void close() {
      checkState(!closed, "Already closed");
      closed = true;
    }

    /**
     * Get the closed flag.
     * @return the state.
     */
    private boolean isClosed() {
      return closed;
    }

  }
  /**
   * Closeable which raises an IOE in close().
   */
  private static final class CloseableRaisingException implements AutoCloseable {

    @Override
    public void close() throws Exception {
      throw new IOException("raised");
    }
  }

}
