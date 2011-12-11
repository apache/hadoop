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
package org.apache.hadoop.test;

import static org.mockito.Mockito.*;

/**
 * Helper class to create one-liner stubs, so that instead of: <pre>
 * SomeType someDescriptiveMock = mock(SomeType.class);
 * when(someDescriptiveMock.someMethod()).thenReturn(someValue);</pre>
 * <p>You can now do: <pre>
 * SomeType someDescriptiveMock = make(stub(SomeType.class)
 *     .returning(someValue).from.someMethod());</pre>
 */
public class MockitoMaker {

  /**
   * Create a mock object from a mocked method call.
   *
   * @param <T> type of mocked object
   * @param methodCall  for mocked object
   * @return mocked object
   */
  @SuppressWarnings("unchecked")
  public static <T> T make(Object methodCall) {
    StubBuilder<T> sb = StubBuilder.current();
    when(methodCall).thenReturn(sb.firstReturn, sb.laterReturns);
    return (T) StubBuilder.current().from;
  }

  /**
   * Create a stub builder of a mocked object.
   *
   * @param <T>     type of the target object to be mocked
   * @param target  class of the target object to be mocked
   * @return the stub builder of the mocked object
   */
  public static <T> StubBuilder<T> stub(Class<T> target) {
    return new StubBuilder<T>(mock(target));
  }

  /**
   * Builder class for stubs
   * @param <T> type of the object to be mocked
   */
  public static class StubBuilder<T> {

    /**
     * The target mock object
     */
    public final T from;

    // We want to be able to use this even when the tests are run in parallel.
    @SuppressWarnings("rawtypes")
    private static final ThreadLocal<StubBuilder> tls =
        new ThreadLocal<StubBuilder>() {
          @Override protected StubBuilder initialValue() {
            return new StubBuilder();
          }
        };

    private Object firstReturn = null;
    private Object[] laterReturns = {};

    /**
     * Default constructor for the initial stub builder
     */
    public StubBuilder() {
      this.from = null;
    }

    /**
     * Construct a stub builder with a mock instance
     *
     * @param mockInstance  the mock object
     */
    public StubBuilder(T mockInstance) {
      tls.set(this);
      this.from = mockInstance;
    }

    /**
     * Get the current stub builder from thread local
     *
     * @param <T>
     * @return the stub builder of the mocked object
     */
    @SuppressWarnings("unchecked")
    public static <T> StubBuilder<T> current() {
      return tls.get();
    }

    /**
     * Set the return value for the current stub builder
     *
     * @param value the return value
     * @return the stub builder
     */
    public StubBuilder<T> returning(Object value) {
      this.firstReturn = value;
      return this;
    }

    /**
     * Set the return values for the current stub builder
     *
     * @param value   the first return value
     * @param values  the return values for later invocations
     * @return the stub builder
     */
    public StubBuilder<T> returning(Object value, Object... values) {
      this.firstReturn = value;
      this.laterReturns = values;
      return this;
    }
  }
}
