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

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A few more asserts
 */
public class MoreAsserts {

  /**
   * Assert equivalence for array and iterable
   *
   * @param <T>      the type of the elements
   * @param s        the name/message for the collection
   * @param expected the expected array of elements
   * @param actual   the actual iterable of elements
   */
  public static <T> void assertEquals(String s, T[] expected,
                                      Iterable<T> actual) {
    Iterator<T> it = actual.iterator();
    int i = 0;
    for (; i < expected.length && it.hasNext(); ++i) {
      Assertions.assertEquals(expected[i], it.next(), "Element " + i + " for " + s);
    }
    Assertions.assertTrue(i == expected.length, "Expected more elements");
    Assertions.assertTrue(!it.hasNext(), "Expected less elements");
  }

  /**
   * Assert equality for two iterables
   *
   * @param <T>      the type of the elements
   * @param s
   * @param expected
   * @param actual
   */
  public static <T> void assertEquals(String s, Iterable<T> expected,
                                      Iterable<T> actual) {
    Iterator<T> ite = expected.iterator();
    Iterator<T> ita = actual.iterator();
    int i = 0;
    while (ite.hasNext() && ita.hasNext()) {
      Assertions.assertEquals(ite.next(), ita.next(), "Element " + i + " for " + s);
    }
    Assertions.assertTrue(!ite.hasNext(), "Expected more elements");
    Assertions.assertTrue(!ita.hasNext(), "Expected less elements");
  }


  public static <T> void assertFutureCompletedSuccessfully(CompletableFuture<T> future) {
    assertThat(future.isDone())
            .describedAs("This future is supposed to be " +
                    "completed successfully")
            .isTrue();
    assertThat(future.isCompletedExceptionally())
            .describedAs("This future is supposed to be " +
                    "completed successfully")
            .isFalse();
  }

  public static <T> void assertFutureFailedExceptionally(CompletableFuture<T> future) {
    assertThat(future.isCompletedExceptionally())
            .describedAs("This future is supposed to be " +
                    "completed exceptionally")
            .isTrue();
  }

  /**
   * Assert two same type of values.
   * @param actual actual value.
   * @param expected expected value.
   * @param message error message to print in case of mismatch.
   */
  public static <T> void assertEqual(T actual, T expected, String message) {
    assertThat(actual)
            .describedAs("Mismatch in %s", message)
            .isEqualTo(expected);
  }
}
