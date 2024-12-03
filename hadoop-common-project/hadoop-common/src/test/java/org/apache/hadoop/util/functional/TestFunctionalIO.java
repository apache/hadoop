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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.util.functional.FunctionalIO.extractIOExceptions;
import static org.apache.hadoop.util.functional.FunctionalIO.toUncheckedFunction;
import static org.apache.hadoop.util.functional.FunctionalIO.toUncheckedIOExceptionSupplier;
import static org.apache.hadoop.util.functional.FunctionalIO.uncheckIOExceptions;

/**
 * Test the functional IO class.
 */
public class TestFunctionalIO extends AbstractHadoopTestBase {

  /**
   * Verify that IOEs are caught and wrapped.
   */
  @Test
  public void testUncheckIOExceptions() throws Throwable {
    final IOException raised = new IOException("text");
    final UncheckedIOException ex = intercept(UncheckedIOException.class, "text", () ->
        uncheckIOExceptions(() -> {
          throw raised;
        }));
    Assertions.assertThat(ex.getCause())
        .describedAs("Cause of %s", ex)
        .isSameAs(raised);
  }

  /**
   * Verify that UncheckedIOEs are not double wrapped.
   */
  @Test
  public void testUncheckIOExceptionsUnchecked() throws Throwable {
    final UncheckedIOException raised = new UncheckedIOException(
        new IOException("text"));
    final UncheckedIOException ex = intercept(UncheckedIOException.class, "text", () ->
        uncheckIOExceptions(() -> {
          throw raised;
        }));
    Assertions.assertThat(ex)
        .describedAs("Propagated Exception %s", ex)
        .isSameAs(raised);
  }

  /**
   * Supplier will also wrap IOEs.
   */
  @Test
  public void testUncheckedSupplier() throws Throwable {
    intercept(UncheckedIOException.class, "text", () ->
        toUncheckedIOExceptionSupplier(() -> {
          throw new IOException("text");
        }).get());
  }

  /**
   * The wrap/unwrap code which will be used to invoke operations
   * through reflection.
   */
  @Test
  public void testUncheckAndExtract() throws Throwable {
    final IOException raised = new IOException("text");
    final IOException ex = intercept(IOException.class, "text", () ->
        extractIOExceptions(toUncheckedIOExceptionSupplier(() -> {
          throw raised;
        })));
    Assertions.assertThat(ex)
        .describedAs("Propagated Exception %s", ex)
        .isSameAs(raised);
  }

  @Test
  public void testUncheckedFunction() throws Throwable {
    // java function which should raise a FileNotFoundException
    // wrapped into an unchecked exeption
    final Function<String, Object> fn =
        toUncheckedFunction((String a) -> {
          throw new FileNotFoundException(a);
        });
    intercept(UncheckedIOException.class, "missing", () ->
        fn.apply("missing"));
  }
}
