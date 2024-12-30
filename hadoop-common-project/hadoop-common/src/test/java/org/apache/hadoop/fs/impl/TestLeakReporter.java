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

import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.fs.impl.LeakReporter.THREAD_FORMAT;
import static org.apache.hadoop.test.GenericTestUtils.LogCapturer.captureLogs;

public final class TestLeakReporter extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestLeakReporter.class);

  /**
   * Count of close calls.
   */
  private final AtomicInteger closeCount = new AtomicInteger();

  /**
   * Big test: creates a reporter, closes it.
   * Verifies that the error message and stack traces is printed when
   * open, and that the close callback was invoked.
   * <p>
   * After the first invocation, a second invocation is ignored.
   */
  @Test
  public void testLeakInvocation() throws Throwable {

    final String message = "<message>";
    final LeakReporter reporter = new LeakReporter(message,
        () -> true,
        this::closed);

    // store the old thread name and change it,
    // so the log test can verify that the old thread name is printed.
    String oldName = Thread.currentThread().getName();
    Thread.currentThread().setName("thread");
    // Capture the logs
    GenericTestUtils.LogCapturer logs =
        captureLogs(LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME));
    expectClose(reporter, 1);

    // check the log
    logs.stopCapturing();
    final String output = logs.getOutput();
    LOG.info("output of leak log is {}", output);

    final String threadInfo = String.format(THREAD_FORMAT,
        oldName,
        Thread.currentThread().getId());
    // log auditing
    Assertions.assertThat(output)
        .describedAs("output from the logs")
        .contains("WARN")
        .contains(message)
        .contains(Thread.currentThread().getName())
        .contains(threadInfo)
        .contains("TestLeakReporter.testLeakInvocation")
        .contains("INFO")
        .contains("stack");

    // no reentrancy
    expectClose(reporter, 1);
  }

  /**
   * Expect the close operation to result in
   * a value of the close count to be as expected.
   * @param reporter leak reporter
   * @param expected expected value after the close
   */
  private void expectClose(final LeakReporter reporter, final int expected) {
    reporter.close();
    assertCloseCount(expected);
  }

  /**
   * Close operation: increments the counter.
   */
  private void closed() {
    closeCount.incrementAndGet();
  }

  /**
   * When the source is closed, no leak cleanup takes place.
   */
  @Test
  public void testLeakSkipped() throws Throwable {

    final LeakReporter reporter = new LeakReporter("<message>",
        () -> false,
        this::closed);
    expectClose(reporter, 0);
  }

  /**
   * If the probe raises an exception, the exception is swallowed
   * and the close action is never invoked.
   */
  @Test
  public void testProbeFailureSwallowed() throws Throwable {
    final LeakReporter reporter = new LeakReporter("<message>",
        this::raiseNPE,
        this::closed);
    expectClose(reporter, 0);
  }

  /**
   * Any exception raised in the close action it is swallowed.
   */
  @Test
  public void testCloseActionSwallowed() throws Throwable {
    final LeakReporter reporter = new LeakReporter("<message>",
        () -> true,
        this::raiseNPE);
    reporter.close();

    Assertions.assertThat(reporter.isClosed())
        .describedAs("reporter closed)")
        .isTrue();
  }

  /**
   * Always raises an NPE.
   * @return never
   */
  private boolean raiseNPE() {
    throw new NullPointerException("oops");
  }

  /**
   * Assert that the value of {@link #closeCount} is as expected.
   * @param ex expected.
   */
  private void assertCloseCount(final int ex) {
    Assertions.assertThat(closeCount.get())
        .describedAs("close count")
        .isEqualTo(ex);
  }
}
