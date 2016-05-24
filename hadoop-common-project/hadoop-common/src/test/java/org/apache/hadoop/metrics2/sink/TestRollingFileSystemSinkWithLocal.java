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

package org.apache.hadoop.metrics2.sink;

import org.apache.hadoop.metrics2.MetricsSystem;

import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test the {@link RollingFileSystemSink} class in the context of the local file
 * system.
 */
public class TestRollingFileSystemSinkWithLocal
    extends RollingFileSystemSinkTestBase {
  /**
   * Test writing logs to the local file system.
   * @throws Exception when things break
   */
  @Test
  public void testWrite() throws Exception {
    String path = methodDir.getAbsolutePath();
    MetricsSystem ms = initMetricsSystem(path, false, false);

    assertMetricsContents(doWriteTest(ms, path, 1));
  }

  /**
   * Test writing logs to the local file system with the sink set to ignore
   * errors.
   * @throws Exception when things break
   */
  @Test
  public void testSilentWrite() throws Exception {
    String path = methodDir.getAbsolutePath();
    MetricsSystem ms = initMetricsSystem(path, true, false);

    assertMetricsContents(doWriteTest(ms, path, 1));
  }

  /**
   * Test writing logs to HDFS when the log file already exists.
   *
   * @throws Exception when things break
   */
  @Test
  public void testExistingWrite() throws Exception {
    String path = methodDir.getAbsolutePath();

    assertMetricsContents(doAppendTest(path, false, false, 2));
  }

  /**
   * Test writing logs to HDFS when the log file and the .1 log file already
   * exist.
   *
   * @throws Exception when things break
   */
  @Test
  public void testExistingWrite2() throws Exception {
    String path = methodDir.getAbsolutePath();
    MetricsSystem ms = initMetricsSystem(path, false, false);

    preCreateLogFile(path, 2);

    assertMetricsContents(doWriteTest(ms, path, 3));
  }

  /**
   * Test writing logs to HDFS with ignore errors enabled when
   * the log file already exists.
   *
   * @throws Exception when things break
   */
  @Test
  public void testSilentExistingWrite() throws Exception {
    String path = methodDir.getAbsolutePath();

    assertMetricsContents(doAppendTest(path, false, false, 2));
  }

  /**
   * Test that writing fails when the directory isn't writable.
   */
  @Test
  public void testFailedWrite() {
    String path = methodDir.getAbsolutePath();
    MetricsSystem ms = initMetricsSystem(path, false, false);

    new MyMetrics1().registerWith(ms);

    methodDir.setWritable(false);
    MockSink.errored = false;

    try {
      // publish the metrics
      ms.publishMetricsNow();

      assertTrue("No exception was generated while writing metrics "
          + "even though the target directory was not writable",
          MockSink.errored);

      ms.stop();
      ms.shutdown();
    } finally {
      // Make sure the dir is writable again so we can delete it at the end
      methodDir.setWritable(true);
    }
  }

  /**
   * Test that writing fails silently when the directory is not writable.
   */
  @Test
  public void testSilentFailedWrite() {
    String path = methodDir.getAbsolutePath();
    MetricsSystem ms = initMetricsSystem(path, true, false);

    new MyMetrics1().registerWith(ms);

    methodDir.setWritable(false);
    MockSink.errored = false;

    try {
      // publish the metrics
      ms.publishMetricsNow();

      assertFalse("An exception was generated while writing metrics "
          + "when the target directory was not writable, even though the "
          + "sink is set to ignore errors",
          MockSink.errored);

      ms.stop();
      ms.shutdown();
    } finally {
      // Make sure the dir is writable again so we can delete it at the end
      methodDir.setWritable(true);
    }
  }
}
