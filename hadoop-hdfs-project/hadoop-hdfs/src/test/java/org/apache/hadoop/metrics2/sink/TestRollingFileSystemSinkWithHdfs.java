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

import java.io.IOException;
import java.net.URI;
import java.util.Calendar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.sink.RollingFileSystemSinkTestBase.MyMetrics1;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test the {@link RollingFileSystemSink} class in the context of HDFS.
 */
public class TestRollingFileSystemSinkWithHdfs
    extends RollingFileSystemSinkTestBase {
  private static final int  NUM_DATANODES = 4;
  private MiniDFSCluster cluster;

  /**
   * Create a {@link MiniDFSCluster} instance with four nodes.  The
   * node count is required to allow append to function. Also clear the
   * sink's test flags.
   *
   * @throws IOException thrown if cluster creation fails
   */
  @Before
  public void setupHdfs() throws IOException {
    Configuration conf = new Configuration();

    // It appears that since HDFS-265, append is always enabled.
    cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES).build();

    // Also clear sink flags
    RollingFileSystemSink.hasFlushed = false;
  }

  /**
   * Stop the {@link MiniDFSCluster}.
   */
  @After
  public void shutdownHdfs() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test writing logs to HDFS.
   *
   * @throws Exception thrown when things break
   */
  @Test
  public void testWrite() throws Exception {
    String path = "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp";
    MetricsSystem ms = initMetricsSystem(path, false, true);

    assertMetricsContents(doWriteTest(ms, path, 1));
  }

  /**
   * Test writing logs to HDFS if append is enabled and the log file already
   * exists.
   *
   * @throws Exception thrown when things break
   */
  @Test
  public void testAppend() throws Exception {
    String path = "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp";

    assertExtraContents(doAppendTest(path, false, true, 1));
  }

  /**
   * Test writing logs to HDFS if append is enabled, the log file already
   * exists, and the sink is set to ignore errors.
   *
   * @throws Exception thrown when things break
   */
  @Test
  public void testSilentAppend() throws Exception {
    String path = "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp";

    assertExtraContents(doAppendTest(path, true, true, 1));
  }

  /**
   * Test writing logs to HDFS without append enabled, when the log file already
   * exists.
   *
   * @throws Exception thrown when things break
   */
  @Test
  public void testNoAppend() throws Exception {
    String path = "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp";

    assertMetricsContents(doAppendTest(path, false, false, 2));
  }

  /**
   * Test writing logs to HDFS without append enabled, with ignore errors
   * enabled, and when the log file already exists.
   *
   * @throws Exception thrown when things break
   */
  @Test
  public void testSilentOverwrite() throws Exception {
    String path = "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp";

    assertMetricsContents(doAppendTest(path, true, false, 2));
  }

  /**
   * Test that writing to HDFS fails when HDFS is unavailable.
   *
   * @throws IOException thrown when reading or writing log files
   */
  @Test
  public void testFailedWrite() throws IOException {
    final String path =
        "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp";
    MetricsSystem ms = initMetricsSystem(path, false, false);

    new MyMetrics1().registerWith(ms);

    shutdownHdfs();
    MockSink.errored = false;

    ms.publishMetricsNow(); // publish the metrics

    assertTrue("No exception was generated while writing metrics "
        + "even though HDFS was unavailable", MockSink.errored);

    try {
      ms.stop();
    } finally {
      ms.shutdown();
    }
  }

  /**
   * Test that closing a file in HDFS fails when HDFS is unavailable.
   *
   * @throws IOException thrown when reading or writing log files
   */
  @Test
  public void testFailedClose() throws IOException {
    final String path =
        "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp";
    MetricsSystem ms = initMetricsSystem(path, false, false);

    new MyMetrics1().registerWith(ms);

    ms.publishMetricsNow(); // publish the metrics

    shutdownHdfs();
    MockSink.errored = false;

    try {
      ms.stop();

      assertTrue("No exception was generated while stopping sink "
          + "even though HDFS was unavailable", MockSink.errored);
    } catch (MetricsException ex) {
      // Expected
    } finally {
      ms.shutdown();
    }
  }

  /**
   * Test that writing to HDFS fails silently when HDFS is unavailable.
   *
   * @throws IOException thrown when reading or writing log files
   * @throws java.lang.InterruptedException thrown if interrupted
   */
  @Test
  public void testSilentFailedWrite() throws IOException, InterruptedException {
    final String path =
        "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp";
    MetricsSystem ms = initMetricsSystem(path, true, false);

    new MyMetrics1().registerWith(ms);

    shutdownHdfs();
    MockSink.errored = false;

    ms.publishMetricsNow(); // publish the metrics

    assertFalse("An exception was generated writing metrics "
        + "while HDFS was unavailable, even though the sink is set to "
        + "ignore errors", MockSink.errored);

    try {
      ms.stop();
    } finally {
      ms.shutdown();
    }
  }

  /**
   * Test that closing a file in HDFS silently fails when HDFS is unavailable.
   *
   * @throws IOException thrown when reading or writing log files
   */
  @Test
  public void testSilentFailedClose() throws IOException {
    final String path =
        "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp";
    MetricsSystem ms = initMetricsSystem(path, true, false);

    new MyMetrics1().registerWith(ms);

    ms.publishMetricsNow(); // publish the metrics

    shutdownHdfs();
    MockSink.errored = false;

    try {
      ms.stop();

      assertFalse("An exception was generated stopping sink "
          + "while HDFS was unavailable, even though the sink is set to "
          + "ignore errors", MockSink.errored);
    } finally {
      ms.shutdown();
    }
  }

  /**
   * This test specifically checks whether the flusher thread is automatically
   * flushing the files.  It unfortunately can only test with the alternative
   * flushing schedule (because of test timing), but it's better than nothing.
   *
   * @throws Exception thrown if something breaks
   */
  @Test
  public void testFlushThread() throws Exception {
    // Cause the sink's flush thread to be run immediately after the second
    // metrics log is written
    RollingFileSystemSink.forceFlush = true;

    String path = "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp";
    MetricsSystem ms = initMetricsSystem(path, true, false, false);

    new MyMetrics1().registerWith(ms);

    // Publish the metrics
    ms.publishMetricsNow();
    // Pubish again because the first write seems to get properly flushed
    // regardless.
    ms.publishMetricsNow();

    int count = 0;

    try {
      // Sleep until the flusher has run. This should never actually need to
      // sleep, but the sleep is here to make sure this test isn't flakey.
      while (!RollingFileSystemSink.hasFlushed) {
        Thread.sleep(10L);

        if (++count > 1000) {
          fail("Flush thread did not run within 10 seconds");
        }
      }

      Calendar now = Calendar.getInstance();
      Path currentDir = new Path(path, DATE_FORMAT.format(now.getTime()) + "00");
      FileSystem fs = FileSystem.newInstance(new URI(path), new Configuration());
      Path currentFile =
          findMostRecentLogFile(fs, new Path(currentDir, getLogFilename()));
      FileStatus status = fs.getFileStatus(currentFile);

      // Each metrics record is 118+ bytes, depending on hostname
      assertTrue("The flusher thread didn't flush the log contents. Expected "
          + "at least 236 bytes in the log file, but got " + status.getLen(),
          status.getLen() >= 236);
    } finally {
      RollingFileSystemSink.forceFlush = false;

      try {
        ms.stop();
      } finally {
        ms.shutdown();
      }
    }
  }

  /**
   * Test that a failure to connect to HDFS does not cause the init() method
   * to fail.
   */
  @Test
  public void testInitWithNoHDFS() {
    String path = "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp";

    shutdownHdfs();
    MockSink.errored = false;
    initMetricsSystem(path, true, false);

    assertTrue("The sink was not initialized as expected",
        MockSink.initialized);
    assertFalse("The sink threw an unexpected error on initialization",
        MockSink.errored);
  }
}
