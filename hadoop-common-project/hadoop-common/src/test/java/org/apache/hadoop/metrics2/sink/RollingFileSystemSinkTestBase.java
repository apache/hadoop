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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Pattern;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.annotation.Metric.Type;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.impl.TestMetricsConfig;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.assertTrue;

/**
 * This class is a base class for testing the {@link RollingFileSystemSink}
 * class in various contexts. It provides the a number of useful utility
 * methods for classes that extend it.
 */
public class RollingFileSystemSinkTestBase {
  protected static final String SINK_PRINCIPAL_KEY = "rfssink.principal";
  protected static final String SINK_KEYTAB_FILE_KEY = "rfssink.keytab";
  protected static final File ROOT_TEST_DIR = GenericTestUtils.getTestDir(
      "RollingFileSystemSinkTest");
  protected static final SimpleDateFormat DATE_FORMAT =
      new SimpleDateFormat("yyyyMMddHH");
  protected static File methodDir;

  /**
   * The name of the current test method.
   */
  @Rule
  public TestName methodName = new TestName();

  /**
   * A sample metric class
   */
  @Metrics(name="testRecord1", context="test1")
  protected class MyMetrics1 {
    @Metric(value={"testTag1", ""}, type=Type.TAG)
    String testTag1() { return "testTagValue1"; }

    @Metric(value={"testTag2", ""}, type=Type.TAG)
    String gettestTag2() { return "testTagValue2"; }

    @Metric(value={"testMetric1", "An integer gauge"}, always=true)
    MutableGaugeInt testMetric1;

    @Metric(value={"testMetric2", "A long gauge"}, always=true)
    MutableGaugeLong testMetric2;

    public MyMetrics1 registerWith(MetricsSystem ms) {
      return ms.register(methodName.getMethodName() + "-m1", null, this);
    }
  }

  /**
   * Another sample metrics class
   */
  @Metrics(name="testRecord2", context="test1")
  protected class MyMetrics2 {
    @Metric(value={"testTag22", ""}, type=Type.TAG)
    String testTag1() { return "testTagValue22"; }

    public MyMetrics2 registerWith(MetricsSystem ms) {
      return ms.register(methodName.getMethodName() + "-m2", null, this);
    }
  }

  /**
   * Set the date format's timezone to GMT.
   */
  @BeforeClass
  public static void setup() {
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
    FileUtil.fullyDelete(ROOT_TEST_DIR);
  }

  /**
   * Delete the test directory for this test.
   * @throws IOException thrown if the delete fails
   */
  @AfterClass
  public static void deleteBaseDir() throws IOException {
    FileUtil.fullyDelete(ROOT_TEST_DIR);
  }

  /**
   * Create the test directory for this test.
   * @throws IOException thrown if the create fails
   */
  @Before
  public void createMethodDir() throws IOException {
    methodDir = new File(ROOT_TEST_DIR, methodName.getMethodName());

    assertTrue("Test directory already exists: " + methodDir,
        methodDir.mkdirs());
  }

  /**
   * Set up the metrics system, start it, and return it. The principal and
   * keytab properties will not be set.
   *
   * @param path the base path for the sink
   * @param ignoreErrors whether the sink should ignore errors
   * @param allowAppend whether the sink is allowed to append to existing files
   * @return the metrics system
   */
  protected MetricsSystem initMetricsSystem(String path, boolean ignoreErrors,
      boolean allowAppend) {
    return initMetricsSystem(path, ignoreErrors, allowAppend, false);
  }

  /**
   * Set up the metrics system, start it, and return it.
   * @param path the base path for the sink
   * @param ignoreErrors whether the sink should ignore errors
   * @param allowAppend whether the sink is allowed to append to existing files
   * @param useSecureParams whether to set the principal and keytab properties
   * @return the org.apache.hadoop.metrics2.MetricsSystem
   */
  protected MetricsSystem initMetricsSystem(String path, boolean ignoreErrors,
      boolean allowAppend, boolean useSecureParams) {
    // If the prefix is not lower case, the metrics system won't be able to
    // read any of the properties.
    String prefix = methodName.getMethodName().toLowerCase();

    ConfigBuilder builder = new ConfigBuilder().add("*.period", 10000)
        .add(prefix + ".sink.mysink0.class", MockSink.class.getName())
        .add(prefix + ".sink.mysink0.basepath", path)
        .add(prefix + ".sink.mysink0.source", "testsrc")
        .add(prefix + ".sink.mysink0.context", "test1")
        .add(prefix + ".sink.mysink0.ignore-error", ignoreErrors)
        .add(prefix + ".sink.mysink0.allow-append", allowAppend)
        .add(prefix + ".sink.mysink0.roll-offset-interval-millis", 0)
        .add(prefix + ".sink.mysink0.roll-interval", "1h");

    if (useSecureParams) {
      builder.add(prefix + ".sink.mysink0.keytab-key", SINK_KEYTAB_FILE_KEY)
        .add(prefix + ".sink.mysink0.principal-key", SINK_PRINCIPAL_KEY);
    }

    builder.save(TestMetricsConfig.getTestFilename("hadoop-metrics2-" + prefix));

    MetricsSystemImpl ms = new MetricsSystemImpl(prefix);

    ms.start();

    return ms;
  }

  /**
   * Helper method that writes metrics files to a target path, reads those
   * files, and returns the contents of all files as a single string. This
   * method will assert that the correct number of files is found.
   *
   * @param ms an initialized MetricsSystem to use
   * @param path the target path from which to read the logs
   * @param count the number of log files to expect
   * @return the contents of the log files
   * @throws IOException when the log file can't be read
   * @throws URISyntaxException when the target path is an invalid URL
   */
  protected String doWriteTest(MetricsSystem ms, String path, int count)
      throws IOException, URISyntaxException {
    final String then = DATE_FORMAT.format(new Date()) + "00";

    MyMetrics1 mm1 = new MyMetrics1().registerWith(ms);
    new MyMetrics2().registerWith(ms);

    mm1.testMetric1.incr();
    mm1.testMetric2.incr(2);

    ms.publishMetricsNow(); // publish the metrics

    try {
      ms.stop();
    } finally {
      ms.shutdown();
    }

    return readLogFile(path, then, count);
  }

  /**
   * Read the log files at the target path and return the contents as a single
   * string. This method will assert that the correct number of files is found.
   *
   * @param path the target path
   * @param then when the test method began. Used to find the log directory in
   * the case that the test run crosses the top of the hour.
   * @param count the number of log files to expect
   * @return
   * @throws IOException
   * @throws URISyntaxException
   */
  protected String readLogFile(String path, String then, int count)
      throws IOException, URISyntaxException {
    final String now = DATE_FORMAT.format(new Date()) + "00";
    final String logFile = getLogFilename();
    FileSystem fs = FileSystem.get(new URI(path), new Configuration());
    StringBuilder metrics = new StringBuilder();
    boolean found = false;

    for (FileStatus status : fs.listStatus(new Path(path))) {
      Path logDir = status.getPath();

      // There are only two possible valid log directory names: the time when
      // the test started and the current time.  Anything else can be ignored.
      if (now.equals(logDir.getName()) || then.equals(logDir.getName())) {
        readLogData(fs, findMostRecentLogFile(fs, new Path(logDir, logFile)),
            metrics);
        assertFileCount(fs, logDir, count);
        found = true;
      }
    }

    assertTrue("No valid log directories found", found);

    return metrics.toString();
  }

  /**
   * Read the target log file and append its contents to the StringBuilder.
   * @param fs the target FileSystem
   * @param logFile the target file path
   * @param metrics where to append the file contents
   * @throws IOException thrown if the file cannot be read
   */
  protected void readLogData(FileSystem fs, Path logFile, StringBuilder metrics)
      throws IOException {
    FSDataInputStream fsin = fs.open(logFile);
    BufferedReader in = new BufferedReader(new InputStreamReader(fsin,
        StandardCharsets.UTF_8));
    String line = null;

    while ((line = in.readLine()) != null) {
      metrics.append(line).append("\n");
    }
  }

  /**
   * Return the path to the log file to use, based on the initial path. The
   * initial path must be a valid log file path. This method will find the
   * most recent version of the file.
   *
   * @param fs the target FileSystem
   * @param initial the path from which to start
   * @return the path to use
   * @throws IOException thrown if testing for file existence fails.
   */
  protected Path findMostRecentLogFile(FileSystem fs, Path initial)
      throws IOException {
    Path logFile = null;
    Path nextLogFile = initial;
    int id = 1;

    do {
      logFile = nextLogFile;
      nextLogFile = new Path(initial.toString() + "." + id);
      id += 1;
    } while (fs.exists(nextLogFile));

    return logFile;
  }

  /**
   * Return the name of the log file for this host.
   *
   * @return the name of the log file for this host
   */
  protected static String getLogFilename() throws UnknownHostException {
    return "testsrc-" + InetAddress.getLocalHost().getHostName() + ".log";
  }

  /**
   * Assert that the given contents match what is expected from the test
   * metrics.
   *
   * @param contents the file contents to test
   */
  protected void assertMetricsContents(String contents) {
    // Note that in the below expression we allow tags and metrics to go in
    // arbitrary order, but the records must be in order.
    final Pattern expectedContentPattern = Pattern.compile(
        "^\\d+\\s+test1.testRecord1:\\s+Context=test1,\\s+"
        + "(testTag1=testTagValue1,\\s+testTag2=testTagValue2|"
        + "testTag2=testTagValue2,\\s+testTag1=testTagValue1),"
        + "\\s+Hostname=.*,\\s+"
        + "(testMetric1=1,\\s+testMetric2=2|testMetric2=2,\\s+testMetric1=1)"
        + "[\\n\\r]*^\\d+\\s+test1.testRecord2:\\s+Context=test1,"
        + "\\s+testTag22=testTagValue22,\\s+Hostname=.*$[\\n\\r]*",
         Pattern.MULTILINE);

    assertTrue("Sink did not produce the expected output. Actual output was: "
        + contents, expectedContentPattern.matcher(contents).matches());
  }

  /**
   * Assert that the given contents match what is expected from the test
   * metrics when there is pre-existing data.
   *
   * @param contents the file contents to test
   */
  protected void assertExtraContents(String contents) {
    // Note that in the below expression we allow tags and metrics to go in
    // arbitrary order, but the records must be in order.
    final Pattern expectedContentPattern = Pattern.compile(
        "Extra stuff[\\n\\r]*"
        + "^\\d+\\s+test1.testRecord1:\\s+Context=test1,\\s+"
        + "(testTag1=testTagValue1,\\s+testTag2=testTagValue2|"
        + "testTag2=testTagValue2,\\s+testTag1=testTagValue1),"
        + "\\s+Hostname=.*,\\s+"
        + "(testMetric1=1,\\s+testMetric2=2|testMetric2=2,\\s+testMetric1=1)"
        + "[\\n\\r]*^\\d+\\s+test1.testRecord2:\\s+Context=test1,"
        + "\\s+testTag22=testTagValue22,\\s+Hostname=.*$[\\n\\r]*",
         Pattern.MULTILINE);

    assertTrue("Sink did not produce the expected output. Actual output was: "
        + contents, expectedContentPattern.matcher(contents).matches());
  }

  /**
   * Call {@link #doWriteTest} after pre-creating the log file and filling it
   * with junk data.
   *
   * @param path the base path for the test
   * @param ignoreErrors whether to ignore errors
   * @param allowAppend whether to allow appends
   * @param count the number of files to expect
   * @return the contents of the final log file
   * @throws IOException if a file system operation fails
   * @throws InterruptedException if interrupted while calling
   * {@link #getNowNotTopOfHour()}
   * @throws URISyntaxException if the path is not a valid URI
   */
  protected String doAppendTest(String path, boolean ignoreErrors,
      boolean allowAppend, int count)
      throws IOException, InterruptedException, URISyntaxException {
    preCreateLogFile(path);

    return doWriteTest(initMetricsSystem(path, ignoreErrors, allowAppend),
        path, count);
  }

  /**
   * Create a file at the target path with some known data in it:
   * &quot;Extra stuff&quot;.
   *
   * If the test run is happening within 20 seconds of the top of the hour,
   * this method will sleep until the top of the hour.
   *
   * @param path the target path under which to create the directory for the
   * current hour that will contain the log file.
   *
   * @throws IOException thrown if the file creation fails
   * @throws InterruptedException thrown if interrupted while waiting for the
   * top of the hour.
   * @throws URISyntaxException thrown if the path isn't a valid URI
   */
  protected void preCreateLogFile(String path)
      throws IOException, InterruptedException, URISyntaxException {
    preCreateLogFile(path, 1);
  }

  /**
   * Create files at the target path with some known data in them.  Each file
   * will have the same content: &quot;Extra stuff&quot;.
   *
   * If the test run is happening within 20 seconds of the top of the hour,
   * this method will sleep until the top of the hour.
   *
   * @param path the target path under which to create the directory for the
   * current hour that will contain the log files.
   * @param numFiles the number of log files to create
   * @throws IOException thrown if the file creation fails
   * @throws InterruptedException thrown if interrupted while waiting for the
   * top of the hour.
   * @throws URISyntaxException thrown if the path isn't a valid URI
   */
  protected void preCreateLogFile(String path, int numFiles)
      throws IOException, InterruptedException, URISyntaxException {
    Calendar now = getNowNotTopOfHour();

    FileSystem fs = FileSystem.get(new URI(path), new Configuration());
    Path dir = new Path(path, DATE_FORMAT.format(now.getTime()) + "00");

    fs.mkdirs(dir);

    Path file = new Path(dir, getLogFilename());

    // Create the log file to force the sink to append
    try (FSDataOutputStream out = fs.create(file)) {
      out.write("Extra stuff\n".getBytes());
      out.flush();
    }

    if (numFiles > 1) {
      int count = 1;

      while (count < numFiles) {
        file = new Path(dir, getLogFilename() + "." + count);

        // Create the log file to force the sink to append
        try (FSDataOutputStream out = fs.create(file)) {
          out.write("Extra stuff\n".getBytes());
          out.flush();
        }

        count += 1;
      }
    }
  }

  /**
   * Return a calendar based on the current time.  If the current time is very
   * near the top of the hour (less than 20 seconds), sleep until the new hour
   * before returning a new Calendar instance.
   *
   * @return a new Calendar instance that isn't near the top of the hour
   * @throws InterruptedException if interrupted while sleeping
   */
  public Calendar getNowNotTopOfHour() throws InterruptedException {
    Calendar now = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

    // If we're at the very top of the hour, sleep until the next hour
    // so that we don't get confused by the directory rolling
    if ((now.get(Calendar.MINUTE) == 59) && (now.get(Calendar.SECOND) > 40)) {
      Thread.sleep((61 - now.get(Calendar.SECOND)) * 1000L);
      now.setTime(new Date());
    }

    return now;
  }

  /**
   * Assert that the number of log files in the target directory is as expected.
   * @param fs the target FileSystem
   * @param dir the target directory path
   * @param expected the expected number of files
   * @throws IOException thrown if listing files fails
   */
  public void assertFileCount(FileSystem fs, Path dir, int expected)
      throws IOException {
    RemoteIterator<LocatedFileStatus> i = fs.listFiles(dir, true);
    int count = 0;

    while (i.hasNext()) {
      i.next();
      count++;
    }

    assertTrue("The sink created additional unexpected log files. " + count
        + " files were created", expected >= count);
    assertTrue("The sink created too few log files. " + count + " files were "
        + "created", expected <= count);
  }

  /**
   * This class is a {@link RollingFileSystemSink} wrapper that tracks whether
   * an exception has been thrown during operations.
   */
  public static class MockSink extends RollingFileSystemSink {
    public static volatile boolean errored = false;
    public static volatile boolean initialized = false;

    @Override
    public void init(SubsetConfiguration conf) {
      try {
        super.init(conf);
      } catch (MetricsException ex) {
        errored = true;

        throw new MetricsException(ex);
      }

      initialized = true;
    }

    @Override
    public void putMetrics(MetricsRecord record) {
      try {
        super.putMetrics(record);
      } catch (MetricsException ex) {
        errored = true;

        throw new MetricsException(ex);
      }
    }

    @Override
    public void close() {
      try {
        super.close();
      } catch (MetricsException ex) {
        errored = true;

        throw new MetricsException(ex);
      }
    }

    @Override
    public void flush() {
      try {
        super.flush();
      } catch (MetricsException ex) {
        errored = true;

        throw new MetricsException(ex);
      }
    }
  }
}
