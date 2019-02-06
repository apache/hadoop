/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Supplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Test periodic logging of NameNode metrics.
 */
public class TestNameNodeMetricsLogger {
  static final Logger LOG =
      LoggerFactory.getLogger(TestNameNodeMetricsLogger.class);

  @Rule
  public Timeout timeout = new Timeout(300000);

  @Test
  public void testMetricsLoggerOnByDefault() throws IOException {
    NameNode nn = makeNameNode(true);
    assertNotNull(nn.metricsLoggerTimer);
  }

  @Test
  public void testDisableMetricsLogger() throws IOException {
    NameNode nn = makeNameNode(false);
    assertNull(nn.metricsLoggerTimer);
  }

  @Test
  public void testMetricsLoggerIsAsync() throws IOException {
    makeNameNode(true);
    org.apache.log4j.Logger logger =
        ((Log4JLogger) NameNode.MetricsLog).getLogger();
    @SuppressWarnings("unchecked")
    List<Appender> appenders = Collections.list(logger.getAllAppenders());
    assertTrue(appenders.get(0) instanceof AsyncAppender);
  }

  /**
   * Publish a fake metric under the "Hadoop:" domain and ensure it is
   * logged by the metrics logger.
   */
  @Test
  public void testMetricsLogOutput()
      throws IOException, InterruptedException, TimeoutException {
    TestFakeMetric metricsProvider = new TestFakeMetric();
    MBeans.register(this.getClass().getSimpleName(),
        "DummyMetrics", metricsProvider);
    makeNameNode(true);     // Log metrics early and often.
    final PatternMatchingAppender appender =
        new PatternMatchingAppender("^.*FakeMetric42.*$");
    addAppender(NameNode.MetricsLog, appender);

    // Ensure that the supplied pattern was matched.
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return appender.isMatched();
      }
    }, 1000, 60000);
  }

  /**
   * Create a NameNode object that listens on a randomly chosen port
   * number.
   *
   * @param enableMetricsLogging true if periodic metrics logging is to be
   *                             enabled, false otherwise.
   */
  private NameNode makeNameNode(boolean enableMetricsLogging)
      throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://localhost:0");
    conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.setInt(DFS_NAMENODE_METRICS_LOGGER_PERIOD_SECONDS_KEY,
        enableMetricsLogging ? 1 : 0);  // If enabled, log early and log often
    return new TestNameNode(conf);
  }

  private void addAppender(Log log, Appender appender) {
    org.apache.log4j.Logger logger = ((Log4JLogger) log).getLogger();
    @SuppressWarnings("unchecked")
    List<Appender> appenders = Collections.list(logger.getAllAppenders());
    ((AsyncAppender) appenders.get(0)).addAppender(appender);
  }

  /**
   * A NameNode that stubs out the NameSystem for testing.
   */
  private static class TestNameNode extends NameNode {
    @Override
    protected void loadNamesystem(Configuration conf) throws IOException {
      this.namesystem = mock(FSNamesystem.class);
    }

    public TestNameNode(Configuration conf) throws IOException {
      super(conf);
    }
  }

  public interface TestFakeMetricMXBean {
    int getFakeMetric42();
  }

  /**
   * MBean for testing
   */
  public static class TestFakeMetric implements TestFakeMetricMXBean {
    @Override
    public int getFakeMetric42() {
      return 0;
    }
  }

  /**
   * An appender that matches logged messages against the given
   * regular expression.
   */
  public static class PatternMatchingAppender extends AppenderSkeleton {
    private final Pattern pattern;
    private volatile boolean matched;

    public PatternMatchingAppender(String pattern) {
      this.pattern = Pattern.compile(pattern);
      this.matched = false;
    }

    public boolean isMatched() {
      return matched;
    }

    @Override
    protected void append(LoggingEvent event) {
      if (pattern.matcher(event.getMessage().toString()).matches()) {
        matched = true;
      }
    }

    @Override
    public void close() {
    }

    @Override
    public boolean requiresLayout() {
      return false;
    }
  }
}
