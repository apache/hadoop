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

package org.apache.hadoop.yarn.server.nodemanager.health;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Test class for {@link NodeHealthCheckerService}.
 */
public class TestNodeHealthCheckerService {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestNodeHealthCheckerService.class);

  private static final File TEST_ROOT_DIR = new File("target",
      TestNodeHealthCheckerService.class.getName() + "-localDir")
      .getAbsoluteFile();

  private static final File NODE_HEALTH_CONFIG_FILE = new File(TEST_ROOT_DIR,
      "modified-mapred-site.xml");

  private File nodeHealthScriptFile = new File(TEST_ROOT_DIR,
      Shell.appendScriptExtension("failingscript"));

  @Before
  public void setup() {
    TEST_ROOT_DIR.mkdirs();
  }

  @After
  public void tearDown() throws Exception {
    if (TEST_ROOT_DIR.exists()) {
      FileContext.getLocalFSFileContext().delete(
          new Path(TEST_ROOT_DIR.getAbsolutePath()), true);
    }
  }

  private void writeNodeHealthScriptFile() throws IOException,
      InterruptedException {
    try (PrintWriter pw = new PrintWriter(
        new FileOutputStream(nodeHealthScriptFile))) {
      FileUtil.chmod(nodeHealthScriptFile.getCanonicalPath(), "u+rwx");
      pw.println("");
      pw.flush();
    }
  }

  private Configuration getConfForNodeHealthScript(String scriptName) {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_HEALTH_CHECK_SCRIPTS, scriptName);
    String timeoutConfig =
        String.format(
            YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS_TEMPLATE,
            scriptName);
    conf.setLong(timeoutConfig, 1000L);

    String intervalConfig =
        String.format(
            YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_INTERVAL_MS_TEMPLATE,
            scriptName);
    conf.setLong(intervalConfig, 500L);

    String pathConfig =
        String.format(
            YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_PATH_TEMPLATE,
            scriptName);
    conf.set(pathConfig, nodeHealthScriptFile.getAbsolutePath());

    return conf;
  }

  private void setHealthStatus(NodeHealthStatus healthStatus, boolean isHealthy,
      String healthReport, long lastHealthReportTime) {
    healthStatus.setHealthReport(healthReport);
    healthStatus.setIsNodeHealthy(isHealthy);
    healthStatus.setLastHealthReportTime(lastHealthReportTime);
  }

  @Test
  public void testNodeHealthService() throws Exception {
    RecordFactory factory = RecordFactoryProvider.getRecordFactory(null);
    NodeHealthStatus healthStatus =
        factory.newRecordInstance(NodeHealthStatus.class);
    String scriptName = "test";
    Configuration conf = getConfForNodeHealthScript(scriptName);
    conf.writeXml(new FileOutputStream(NODE_HEALTH_CONFIG_FILE));
    conf.addResource(NODE_HEALTH_CONFIG_FILE.getName());
    writeNodeHealthScriptFile();

    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    NodeHealthScriptRunner nodeHealthScriptRunner =
        NodeHealthScriptRunner.newInstance(scriptName, conf);
    if (nodeHealthScriptRunner == null) {
      fail("Should have created NodeHealthScriptRunner instance");
    }
    nodeHealthScriptRunner = spy(nodeHealthScriptRunner);
    NodeHealthCheckerService nodeHealthChecker =
        new NodeHealthCheckerService(dirsHandler);
    nodeHealthChecker.addHealthReporter(nodeHealthScriptRunner);
    nodeHealthChecker.init(conf);

    doReturn(true).when(nodeHealthScriptRunner).isHealthy();
    doReturn("").when(nodeHealthScriptRunner).getHealthReport();
    setHealthStatus(healthStatus, nodeHealthChecker.isHealthy(),
        nodeHealthChecker.getHealthReport(),
        nodeHealthChecker.getLastHealthReportTime());
    LOG.info("Checking initial healthy condition");
    // Check proper report conditions.
    Assert.assertTrue("Node health status reported unhealthy", healthStatus
        .getIsNodeHealthy());
    Assert.assertTrue("Node health status reported unhealthy", healthStatus
        .getHealthReport().equals(nodeHealthChecker.getHealthReport()));

    doReturn(false).when(nodeHealthScriptRunner).isHealthy();
    // update health status
    setHealthStatus(healthStatus, nodeHealthChecker.isHealthy(),
        nodeHealthChecker.getHealthReport(),
        nodeHealthChecker.getLastHealthReportTime());
    LOG.info("Checking Healthy--->Unhealthy");
    Assert.assertFalse("Node health status reported healthy", healthStatus
        .getIsNodeHealthy());
    Assert.assertTrue("Node health status reported healthy", healthStatus
        .getHealthReport().equals(nodeHealthChecker.getHealthReport()));

    doReturn(true).when(nodeHealthScriptRunner).isHealthy();
    setHealthStatus(healthStatus, nodeHealthChecker.isHealthy(),
        nodeHealthChecker.getHealthReport(),
        nodeHealthChecker.getLastHealthReportTime());
    LOG.info("Checking UnHealthy--->healthy");
    // Check proper report conditions.
    Assert.assertTrue("Node health status reported unhealthy", healthStatus
        .getIsNodeHealthy());
    Assert.assertTrue("Node health status reported unhealthy", healthStatus
        .getHealthReport().equals(nodeHealthChecker.getHealthReport()));

    // Healthy to timeout transition.
    doReturn(false).when(nodeHealthScriptRunner).isHealthy();
    doReturn(NodeHealthScriptRunner.NODE_HEALTH_SCRIPT_TIMED_OUT_MSG)
        .when(nodeHealthScriptRunner).getHealthReport();
    setHealthStatus(healthStatus, nodeHealthChecker.isHealthy(),
        nodeHealthChecker.getHealthReport(),
        nodeHealthChecker.getLastHealthReportTime());
    LOG.info("Checking Healthy--->timeout");
    Assert.assertFalse("Node health status reported healthy even after timeout",
        healthStatus.getIsNodeHealthy());
    Assert.assertTrue("Node script time out message not propagated",
        healthStatus.getHealthReport().equals(
            Joiner.on(NodeHealthCheckerService.SEPARATOR).skipNulls().join(
                NodeHealthScriptRunner.NODE_HEALTH_SCRIPT_TIMED_OUT_MSG,
                Strings.emptyToNull(
                    nodeHealthChecker.getDiskHandler()
                        .getDisksHealthReport(false))
            )));
  }

  private abstract class HealthReporterService extends AbstractService
      implements HealthReporter {
    HealthReporterService() {
      super(HealthReporterService.class.getName());
    }
  }

  @Test
  public void testCustomHealthReporter() throws Exception {
    String healthReport = "dummy health report";
    HealthReporterService customHealthReporter = new HealthReporterService() {
      private int counter = 0;

      @Override
      public boolean isHealthy() {
        return counter++ % 2 == 0;
      }

      @Override
      public String getHealthReport() {
        return healthReport;
      }

      @Override
      public long getLastHealthReportTime() {
        return Long.MAX_VALUE;
      }
    };

    Configuration conf = new Configuration();
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    NodeHealthCheckerService nodeHealthChecker =
        new NodeHealthCheckerService(dirsHandler);
    nodeHealthChecker.addHealthReporter(customHealthReporter);
    nodeHealthChecker.init(conf);

    assertThat(nodeHealthChecker.isHealthy()).isTrue();
    assertThat(nodeHealthChecker.isHealthy()).isFalse();
    assertThat(nodeHealthChecker.getHealthReport()).isEqualTo(healthReport);
    assertThat(nodeHealthChecker.getLastHealthReportTime())
        .isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void testExceptionReported() {
    Configuration conf = new Configuration();
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    NodeHealthCheckerService nodeHealthChecker =
        new NodeHealthCheckerService(dirsHandler);
    nodeHealthChecker.init(conf);
    assertThat(nodeHealthChecker.isHealthy()).isTrue();

    String message = "An exception was thrown.";
    Exception exception = new Exception(message);
    nodeHealthChecker.reportException(exception);
    assertThat(nodeHealthChecker.isHealthy()).isFalse();
    assertThat(nodeHealthChecker.getHealthReport()).isEqualTo(message);
  }
}
