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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public class TestNodeHealthService {

  private static volatile Log LOG = LogFactory
      .getLog(TestNodeHealthService.class);

  protected static File testRootDir = new File("target",
      TestNodeHealthService.class.getName() + "-localDir").getAbsoluteFile();

  final static File nodeHealthConfigFile = new File(testRootDir,
      "modified-mapred-site.xml");

  private File nodeHealthscriptFile = new File(testRootDir,
      Shell.appendScriptExtension("failingscript"));

  @Before
  public void setup() {
    testRootDir.mkdirs();
  }

  @After
  public void tearDown() throws Exception {
    if (testRootDir.exists()) {
      FileContext.getLocalFSFileContext().delete(
          new Path(testRootDir.getAbsolutePath()), true);
    }
  }

  private Configuration getConfForNodeHealthScript() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_PATH,
        nodeHealthscriptFile.getAbsolutePath());
    conf.setLong(YarnConfiguration.NM_HEALTH_CHECK_INTERVAL_MS, 500);
    conf.setLong(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS, 1000);
    return conf;
  }

  private void writeNodeHealthScriptFile(String scriptStr, boolean setExecutable)
          throws IOException {
    PrintWriter pw = null;
    try {
      FileUtil.setWritable(nodeHealthscriptFile, true);
      FileUtil.setReadable(nodeHealthscriptFile, true);
      pw = new PrintWriter(new FileOutputStream(nodeHealthscriptFile));
      pw.println(scriptStr);
      pw.flush();
    } finally {
      pw.close();
    }
    FileUtil.setExecutable(nodeHealthscriptFile, setExecutable);
  }

  @Test
  public void testNodeHealthScriptShouldRun() throws IOException {
    // Node health script should not start if there is no property called
    // node health script path.
    Assert.assertFalse("By default Health script should not have started",
        NodeHealthScriptRunner.shouldRun(new Configuration()));
    Configuration conf = getConfForNodeHealthScript();
    // Node health script should not start if the node health script does not
    // exists
    Assert.assertFalse("Node health script should start",
        NodeHealthScriptRunner.shouldRun(conf));
    // Create script path.
    conf.writeXml(new FileOutputStream(nodeHealthConfigFile));
    conf.addResource(nodeHealthConfigFile.getName());
    writeNodeHealthScriptFile("", false);
    // Node health script should not start if the node health script is not
    // executable.
    Assert.assertFalse("Node health script should start",
        NodeHealthScriptRunner.shouldRun(conf));
    writeNodeHealthScriptFile("", true);
    Assert.assertTrue("Node health script should start",
        NodeHealthScriptRunner.shouldRun(conf));
  }

  private void setHealthStatus(NodeHealthStatus healthStatus, boolean isHealthy,
      String healthReport, long lastHealthReportTime) {
    healthStatus.setHealthReport(healthReport);
    healthStatus.setIsNodeHealthy(isHealthy);
    healthStatus.setLastHealthReportTime(lastHealthReportTime);
  }

  @Test
  public void testNodeHealthScript() throws Exception {
    RecordFactory factory = RecordFactoryProvider.getRecordFactory(null);
    NodeHealthStatus healthStatus =
        factory.newRecordInstance(NodeHealthStatus.class);
    String errorScript = "echo ERROR\n echo \"Tracker not healthy\"";
    String normalScript = "echo \"I am all fine\"";
    String timeOutScript = Shell.WINDOWS ? "@echo off\nping -n 4 127.0.0.1 >nul\necho \"I am fine\""
        : "sleep 4\necho \"I am fine\"";
    Configuration conf = getConfForNodeHealthScript();
    conf.writeXml(new FileOutputStream(nodeHealthConfigFile));
    conf.addResource(nodeHealthConfigFile.getName());

    writeNodeHealthScriptFile(normalScript, true);
    NodeHealthCheckerService nodeHealthChecker = new NodeHealthCheckerService();
    nodeHealthChecker.init(conf);
    NodeHealthScriptRunner nodeHealthScriptRunner =
        nodeHealthChecker.getNodeHealthScriptRunner();
    TimerTask timerTask = nodeHealthScriptRunner.getTimerTask();

    timerTask.run();

    setHealthStatus(healthStatus, nodeHealthChecker.isHealthy(),
        nodeHealthChecker.getHealthReport(),
        nodeHealthChecker.getLastHealthReportTime());
    LOG.info("Checking initial healthy condition");
    // Check proper report conditions.
    Assert.assertTrue("Node health status reported unhealthy", healthStatus
        .getIsNodeHealthy());
    Assert.assertTrue("Node health status reported unhealthy", healthStatus
        .getHealthReport().equals(nodeHealthChecker.getHealthReport()));

    // write out error file.
    // Healthy to unhealthy transition
    writeNodeHealthScriptFile(errorScript, true);
    // Run timer
    timerTask.run();
    // update health status
    setHealthStatus(healthStatus, nodeHealthChecker.isHealthy(),
        nodeHealthChecker.getHealthReport(),
        nodeHealthChecker.getLastHealthReportTime());
    LOG.info("Checking Healthy--->Unhealthy");
    Assert.assertFalse("Node health status reported healthy", healthStatus
        .getIsNodeHealthy());
    Assert.assertTrue("Node health status reported healthy", healthStatus
        .getHealthReport().equals(nodeHealthChecker.getHealthReport()));
    
    // Check unhealthy to healthy transitions.
    writeNodeHealthScriptFile(normalScript, true);
    timerTask.run();
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
    writeNodeHealthScriptFile(timeOutScript, true);
    timerTask.run();
    setHealthStatus(healthStatus, nodeHealthChecker.isHealthy(),
        nodeHealthChecker.getHealthReport(),
        nodeHealthChecker.getLastHealthReportTime());
    LOG.info("Checking Healthy--->timeout");
    Assert.assertFalse("Node health status reported healthy even after timeout",
        healthStatus.getIsNodeHealthy());
    Assert.assertTrue("Node script time out message not propogated",
        healthStatus.getHealthReport().equals(
            NodeHealthScriptRunner.NODE_HEALTH_SCRIPT_TIMED_OUT_MSG
            + NodeHealthCheckerService.SEPARATOR
            + nodeHealthChecker.getDiskHandler().getDisksHealthReport(false)));
  }

}
