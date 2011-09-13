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

package org.apache.hadoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
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
      "failingscript.sh");

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
    PrintWriter pw = new PrintWriter(new FileOutputStream(nodeHealthscriptFile));
    pw.println(scriptStr);
    pw.flush();
    pw.close();
    nodeHealthscriptFile.setExecutable(setExecutable);
  }

  @Test
  public void testNodeHealthScriptShouldRun() throws IOException {
    // Node health script should not start if there is no property called
    // node health script path.
    Assert.assertFalse("By default Health checker should not have started",
        NodeHealthCheckerService.shouldRun(new Configuration()));
    Configuration conf = getConfForNodeHealthScript();
    // Node health script should not start if the node health script does not
    // exists
    Assert.assertFalse("Node health script should start", NodeHealthCheckerService
        .shouldRun(conf));
    // Create script path.
    conf.writeXml(new FileOutputStream(nodeHealthConfigFile));
    conf.addResource(nodeHealthConfigFile.getName());
    writeNodeHealthScriptFile("", false);
    // Node health script should not start if the node health script is not
    // executable.
    Assert.assertFalse("Node health script should start", NodeHealthCheckerService
        .shouldRun(conf));
    writeNodeHealthScriptFile("", true);
    Assert.assertTrue("Node health script should start", NodeHealthCheckerService
        .shouldRun(conf));
  }

  @Test
  public void testNodeHealthScript() throws Exception {
    RecordFactory factory = RecordFactoryProvider.getRecordFactory(null);
    NodeHealthStatus healthStatus =
        factory.newRecordInstance(NodeHealthStatus.class);
    String errorScript = "echo ERROR\n echo \"Tracker not healthy\"";
    String normalScript = "echo \"I am all fine\"";
    String timeOutScript = "sleep 4\n echo\"I am fine\"";
    Configuration conf = getConfForNodeHealthScript();
    conf.writeXml(new FileOutputStream(nodeHealthConfigFile));
    conf.addResource(nodeHealthConfigFile.getName());

    NodeHealthCheckerService nodeHealthChecker = new NodeHealthCheckerService(
        conf);
    TimerTask timer = nodeHealthChecker.getTimer();
    writeNodeHealthScriptFile(normalScript, true);
    timer.run();

    nodeHealthChecker.setHealthStatus(healthStatus);
    LOG.info("Checking initial healthy condition");
    // Check proper report conditions.
    Assert.assertTrue("Node health status reported unhealthy", healthStatus
        .getIsNodeHealthy());
    Assert.assertTrue("Node health status reported unhealthy", healthStatus
        .getHealthReport().isEmpty());

    // write out error file.
    // Healthy to unhealthy transition
    writeNodeHealthScriptFile(errorScript, true);
    // Run timer
    timer.run();
    // update health status
    nodeHealthChecker.setHealthStatus(healthStatus);
    LOG.info("Checking Healthy--->Unhealthy");
    Assert.assertFalse("Node health status reported healthy", healthStatus
        .getIsNodeHealthy());
    Assert.assertFalse("Node health status reported healthy", healthStatus
        .getHealthReport().isEmpty());
    
    // Check unhealthy to healthy transitions.
    writeNodeHealthScriptFile(normalScript, true);
    timer.run();
    nodeHealthChecker.setHealthStatus(healthStatus);
    LOG.info("Checking UnHealthy--->healthy");
    // Check proper report conditions.
    Assert.assertTrue("Node health status reported unhealthy", healthStatus
        .getIsNodeHealthy());
    Assert.assertTrue("Node health status reported unhealthy", healthStatus
        .getHealthReport().isEmpty());

    // Healthy to timeout transition.
    writeNodeHealthScriptFile(timeOutScript, true);
    timer.run();
    nodeHealthChecker.setHealthStatus(healthStatus);
    LOG.info("Checking Healthy--->timeout");
    Assert.assertFalse("Node health status reported healthy even after timeout",
        healthStatus.getIsNodeHealthy());
    Assert.assertEquals("Node time out message not propogated", healthStatus
        .getHealthReport(),
        NodeHealthCheckerService.NODE_HEALTH_SCRIPT_TIMED_OUT_MSG);
  }

}
