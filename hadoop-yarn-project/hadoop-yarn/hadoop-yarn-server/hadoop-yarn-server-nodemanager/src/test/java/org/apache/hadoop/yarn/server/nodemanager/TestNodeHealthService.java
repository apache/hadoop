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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.NodeHealthScriptRunner;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class TestNodeHealthService {

  private static volatile Logger LOG =
       LoggerFactory.getLogger(TestNodeHealthService.class);

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

  private Configuration getConfForNodeHealthScript() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_PATH,
        nodeHealthscriptFile.getAbsolutePath());
    conf.setLong(YarnConfiguration.NM_HEALTH_CHECK_INTERVAL_MS, 500);
    conf.setLong(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS, 1000);
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
    Configuration conf = getConfForNodeHealthScript();
    conf.writeXml(new FileOutputStream(nodeHealthConfigFile));
    conf.addResource(nodeHealthConfigFile.getName());
    writeNodeHealthScriptFile("", true);

    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    NodeHealthScriptRunner nodeHealthScriptRunner =
        spy(NodeManager.getNodeHealthScriptRunner(conf));
    NodeHealthCheckerService nodeHealthChecker = new NodeHealthCheckerService(
    		nodeHealthScriptRunner, dirsHandler);
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
}
