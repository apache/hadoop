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
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link NodeHealthScriptRunner}.
 */
public class TestNodeHealthScriptRunner {

  private static File testRootDir = new File("target",
      TestNodeHealthScriptRunner.class.getName() +
      "-localDir").getAbsoluteFile();

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

  private void writeNodeHealthScriptFile(String scriptStr,
      boolean setExecutable) throws IOException {
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

  private NodeHealthScriptRunner createNodeHealthScript() {
    String scriptName = "custom";

    YarnConfiguration conf = new YarnConfiguration();
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
    conf.set(pathConfig, nodeHealthscriptFile.getAbsolutePath());

    return NodeHealthScriptRunner.newInstance("custom", conf);
  }

  @Test
  public void testNodeHealthScriptShouldRun() throws IOException {
    assertFalse("Node health script should start",
        NodeHealthScriptRunner.shouldRun("script",
            nodeHealthscriptFile.getAbsolutePath()));
    writeNodeHealthScriptFile("", false);
    // Node health script should not start if the node health script is not
    // executable.
    assertFalse("Node health script should start",
        NodeHealthScriptRunner.shouldRun("script",
            nodeHealthscriptFile.getAbsolutePath()));
    writeNodeHealthScriptFile("", true);
    assertTrue("Node health script should start",
        NodeHealthScriptRunner.shouldRun("script",
            nodeHealthscriptFile.getAbsolutePath()));
  }

  @Test
  public void testNodeHealthScript() throws Exception {
    String errorScript = "echo ERROR\n echo \"Tracker not healthy\"";
    String normalScript = "echo \"I am all fine\"";
    String timeOutScript =
        Shell.WINDOWS ?
            "@echo off\nping -n 4 127.0.0.1 >nul\necho \"I am fine\""
            : "sleep 4\necho \"I am fine\"";
    String exitCodeScript = "exit 127";

    Configuration conf = new Configuration();
    writeNodeHealthScriptFile(normalScript, true);
    NodeHealthScriptRunner nodeHealthScriptRunner = createNodeHealthScript();
    nodeHealthScriptRunner.init(conf);
    TimerTask timerTask = nodeHealthScriptRunner.getTimerTask();

    timerTask.run();
    // Normal Script runs successfully
    assertTrue("Node health status reported unhealthy",
        nodeHealthScriptRunner.isHealthy());
    assertTrue(nodeHealthScriptRunner.getHealthReport().isEmpty());

    // Error script.
    writeNodeHealthScriptFile(errorScript, true);
    // Run timer
    timerTask.run();
    assertFalse("Node health status reported healthy",
        nodeHealthScriptRunner.isHealthy());
    assertTrue(
        nodeHealthScriptRunner.getHealthReport().contains("ERROR"));

    // Healthy script.
    writeNodeHealthScriptFile(normalScript, true);
    timerTask.run();
    assertTrue("Node health status reported unhealthy",
        nodeHealthScriptRunner.isHealthy());
    assertTrue(nodeHealthScriptRunner.getHealthReport().isEmpty());

    // Timeout script.
    writeNodeHealthScriptFile(timeOutScript, true);
    timerTask.run();
    assertFalse("Node health status reported healthy even after timeout",
        nodeHealthScriptRunner.isHealthy());
    assertEquals(
        NodeHealthScriptRunner.NODE_HEALTH_SCRIPT_TIMED_OUT_MSG,
        nodeHealthScriptRunner.getHealthReport());

    // Exit code 127
    writeNodeHealthScriptFile(exitCodeScript, true);
    timerTask.run();
    assertTrue("Node health status reported unhealthy",
        nodeHealthScriptRunner.isHealthy());
    assertEquals("", nodeHealthScriptRunner.getHealthReport());
  }
}
