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

package org.apache.hadoop.yarn.server.nodemanager.nodelabels;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.NodeLabelTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestScriptBasedNodeLabelsProvider extends NodeLabelTestBase {

  protected static File testRootDir = new File("target",
      TestScriptBasedNodeLabelsProvider.class.getName() + "-localDir")
          .getAbsoluteFile();

  private final File nodeLabelsScriptFile =
      new File(testRootDir, Shell.appendScriptExtension("failingscript"));

  private ScriptBasedNodeLabelsProvider nodeLabelsProvider;

  @Before
  public void setup() {
    testRootDir.mkdirs();
    nodeLabelsProvider = new ScriptBasedNodeLabelsProvider();
  }

  @After
  public void tearDown() throws Exception {
    if (testRootDir.exists()) {
      FileContext.getLocalFSFileContext()
          .delete(new Path(testRootDir.getAbsolutePath()), true);
    }
    if (nodeLabelsProvider != null) {
      nodeLabelsProvider.stop();
    }
  }

  private Configuration getConfForNodeLabelScript() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_SCRIPT_BASED_NODE_LABELS_PROVIDER_PATH,
        nodeLabelsScriptFile.getAbsolutePath());
    // set bigger interval so that test cases can be run
    conf.setLong(YarnConfiguration.NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS,
        1 * 60 * 60 * 1000l);
    conf.setLong(YarnConfiguration.NM_NODE_LABELS_PROVIDER_FETCH_TIMEOUT_MS,
        1000);
    return conf;
  }

  private void writeNodeLabelsScriptFile(String scriptStr,
      boolean setExecutable) throws IOException {
    PrintWriter pw = null;
    try {
      FileUtil.setWritable(nodeLabelsScriptFile, true);
      FileUtil.setReadable(nodeLabelsScriptFile, true);
      pw = new PrintWriter(new FileOutputStream(nodeLabelsScriptFile));
      pw.println(scriptStr);
      pw.flush();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      if (null != pw) {
        pw.close();
      }
    }
    FileUtil.setExecutable(nodeLabelsScriptFile, setExecutable);
  }

  @Test
  public void testNodeLabelsScriptRunnerCreation() throws IOException {
    // If no script configured then initialization of service should fail
    ScriptBasedNodeLabelsProvider nodeLabelsProvider =
        new ScriptBasedNodeLabelsProvider();
    initilizeServiceFailTest(
        "Expected to fail fast when no script is configured and "
            + "ScriptBasedNodeLabelsProvider service is inited",
        nodeLabelsProvider);

    // If script configured is blank then initialization of service should fail
    nodeLabelsProvider = new ScriptBasedNodeLabelsProvider();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_SCRIPT_BASED_NODE_LABELS_PROVIDER_PATH, "");
    initilizeServiceFailTest(
        "Expected to fail fast when script path configuration is blank"
            + "and ScriptBasedNodeLabelsProvider service is inited.",
        nodeLabelsProvider);

    // If script configured is not executable then no timertask /
    // NodeLabelsScriptRunner initialized
    nodeLabelsProvider = new ScriptBasedNodeLabelsProvider();
    writeNodeLabelsScriptFile("", false);
    initilizeServiceFailTest(
        "Expected to fail fast when script is not executable"
            + "and ScriptBasedNodeLabelsProvider service is inited.",
        nodeLabelsProvider);

    // If configured script is executable then timertask /
    // NodeLabelsScriptRunner should be initialized
    nodeLabelsProvider = new ScriptBasedNodeLabelsProvider();
    writeNodeLabelsScriptFile("", true);
    nodeLabelsProvider.init(getConfForNodeLabelScript());
    nodeLabelsProvider.start();
    Assert
        .assertNotNull("Node Label Script runner should be started when script"
            + " is executable", nodeLabelsProvider.getTimerTask());
    nodeLabelsProvider.stop();
  }

  private void initilizeServiceFailTest(String message,
      ScriptBasedNodeLabelsProvider nodeLabelsProvider) {
    try {
      nodeLabelsProvider.init(new Configuration());
      Assert.fail(message);
    } catch (ServiceStateException ex) {
      Assert.assertEquals("IOException was expected", IOException.class,
          ex.getCause().getClass());
    }
  }

  @Test
  public void testConfigForNoTimer() throws Exception {
    Configuration conf = getConfForNodeLabelScript();
    conf.setLong(YarnConfiguration
            .NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS,
        AbstractNodeDescriptorsProvider
            .DISABLE_NODE_DESCRIPTORS_PROVIDER_FETCH_TIMER);
    String normalScript = "echo NODE_PARTITION:X86";
    writeNodeLabelsScriptFile(normalScript, true);
    nodeLabelsProvider.init(conf);
    nodeLabelsProvider.start();
    Assert.assertNull(
        "Timer is not expected to be created when interval is configured as -1",
        nodeLabelsProvider.getScheduler());
    // Ensure that even though timer is not run script is run at least once so
    // that NM registers/updates Labels with RM
    assertNLCollectionEquals(toNodeLabelSet("X86"),
        nodeLabelsProvider.getDescriptors());
  }

  @Test
  public void testNodeLabelsScript() throws Exception {
    String scriptWithoutLabels = "";
    String normalScript = "echo NODE_PARTITION:Windows";
    String scrptWithMultipleLinesHavingNodeLabels =
        "echo NODE_PARTITION:RAM\n echo NODE_PARTITION:JDK1_6";
    String timeOutScript = Shell.WINDOWS
        ? "@echo off\nping -n 4 127.0.0.1 >nul\n" + "echo NODE_PARTITION:ALL"
        : "sleep 4\necho NODE_PARTITION:ALL";

    writeNodeLabelsScriptFile(scriptWithoutLabels, true);
    nodeLabelsProvider.init(getConfForNodeLabelScript());
    nodeLabelsProvider.start();
    Thread.sleep(500l);
    TimerTask timerTask = nodeLabelsProvider.getTimerTask();
    timerTask.run();
    Assert.assertNull(
        "Node Label Script runner should return null when script doesnt "
            + "give any Labels output",
        nodeLabelsProvider.getDescriptors());

    writeNodeLabelsScriptFile(normalScript, true);
    timerTask.run();
    assertNLCollectionEquals(toNodeLabelSet("Windows"),
        nodeLabelsProvider.getDescriptors());

    // multiple lines with partition tag then the last line's partition info
    // needs to be taken.
    writeNodeLabelsScriptFile(scrptWithMultipleLinesHavingNodeLabels, true);
    timerTask.run();
    assertNLCollectionEquals(toNodeLabelSet("JDK1_6"),
        nodeLabelsProvider.getDescriptors());

    // timeout script.
    writeNodeLabelsScriptFile(timeOutScript, true);
    timerTask.run();

    Assert.assertNotEquals("Node Labels should not be set after timeout ",
        toNodeLabelSet("ALL"), nodeLabelsProvider.getDescriptors());
  }
}
