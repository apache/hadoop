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

package org.apache.hadoop.tools;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class TestHadoopArchiveLogs {

  private static final long CLUSTER_TIMESTAMP = System.currentTimeMillis();
  private static final int FILE_SIZE_INCREMENT = 4096;
  private static final byte[] DUMMY_DATA = new byte[FILE_SIZE_INCREMENT];
  static {
    new Random().nextBytes(DUMMY_DATA);
  }

  @Test(timeout = 10000)
  public void testCheckFiles() throws Exception {
    Configuration conf = new Configuration();
    HadoopArchiveLogs hal = new HadoopArchiveLogs(conf);
    FileSystem fs = FileSystem.getLocal(conf);
    Path rootLogDir = new Path("target", "logs");
    String suffix = "logs";
    Path logDir = new Path(rootLogDir,
        new Path(System.getProperty("user.name"), suffix));
    fs.mkdirs(logDir);

    Assert.assertEquals(0, hal.eligibleApplications.size());
    ApplicationReport app1 = createAppReport(1);  // no files found
    ApplicationReport app2 = createAppReport(2);  // too few files
    Path app2Path = new Path(logDir, app2.getApplicationId().toString());
    fs.mkdirs(app2Path);
    createFile(fs, new Path(app2Path, "file1"), 1);
    hal.minNumLogFiles = 2;
    ApplicationReport app3 = createAppReport(3);  // too large
    Path app3Path = new Path(logDir, app3.getApplicationId().toString());
    fs.mkdirs(app3Path);
    createFile(fs, new Path(app3Path, "file1"), 2);
    createFile(fs, new Path(app3Path, "file2"), 5);
    hal.maxTotalLogsSize = FILE_SIZE_INCREMENT * 6;
    ApplicationReport app4 = createAppReport(4);  // has har already
    Path app4Path = new Path(logDir, app4.getApplicationId().toString());
    fs.mkdirs(app4Path);
    createFile(fs, new Path(app4Path, app4.getApplicationId() + ".har"), 1);
    ApplicationReport app5 = createAppReport(5);  // just right
    Path app5Path = new Path(logDir, app5.getApplicationId().toString());
    fs.mkdirs(app5Path);
    createFile(fs, new Path(app5Path, "file1"), 2);
    createFile(fs, new Path(app5Path, "file2"), 3);
    hal.eligibleApplications.add(app1);
    hal.eligibleApplications.add(app2);
    hal.eligibleApplications.add(app3);
    hal.eligibleApplications.add(app4);
    hal.eligibleApplications.add(app5);

    hal.checkFiles(fs, rootLogDir, suffix);
    Assert.assertEquals(1, hal.eligibleApplications.size());
    Assert.assertEquals(app5, hal.eligibleApplications.iterator().next());
  }

  @Test(timeout = 10000)
  public void testCheckMaxEligible() throws Exception {
    Configuration conf = new Configuration();
    HadoopArchiveLogs hal = new HadoopArchiveLogs(conf);
    ApplicationReport app1 = createAppReport(1);
    app1.setFinishTime(CLUSTER_TIMESTAMP - 5);
    ApplicationReport app2 = createAppReport(2);
    app2.setFinishTime(CLUSTER_TIMESTAMP - 10);
    ApplicationReport app3 = createAppReport(3);
    app3.setFinishTime(CLUSTER_TIMESTAMP + 5);
    ApplicationReport app4 = createAppReport(4);
    app4.setFinishTime(CLUSTER_TIMESTAMP + 10);
    ApplicationReport app5 = createAppReport(5);
    app5.setFinishTime(CLUSTER_TIMESTAMP);
    Assert.assertEquals(0, hal.eligibleApplications.size());
    hal.eligibleApplications.add(app1);
    hal.eligibleApplications.add(app2);
    hal.eligibleApplications.add(app3);
    hal.eligibleApplications.add(app4);
    hal.eligibleApplications.add(app5);
    hal.maxEligible = -1;
    hal.checkMaxEligible();
    Assert.assertEquals(5, hal.eligibleApplications.size());

    hal.maxEligible = 4;
    hal.checkMaxEligible();
    Assert.assertEquals(4, hal.eligibleApplications.size());
    Assert.assertFalse(hal.eligibleApplications.contains(app4));

    hal.maxEligible = 3;
    hal.checkMaxEligible();
    Assert.assertEquals(3, hal.eligibleApplications.size());
    Assert.assertFalse(hal.eligibleApplications.contains(app3));

    hal.maxEligible = 2;
    hal.checkMaxEligible();
    Assert.assertEquals(2, hal.eligibleApplications.size());
    Assert.assertFalse(hal.eligibleApplications.contains(app5));

    hal.maxEligible = 1;
    hal.checkMaxEligible();
    Assert.assertEquals(1, hal.eligibleApplications.size());
    Assert.assertFalse(hal.eligibleApplications.contains(app1));
  }

  @Test(timeout = 10000)
  public void testFindAggregatedApps() throws Exception {
    MiniYARNCluster yarnCluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
      yarnCluster =
          new MiniYARNCluster(TestHadoopArchiveLogs.class.getSimpleName(), 1,
              1, 1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();
      conf = yarnCluster.getConfig();

      RMContext rmContext = yarnCluster.getResourceManager().getRMContext();
      RMAppImpl app1 = (RMAppImpl)createRMApp(1, conf, rmContext,
          LogAggregationStatus.DISABLED);
      RMAppImpl app2 = (RMAppImpl)createRMApp(2, conf, rmContext,
          LogAggregationStatus.FAILED);
      RMAppImpl app3 = (RMAppImpl)createRMApp(3, conf, rmContext,
          LogAggregationStatus.NOT_START);
      RMAppImpl app4 = (RMAppImpl)createRMApp(4, conf, rmContext,
          LogAggregationStatus.SUCCEEDED);
      RMAppImpl app5 = (RMAppImpl)createRMApp(5, conf, rmContext,
          LogAggregationStatus.RUNNING);
      RMAppImpl app6 = (RMAppImpl)createRMApp(6, conf, rmContext,
          LogAggregationStatus.RUNNING_WITH_FAILURE);
      RMAppImpl app7 = (RMAppImpl)createRMApp(7, conf, rmContext,
          LogAggregationStatus.TIME_OUT);
      rmContext.getRMApps().put(app1.getApplicationId(), app1);
      rmContext.getRMApps().put(app2.getApplicationId(), app2);
      rmContext.getRMApps().put(app3.getApplicationId(), app3);
      rmContext.getRMApps().put(app4.getApplicationId(), app4);
      rmContext.getRMApps().put(app5.getApplicationId(), app5);
      rmContext.getRMApps().put(app6.getApplicationId(), app6);
      rmContext.getRMApps().put(app7.getApplicationId(), app7);

      HadoopArchiveLogs hal = new HadoopArchiveLogs(conf);
      Assert.assertEquals(0, hal.eligibleApplications.size());
      hal.findAggregatedApps();
      Assert.assertEquals(2, hal.eligibleApplications.size());
    } finally {
      if (yarnCluster != null) {
        yarnCluster.stop();
      }
    }
  }

  @Test(timeout = 10000)
  public void testGenerateScript() throws Exception {
    Configuration conf = new Configuration();
    HadoopArchiveLogs hal = new HadoopArchiveLogs(conf);
    ApplicationReport app1 = createAppReport(1);
    ApplicationReport app2 = createAppReport(2);
    hal.eligibleApplications.add(app1);
    hal.eligibleApplications.add(app2);

    File localScript = new File("target", "script.sh");
    Path workingDir = new Path("/tmp", "working");
    Path remoteRootLogDir = new Path("/tmp", "logs");
    String suffix = "logs";
    localScript.delete();
    Assert.assertFalse(localScript.exists());
    hal.generateScript(localScript, workingDir, remoteRootLogDir, suffix);
    Assert.assertTrue(localScript.exists());
    String script = IOUtils.toString(localScript.toURI());
    String[] lines = script.split(System.lineSeparator());
    Assert.assertEquals(16, lines.length);
    Assert.assertEquals("#!/bin/bash", lines[0]);
    Assert.assertEquals("set -e", lines[1]);
    Assert.assertEquals("set -x", lines[2]);
    Assert.assertEquals("if [ \"$YARN_SHELL_ID\" == \"1\" ]; then", lines[3]);
    if (lines[4].contains(app1.getApplicationId().toString())) {
      Assert.assertEquals("\tappId=\"" + app1.getApplicationId().toString()
          + "\"", lines[4]);
      Assert.assertEquals("\tappId=\"" + app2.getApplicationId().toString()
          + "\"", lines[7]);
    } else {
      Assert.assertEquals("\tappId=\"" + app2.getApplicationId().toString()
          + "\"", lines[4]);
      Assert.assertEquals("\tappId=\"" + app1.getApplicationId().toString()
          + "\"", lines[7]);
    }
    Assert.assertEquals("\tuser=\"" + System.getProperty("user.name") + "\"",
        lines[5]);
    Assert.assertEquals("elif [ \"$YARN_SHELL_ID\" == \"2\" ]; then", lines[6]);
    Assert.assertEquals("\tuser=\"" + System.getProperty("user.name") + "\"",
        lines[8]);
    Assert.assertEquals("else", lines[9]);
    Assert.assertEquals("\techo \"Unknown Mapping!\"", lines[10]);
    Assert.assertEquals("\texit 1", lines[11]);
    Assert.assertEquals("fi", lines[12]);
    Assert.assertEquals("export HADOOP_CLIENT_OPTS=\"-Xmx1024m\"", lines[13]);
    Assert.assertTrue(lines[14].startsWith("export HADOOP_CLASSPATH="));
    Assert.assertEquals("\"$HADOOP_HOME\"/bin/hadoop org.apache.hadoop.tools." +
        "HadoopArchiveLogsRunner -appId \"$appId\" -user \"$user\" -workingDir "
        + workingDir.toString() + " -remoteRootLogDir " +
        remoteRootLogDir.toString() + " -suffix " + suffix, lines[15]);
  }

  private static ApplicationReport createAppReport(int id) {
    ApplicationId appId = ApplicationId.newInstance(CLUSTER_TIMESTAMP, id);
    return ApplicationReport.newInstance(
        appId,
        ApplicationAttemptId.newInstance(appId, 1),
        System.getProperty("user.name"),
        null, null, null, 0, null, YarnApplicationState.FINISHED, null,
        null, 0L, 0L, FinalApplicationStatus.SUCCEEDED, null, null, 100f,
        null, null);
  }

  private static void createFile(FileSystem fs, Path p, long sizeMultiple)
      throws IOException {
    FSDataOutputStream out = null;
    try {
      out = fs.create(p);
      for (int i = 0 ; i < sizeMultiple; i++) {
        out.write(DUMMY_DATA);
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  private static RMApp createRMApp(int id, Configuration conf, RMContext rmContext,
       final LogAggregationStatus aggStatus) {
    ApplicationId appId = ApplicationId.newInstance(CLUSTER_TIMESTAMP, id);
    ApplicationSubmissionContext submissionContext =
        ApplicationSubmissionContext.newInstance(appId, "test", "default",
            Priority.newInstance(0), null, false, true,
            2, Resource.newInstance(10, 2), "test");
    return new RMAppImpl(appId, rmContext, conf, "test",
        System.getProperty("user.name"), "default", submissionContext,
        rmContext.getScheduler(),
        rmContext.getApplicationMasterService(),
        System.currentTimeMillis(), "test",
        null, null) {
      @Override
      public ApplicationReport createAndGetApplicationReport(
          String clientUserName, boolean allowAccess) {
        ApplicationReport report =
            super.createAndGetApplicationReport(clientUserName, allowAccess);
        report.setLogAggregationStatus(aggStatus);
        return report;
      }
    };
  }
}
