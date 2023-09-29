/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestQueueConfigurationAutoRefreshPolicy  {

  private Configuration configuration;
  private MockRM rm = null;
  private FileSystem fs;
  private Path workingPath;
  private Path workingPathRecover;
  private Path fileSystemWorkingPath;
  private Path tmpDir;
  private QueueConfigurationAutoRefreshPolicy policy;

  static {
    YarnConfiguration.addDefaultResource(
        YarnConfiguration.CS_CONFIGURATION_FILE);
    YarnConfiguration.addDefaultResource(
        YarnConfiguration.DR_CONFIGURATION_FILE);
  }

  @Before
  public void setup() throws IOException {
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.setMiniClusterMode(true);

    configuration = new YarnConfiguration();
    configuration.set(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class.getCanonicalName());
    fs = FileSystem.get(configuration);
    workingPath = new Path(QueueConfigurationAutoRefreshPolicy.
        class.getClassLoader().
        getResource(".").toString());
    workingPathRecover = new Path(QueueConfigurationAutoRefreshPolicy.
        class.getClassLoader().
        getResource(".").toString() + "/" + "Recover");
    fileSystemWorkingPath =
        new Path(new File("target", this.getClass().getSimpleName()
            + "-remoteDir").getAbsolutePath());

    tmpDir = new Path(new File("target", this.getClass().getSimpleName()
        + "-tmpDir").getAbsolutePath());
    fs.delete(fileSystemWorkingPath, true);
    fs.mkdirs(fileSystemWorkingPath);
    fs.delete(tmpDir, true);
    fs.mkdirs(tmpDir);

    policy =
        new QueueConfigurationAutoRefreshPolicy();
  }

  private String writeConfigurationXML(Configuration conf, String confXMLName)
      throws IOException {
    DataOutputStream output = null;
    try {
      final File confFile = new File(tmpDir.toString(), confXMLName);
      if (confFile.exists()) {
        confFile.delete();
      }
      if (!confFile.createNewFile()) {
        Assert.fail("Can not create " + confXMLName);
      }
      output = new DataOutputStream(
          new FileOutputStream(confFile));
      conf.writeXml(output);
      return confFile.getAbsolutePath();
    } finally {
      if (output != null) {
        output.close();
      }
    }
  }

  private void uploadConfiguration(Boolean isFileSystemBased,
      Configuration conf, String confFileName)
      throws IOException {
    String csConfFile = writeConfigurationXML(conf, confFileName);
    if (isFileSystemBased) {
      // upload the file into Remote File System
      uploadToRemoteFileSystem(new Path(csConfFile),
          fileSystemWorkingPath);
    } else {
      // upload the file into Work Path for Local File
      uploadToRemoteFileSystem(new Path(csConfFile),
          workingPath);
    }
  }

  private void uploadToRemoteFileSystem(Path filePath, Path remotePath)
      throws IOException {
    fs.copyFromLocalFile(filePath, remotePath);
  }

  private void uploadDefaultConfiguration(Boolean
      isFileSystemBased) throws IOException {
    Configuration conf = new Configuration();
    uploadConfiguration(isFileSystemBased,
        conf, "core-site.xml");

    YarnConfiguration yarnConf = new YarnConfiguration();

    uploadConfiguration(isFileSystemBased,
        yarnConf, "yarn-site.xml");

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    uploadConfiguration(isFileSystemBased,
        csConf, "capacity-scheduler.xml");

    Configuration hadoopPolicyConf = new Configuration(false);
    hadoopPolicyConf
        .addResource(YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
    uploadConfiguration(isFileSystemBased,
        hadoopPolicyConf, "hadoop-policy.xml");
  }

  @Test
  public void testFileSystemBasedEditSchedule() throws Exception {
    // Test FileSystemBasedConfigurationProvider scheduled
    testCommon(true);
  }

  @Test
  public void testLocalFileBasedEditSchedule() throws Exception {
    // Prepare for recover for local file default.
    fs.mkdirs(workingPath);
    fs.copyFromLocalFile(new Path(workingPath.toString()
        + "/" + YarnConfiguration.CORE_SITE_CONFIGURATION_FILE),
        new Path(workingPathRecover.toString()
        + "/" + YarnConfiguration.CORE_SITE_CONFIGURATION_FILE));

    fs.copyFromLocalFile(new Path(workingPath.toString()
        + "/" + YarnConfiguration.YARN_SITE_CONFIGURATION_FILE),
        new Path(workingPathRecover.toString()
        + "/" + YarnConfiguration.YARN_SITE_CONFIGURATION_FILE));

    fs.copyFromLocalFile(new Path(workingPath.toString()
        + "/" + YarnConfiguration.CS_CONFIGURATION_FILE),
        new Path(workingPathRecover.toString()
        + "/" + YarnConfiguration.CS_CONFIGURATION_FILE));

    // Test LocalConfigurationProvider scheduled
    testCommon(false);

    // Recover for recover for local file default.
    fs.copyFromLocalFile(new Path(workingPathRecover.toString()
        + "/" + YarnConfiguration.CORE_SITE_CONFIGURATION_FILE),
        new Path(workingPath.toString()
        + "/" + YarnConfiguration.CORE_SITE_CONFIGURATION_FILE));

    fs.copyFromLocalFile(new Path(workingPathRecover.toString()
        + "/" + YarnConfiguration.YARN_SITE_CONFIGURATION_FILE),
        new Path(workingPath.toString()
        + "/" + YarnConfiguration.YARN_SITE_CONFIGURATION_FILE));

    fs.copyFromLocalFile(new Path(workingPathRecover.toString()
        + "/" + YarnConfiguration.CS_CONFIGURATION_FILE),
        new Path(workingPath.toString()
        + "/" + YarnConfiguration.CS_CONFIGURATION_FILE));

    fs.delete(workingPathRecover, true);
  }

  public void testCommon(Boolean isFileSystemBased) throws Exception {

    // Set auto refresh interval to 1s
    configuration.setLong(CapacitySchedulerConfiguration.
            QUEUE_AUTO_REFRESH_MONITORING_INTERVAL,
        1000L);

    if (isFileSystemBased) {
      configuration.set(YarnConfiguration.FS_BASED_RM_CONF_STORE,
          fileSystemWorkingPath.toString());
    }

    //upload default configurations
    uploadDefaultConfiguration(isFileSystemBased);

    if (isFileSystemBased) {
      configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
          FileSystemBasedConfigurationProvider.class.getCanonicalName());
    } else {
      configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
          LocalConfigurationProvider.class.getCanonicalName());
    }

    // upload the auto refresh related configurations
    uploadConfiguration(isFileSystemBased,
        configuration, "yarn-site.xml");
    uploadConfiguration(isFileSystemBased,
        configuration, "capacity-scheduler.xml");

    rm = new MockRM(configuration);
    rm.init(configuration);
    policy.init(configuration,
        rm.getRMContext(),
        rm.getResourceScheduler());
    rm.start();

    CapacityScheduler cs =
        (CapacityScheduler) rm.getRMContext().getScheduler();

    int maxAppsBefore = cs.getConfiguration().getMaximumSystemApplications();

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setInt(CapacitySchedulerConfiguration.MAXIMUM_SYSTEM_APPLICATIONS,
        5000);
    uploadConfiguration(isFileSystemBased,
        csConf, "capacity-scheduler.xml");

    // Refreshed first time.
    policy.editSchedule();

    // Make sure refresh successfully.
    Assert.assertFalse(policy.getLastReloadAttemptFailed());
    long oldModified = policy.getLastModified();
    long oldSuccess = policy.getLastReloadAttempt();

    Assert.assertTrue(oldSuccess > oldModified);

    int maxAppsAfter = cs.getConfiguration().getMaximumSystemApplications();
    Assert.assertEquals(maxAppsAfter, 5000);
    Assert.assertTrue(maxAppsAfter != maxAppsBefore);

    // Trigger interval for refresh.
    GenericTestUtils.waitFor(() -> (policy.getClock().getTime() -
            policy.getLastReloadAttempt()) / 1000 > 1,
        500, 3000);

    // Upload for modified.
    csConf.setInt(CapacitySchedulerConfiguration.MAXIMUM_SYSTEM_APPLICATIONS,
        3000);
    uploadConfiguration(isFileSystemBased,
        csConf, "capacity-scheduler.xml");

    policy.editSchedule();
    // Wait for triggered refresh.
    GenericTestUtils.waitFor(() -> policy.getLastReloadAttempt() >
                    policy.getLastModified(),
        500, 3000);

    // Make sure refresh successfully.
    Assert.assertFalse(policy.getLastReloadAttemptFailed());
    oldModified = policy.getLastModified();
    oldSuccess = policy.getLastReloadAttempt();
    Assert.assertTrue(oldSuccess > oldModified);
    Assert.assertEquals(cs.getConfiguration().
        getMaximumSystemApplications(), 3000);

    // Trigger interval for refresh.
    GenericTestUtils.waitFor(() -> (policy.getClock().getTime() -
          policy.getLastReloadAttempt()) / 1000 > 1,
        500, 3000);

    // Without modified
    policy.editSchedule();
    Assert.assertEquals(oldModified,
        policy.getLastModified());
    Assert.assertEquals(oldSuccess,
        policy.getLastReloadAttempt());
  }

  @After
  public void tearDown() throws IOException {
    if (rm != null) {
      rm.stop();
    }
    fs.delete(fileSystemWorkingPath, true);
    fs.delete(tmpDir, true);
  }
}
