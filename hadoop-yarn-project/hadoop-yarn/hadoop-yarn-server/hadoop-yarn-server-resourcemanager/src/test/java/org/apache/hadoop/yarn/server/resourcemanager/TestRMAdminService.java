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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.junit.Assert.fail;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestRMAdminService {

  private final Configuration configuration = new YarnConfiguration();
  private MockRM rm = null;
  private FileSystem fs;
  private Path workingPath;
  private Path tmpDir;

  @Before
  public void setup() throws IOException {
    Configuration.addDefaultResource(YarnConfiguration.CS_CONFIGURATION_FILE);
    fs = FileSystem.get(configuration);
    workingPath =
        new Path(new File("target", this.getClass().getSimpleName()
            + "-remoteDir").getAbsolutePath());
    configuration.set(YarnConfiguration.FS_BASED_RM_CONF_STORE,
        workingPath.toString());
    tmpDir = new Path(new File("target", this.getClass().getSimpleName()
        + "-tmpDir").getAbsolutePath());
    fs.delete(workingPath, true);
    fs.delete(tmpDir, true);
    fs.mkdirs(workingPath);
    fs.mkdirs(tmpDir);
  }

  @After
  public void tearDown() throws IOException {
    if (rm != null) {
      rm.stop();
    }
    fs.delete(workingPath, true);
    fs.delete(tmpDir, true);
  }

  @Test
  public void testAdminRefreshQueuesWithLocalConfigurationProvider()
      throws IOException, YarnException {
    rm = new MockRM(configuration);
    rm.init(configuration);
    rm.start();

    CapacityScheduler cs =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    int maxAppsBefore = cs.getConfiguration().getMaximumSystemApplications();

    try {
      rm.adminService.refreshQueues(RefreshQueuesRequest.newInstance());
      Assert.assertEquals(maxAppsBefore, cs.getConfiguration()
          .getMaximumSystemApplications());
    } catch (Exception ex) {
      fail("Using localConfigurationProvider. Should not get any exception.");
    }
  }

  @Test
  public void testAdminRefreshQueuesWithFileSystemBasedConfigurationProvider()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");
    rm = new MockRM(configuration);
    rm.init(configuration);
    rm.start();

    // clean the remoteDirectory
    cleanRemoteDirectory();

    CapacityScheduler cs =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    int maxAppsBefore = cs.getConfiguration().getMaximumSystemApplications();

    try {
      rm.adminService.refreshQueues(RefreshQueuesRequest.newInstance());
      fail("FileSystemBasedConfigurationProvider is used." +
          " Should get an exception here");
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage().contains(
          "Can not find Configuration: capacity-scheduler.xml"));
    }

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.set("yarn.scheduler.capacity.maximum-applications", "5000");
    String csConfFile = writeConfigurationXML(csConf,
        "capacity-scheduler.xml");

    // upload the file into Remote File System
    uploadToRemoteFileSystem(new Path(csConfFile));

    rm.adminService.refreshQueues(RefreshQueuesRequest.newInstance());

    int maxAppsAfter = cs.getConfiguration().getMaximumSystemApplications();
    Assert.assertEquals(maxAppsAfter, 5000);
    Assert.assertTrue(maxAppsAfter != maxAppsBefore);
  }

  @Test
  public void testAdminAclsWithLocalConfigurationProvider() {
    rm = new MockRM(configuration);
    rm.init(configuration);
    rm.start();

    try {
      rm.adminService.refreshAdminAcls(RefreshAdminAclsRequest.newInstance());
    } catch (Exception ex) {
      fail("Using localConfigurationProvider. Should not get any exception.");
    }
  }

  @Test
  public void testAdminAclsWithFileSystemBasedConfigurationProvider()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");
    rm = new MockRM(configuration);
    rm.init(configuration);
    rm.start();

    // clean the remoteDirectory
    cleanRemoteDirectory();

    try {
      rm.adminService.refreshAdminAcls(RefreshAdminAclsRequest.newInstance());
      fail("FileSystemBasedConfigurationProvider is used." +
          " Should get an exception here");
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage().contains(
          "Can not find Configuration: yarn-site.xml"));
    }

    String aclStringBefore =
        rm.adminService.getAccessControlList().getAclString().trim();

    YarnConfiguration yarnConf = new YarnConfiguration();
    yarnConf.set(YarnConfiguration.YARN_ADMIN_ACL, "world:anyone:rwcda");
    String yarnConfFile = writeConfigurationXML(yarnConf, "yarn-site.xml");

    // upload the file into Remote File System
    uploadToRemoteFileSystem(new Path(yarnConfFile));
    rm.adminService.refreshAdminAcls(RefreshAdminAclsRequest.newInstance());

    String aclStringAfter =
        rm.adminService.getAccessControlList().getAclString().trim();

    Assert.assertTrue(!aclStringAfter.equals(aclStringBefore));
    Assert.assertEquals(aclStringAfter, "world:anyone:rwcda");
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

  private void uploadToRemoteFileSystem(Path filePath)
      throws IOException {
    fs.copyFromLocalFile(filePath, workingPath);
  }

  private void cleanRemoteDirectory() throws IOException {
    if (fs.exists(workingPath)) {
      for (FileStatus file : fs.listStatus(workingPath)) {
        fs.delete(file.getPath(), true);
      }
    }
  }
}
