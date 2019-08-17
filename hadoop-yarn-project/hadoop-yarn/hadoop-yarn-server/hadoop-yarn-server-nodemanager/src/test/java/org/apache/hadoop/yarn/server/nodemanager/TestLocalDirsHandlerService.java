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
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLocalDirsHandlerService {
  private static final File testDir = new File("target",
      TestDirectoryCollection.class.getName()).getAbsoluteFile();
  private static final File testFile = new File(testDir, "testfile");

  @Before
  public void setup() throws IOException {
    testDir.mkdirs();
    testFile.createNewFile();
  }

  @After
  public void teardown() {
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testDirStructure() throws Exception {
    Configuration conf = new YarnConfiguration();
    String localDir1 = new File("file:///" + testDir, "localDir1").getPath();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir1);
    String logDir1 = new File("file:///" + testDir, "logDir1").getPath();
    conf.set(YarnConfiguration.NM_LOG_DIRS, logDir1);
    LocalDirsHandlerService dirSvc = new LocalDirsHandlerService();
    dirSvc.init(conf);
    Assert.assertEquals(1, dirSvc.getLocalDirs().size());
    dirSvc.close();
  }

  @Test
  public void testValidPathsDirHandlerService() throws Exception {
    Configuration conf = new YarnConfiguration();
    String localDir1 = new File("file:///" + testDir, "localDir1").getPath();
    String localDir2 = new File("hdfs:///" + testDir, "localDir2").getPath();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir1 + "," + localDir2);
    String logDir1 = new File("file:///" + testDir, "logDir1").getPath();
    conf.set(YarnConfiguration.NM_LOG_DIRS, logDir1);
    LocalDirsHandlerService dirSvc = new LocalDirsHandlerService();
    try {
      dirSvc.init(conf);
      Assert.fail("Service should have thrown an exception due to wrong URI");
    } catch (YarnRuntimeException e) {
    }
    Assert.assertEquals("Service should not be inited",
                        STATE.STOPPED,
                        dirSvc.getServiceState());
    dirSvc.close();
  }
  
  @Test
  public void testGetFullDirs() throws Exception {
    Configuration conf = new YarnConfiguration();

    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    FileContext localFs = FileContext.getLocalFSFileContext(conf);

    String localDir1 = new File(testDir, "localDir1").getPath();
    String localDir2 = new File(testDir, "localDir2").getPath();
    String logDir1 = new File(testDir, "logDir1").getPath();
    String logDir2 = new File(testDir, "logDir2").getPath();
    Path localDir1Path = new Path(localDir1);
    Path logDir1Path = new Path(logDir1);
    FsPermission dirPermissions = new FsPermission((short) 0410);
    localFs.mkdir(localDir1Path, dirPermissions, true);
    localFs.mkdir(logDir1Path, dirPermissions, true);

    conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir1 + "," + localDir2);
    conf.set(YarnConfiguration.NM_LOG_DIRS, logDir1 + "," + logDir2);
    conf.setFloat(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE,
      0.0f);
    NodeManagerMetrics nm = NodeManagerMetrics.create();
    LocalDirsHandlerService dirSvc = new LocalDirsHandlerService(nm);
    dirSvc.init(conf);
    Assert.assertEquals(0, dirSvc.getLocalDirs().size());
    Assert.assertEquals(0, dirSvc.getLogDirs().size());
    Assert.assertEquals(1, dirSvc.getDiskFullLocalDirs().size());
    Assert.assertEquals(1, dirSvc.getDiskFullLogDirs().size());
    // check the metrics
    Assert.assertEquals(2, nm.getBadLocalDirs());
    Assert.assertEquals(2, nm.getBadLogDirs());
    Assert.assertEquals(0, nm.getGoodLocalDirsDiskUtilizationPerc());
    Assert.assertEquals(0, nm.getGoodLogDirsDiskUtilizationPerc());

    Assert.assertEquals("",
        dirSvc.getConfig().get(LocalDirsHandlerService.NM_GOOD_LOCAL_DIRS));
    Assert.assertEquals("",
        dirSvc.getConfig().get(LocalDirsHandlerService.NM_GOOD_LOG_DIRS));
    Assert.assertEquals(localDir1 + "," + localDir2,
        dirSvc.getConfig().get(YarnConfiguration.NM_LOCAL_DIRS));
    Assert.assertEquals(logDir1 + "," + logDir2,
        dirSvc.getConfig().get(YarnConfiguration.NM_LOG_DIRS));

    conf.setFloat(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE,
      100.0f);
    nm = NodeManagerMetrics.create();
    dirSvc = new LocalDirsHandlerService(nm);
    dirSvc.init(conf);
    Assert.assertEquals(1, dirSvc.getLocalDirs().size());
    Assert.assertEquals(1, dirSvc.getLogDirs().size());
    Assert.assertEquals(0, dirSvc.getDiskFullLocalDirs().size());
    Assert.assertEquals(0, dirSvc.getDiskFullLogDirs().size());
    // check the metrics
    File dir = new File(localDir1);
    int utilizationPerc =
        (int) ((dir.getTotalSpace() - dir.getUsableSpace()) * 100 /
            dir.getTotalSpace());
    Assert.assertEquals(1, nm.getBadLocalDirs());
    Assert.assertEquals(1, nm.getBadLogDirs());
    Assert.assertEquals(utilizationPerc,
      nm.getGoodLocalDirsDiskUtilizationPerc());
    Assert
      .assertEquals(utilizationPerc, nm.getGoodLogDirsDiskUtilizationPerc());

    Assert.assertEquals(new Path(localDir2).toString(),
        dirSvc.getConfig().get(LocalDirsHandlerService.NM_GOOD_LOCAL_DIRS));
    Assert.assertEquals(new Path(logDir2).toString(),
        dirSvc.getConfig().get(LocalDirsHandlerService.NM_GOOD_LOG_DIRS));
    Assert.assertEquals(localDir1 + "," + localDir2,
        dirSvc.getConfig().get(YarnConfiguration.NM_LOCAL_DIRS));
    Assert.assertEquals(logDir1 + "," + logDir2,
        dirSvc.getConfig().get(YarnConfiguration.NM_LOG_DIRS));

    FileUtils.deleteDirectory(new File(localDir1));
    FileUtils.deleteDirectory(new File(localDir2));
    FileUtils.deleteDirectory(new File(logDir1));
    FileUtils.deleteDirectory(new File(logDir2));
    dirSvc.close();
  }
}
