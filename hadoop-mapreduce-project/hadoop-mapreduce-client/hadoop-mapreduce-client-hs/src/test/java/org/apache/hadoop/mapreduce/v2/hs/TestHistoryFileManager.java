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

package org.apache.hadoop.mapreduce.v2.hs;


import java.io.File;
import java.io.FileOutputStream;
import java.util.UUID;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.test.CoreTestDriver;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestHistoryFileManager {
  private static MiniDFSCluster dfsCluster = null;
  private static MiniDFSCluster dfsCluster2 = null;
  private static String coreSitePath;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpClass() throws Exception {
    coreSitePath = "." + File.separator + "target" + File.separator +
            "test-classes" + File.separator + "core-site.xml";
    Configuration conf = new HdfsConfiguration();
    Configuration conf2 = new HdfsConfiguration();
    dfsCluster = new MiniDFSCluster.Builder(conf).build();
    conf2.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
            conf.get(MiniDFSCluster.HDFS_MINIDFS_BASEDIR) + "_2");
    dfsCluster2 = new MiniDFSCluster.Builder(conf2).build();
  }

  @AfterClass
  public static void cleanUpClass() throws Exception {
    dfsCluster.shutdown();
    dfsCluster2.shutdown();
  }

  @After
  public void cleanTest() throws Exception {
    new File(coreSitePath).delete();
  }

  private String getDoneDirNameForTest() {
    return "/" + name.getMethodName();
  }

  private String getIntermediateDoneDirNameForTest() {
    return "/intermediate_" + name.getMethodName();
  }

  private void testTryCreateHistoryDirs(Configuration conf, boolean expected)
      throws Exception {
    conf.set(JHAdminConfig.MR_HISTORY_DONE_DIR, getDoneDirNameForTest());
    conf.set(JHAdminConfig.MR_HISTORY_INTERMEDIATE_DONE_DIR, getIntermediateDoneDirNameForTest());
    HistoryFileManager hfm = new HistoryFileManager();
    hfm.conf = conf;
    Assert.assertEquals(expected, hfm.tryCreatingHistoryDirs(false));
  }

  @Test
  public void testCreateDirsWithoutFileSystem() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:1");
    testTryCreateHistoryDirs(conf, false);
  }

  @Test
  public void testCreateDirsWithFileSystem() throws Exception {
    dfsCluster.getFileSystem().setSafeMode(
        HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    Assert.assertFalse(dfsCluster.getFileSystem().isInSafeMode());
    testTryCreateHistoryDirs(dfsCluster.getConfiguration(0), true);
  }

  @Test
  public void testUpdateDirPermissions() throws Exception {
    DistributedFileSystem fs = dfsCluster.getFileSystem();
    fs.setSafeMode( HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    Assert.assertFalse(dfsCluster.getFileSystem().isInSafeMode());
    Configuration conf = dfsCluster.getConfiguration(0);
    conf.set(JHAdminConfig.MR_HISTORY_DONE_DIR, getDoneDirNameForTest());
    conf.set(JHAdminConfig.MR_HISTORY_INTERMEDIATE_DONE_DIR, getIntermediateDoneDirNameForTest());
    Path p1a = new Path(getDoneDirNameForTest(), "2013");
    Path p1b = new Path(p1a, "02");
    Path p1c = new Path(p1b, "15");
    Path p1d = new Path(p1c, "000000");
    Path p2a = new Path(getDoneDirNameForTest(), "2013");
    Path p2b = new Path(p2a, "03");
    Path p2c = new Path(p2b, "14");
    Path p2d = new Path(p2c, "000001");
    FsPermission oldPerms = new FsPermission((short) 0770);
    fs.mkdirs(p1d);
    fs.mkdirs(p2d);
    fs.setPermission(p1a, oldPerms);
    fs.setPermission(p1b, oldPerms);
    fs.setPermission(p1c, oldPerms);
    fs.setPermission(p1d, oldPerms);
    fs.setPermission(p2a, oldPerms);
    fs.setPermission(p2b, oldPerms);
    fs.setPermission(p2c, oldPerms);
    fs.setPermission(p2d, oldPerms);
    Path p1File = new Path(p1d, "foo.jhist");
    Assert.assertTrue(fs.createNewFile(p1File));
    fs.setPermission(p1File, JobHistoryUtils.HISTORY_DONE_FILE_PERMISSION);
    Path p2File = new Path(p2d, "bar.jhist");
    Assert.assertTrue(fs.createNewFile(p2File));
    fs.setPermission(p2File, JobHistoryUtils.HISTORY_DONE_FILE_PERMISSION);
    Assert.assertEquals(oldPerms, fs.getFileStatus(p1a).getPermission());
    Assert.assertEquals(oldPerms, fs.getFileStatus(p1b).getPermission());
    Assert.assertEquals(oldPerms, fs.getFileStatus(p1c).getPermission());
    Assert.assertEquals(oldPerms, fs.getFileStatus(p1d).getPermission());
    Assert.assertEquals(JobHistoryUtils.HISTORY_DONE_FILE_PERMISSION,
        fs.getFileStatus(p1File).getPermission());
    Assert.assertEquals(oldPerms, fs.getFileStatus(p2a).getPermission());
    Assert.assertEquals(oldPerms, fs.getFileStatus(p2b).getPermission());
    Assert.assertEquals(oldPerms, fs.getFileStatus(p2c).getPermission());
    Assert.assertEquals(oldPerms, fs.getFileStatus(p2d).getPermission());
    Assert.assertEquals(JobHistoryUtils.HISTORY_DONE_FILE_PERMISSION,
        fs.getFileStatus(p2File).getPermission());
    HistoryFileManager hfm = new HistoryFileManager();
    hfm.conf = conf;
    Assert.assertEquals(true, hfm.tryCreatingHistoryDirs(false));
    Assert.assertEquals(JobHistoryUtils.HISTORY_DONE_DIR_PERMISSION,
        fs.getFileStatus(p1a).getPermission());
    Assert.assertEquals(JobHistoryUtils.HISTORY_DONE_DIR_PERMISSION,
        fs.getFileStatus(p1b).getPermission());
    Assert.assertEquals(JobHistoryUtils.HISTORY_DONE_DIR_PERMISSION,
        fs.getFileStatus(p1c).getPermission());
    Assert.assertEquals(JobHistoryUtils.HISTORY_DONE_DIR_PERMISSION,
        fs.getFileStatus(p1d).getPermission());
    Assert.assertEquals(JobHistoryUtils.HISTORY_DONE_FILE_PERMISSION,
        fs.getFileStatus(p2File).getPermission());
    Assert.assertEquals(JobHistoryUtils.HISTORY_DONE_DIR_PERMISSION,
        fs.getFileStatus(p2a).getPermission());
    Assert.assertEquals(JobHistoryUtils.HISTORY_DONE_DIR_PERMISSION,
        fs.getFileStatus(p2b).getPermission());
    Assert.assertEquals(JobHistoryUtils.HISTORY_DONE_DIR_PERMISSION,
        fs.getFileStatus(p2c).getPermission());
    Assert.assertEquals(JobHistoryUtils.HISTORY_DONE_DIR_PERMISSION,
        fs.getFileStatus(p2d).getPermission());
    Assert.assertEquals(JobHistoryUtils.HISTORY_DONE_FILE_PERMISSION,
        fs.getFileStatus(p2File).getPermission());
  }

  @Test
  public void testCreateDirsWithAdditionalFileSystem() throws Exception {
    dfsCluster.getFileSystem().setSafeMode(
        HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    dfsCluster2.getFileSystem().setSafeMode(
        HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    Assert.assertFalse(dfsCluster.getFileSystem().isInSafeMode());
    Assert.assertFalse(dfsCluster2.getFileSystem().isInSafeMode());

    // Set default configuration to the first cluster
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
            dfsCluster.getURI().toString());
    FileOutputStream os = new FileOutputStream(coreSitePath);
    conf.writeXml(os);
    os.close();

    testTryCreateHistoryDirs(dfsCluster2.getConfiguration(0), true);

    // Directories should be created only in the default file system (dfsCluster)
    Assert.assertTrue(dfsCluster.getFileSystem()
            .exists(new Path(getDoneDirNameForTest())));
    Assert.assertTrue(dfsCluster.getFileSystem()
            .exists(new Path(getIntermediateDoneDirNameForTest())));
    Assert.assertFalse(dfsCluster2.getFileSystem()
            .exists(new Path(getDoneDirNameForTest())));
    Assert.assertFalse(dfsCluster2.getFileSystem()
            .exists(new Path(getIntermediateDoneDirNameForTest())));
  }

  @Test
  public void testCreateDirsWithFileSystemInSafeMode() throws Exception {
    dfsCluster.getFileSystem().setSafeMode(
        HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    Assert.assertTrue(dfsCluster.getFileSystem().isInSafeMode());
    testTryCreateHistoryDirs(dfsCluster.getConfiguration(0), false);
  }

  private void testCreateHistoryDirs(Configuration conf, Clock clock)
      throws Exception {
    conf.set(JHAdminConfig.MR_HISTORY_DONE_DIR, "/" + UUID.randomUUID());
    conf.set(JHAdminConfig.MR_HISTORY_INTERMEDIATE_DONE_DIR, "/" + UUID.randomUUID());
    HistoryFileManager hfm = new HistoryFileManager();
    hfm.conf = conf;
    hfm.createHistoryDirs(clock, 500, 2000);
  }

  @Test
  public void testCreateDirsWithFileSystemBecomingAvailBeforeTimeout()
      throws Exception {
    dfsCluster.getFileSystem().setSafeMode(
        HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    Assert.assertTrue(dfsCluster.getFileSystem().isInSafeMode());
    new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(500);
          dfsCluster.getFileSystem().setSafeMode(
              HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
          Assert.assertTrue(dfsCluster.getFileSystem().isInSafeMode());
        } catch (Exception ex) {
          Assert.fail(ex.toString());
        }
      }
    }.start();
    testCreateHistoryDirs(dfsCluster.getConfiguration(0), new SystemClock());
  }

  @Test(expected = YarnRuntimeException.class)
  public void testCreateDirsWithFileSystemNotBecomingAvailBeforeTimeout()
      throws Exception {
    dfsCluster.getFileSystem().setSafeMode(
        HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    Assert.assertTrue(dfsCluster.getFileSystem().isInSafeMode());
    final ControlledClock clock = new ControlledClock(new SystemClock());
    clock.setTime(1);
    new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(500);
          clock.setTime(3000);
        } catch (Exception ex) {
          Assert.fail(ex.toString());
        }
      }
    }.start();
    testCreateHistoryDirs(dfsCluster.getConfiguration(0), clock);
  }

}
