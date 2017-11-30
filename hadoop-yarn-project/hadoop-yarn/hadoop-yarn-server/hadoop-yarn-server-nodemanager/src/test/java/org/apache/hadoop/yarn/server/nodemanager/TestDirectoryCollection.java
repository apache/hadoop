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
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.DirectoryCollection.DirsChangeListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;

public class TestDirectoryCollection {

  private static final File testDir = new File("target",
      TestDirectoryCollection.class.getName()).getAbsoluteFile();
  private static final File testFile = new File(testDir, "testfile");

  private Configuration conf;
  private FileContext localFs;

  @Before
  public void setupForTests() throws IOException {
    conf = new Configuration();
    localFs = FileContext.getLocalFSFileContext(conf);
    testDir.mkdirs();
    testFile.createNewFile();
  }

  @After
  public void teardown() {
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testConcurrentAccess() throws IOException {
    // Initialize DirectoryCollection with a file instead of a directory
    
    String[] dirs = {testFile.getPath()};
    DirectoryCollection dc =
        new DirectoryCollection(dirs, conf.getFloat(
          YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE,
          YarnConfiguration.DEFAULT_NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE));

    // Create an iterator before checkDirs is called to reliable test case
    List<String> list = dc.getGoodDirs();
    ListIterator<String> li = list.listIterator();

    // DiskErrorException will invalidate iterator of non-concurrent
    // collections. ConcurrentModificationException will be thrown upon next
    // use of the iterator.
    Assert.assertTrue("checkDirs did not remove test file from directory list",
        dc.checkDirs());

    // Verify no ConcurrentModification is thrown
    li.next();
  }

  @Test
  public void testCreateDirectories() throws IOException {
    
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");

    String dirA = new File(testDir, "dirA").getPath();
    String dirB = new File(dirA, "dirB").getPath();
    String dirC = new File(testDir, "dirC").getPath();
    Path pathC = new Path(dirC);
    FsPermission permDirC = new FsPermission((short)0710);

    localFs.mkdir(pathC, null, true);
    localFs.setPermission(pathC, permDirC);

    String[] dirs = { dirA, dirB, dirC };
    DirectoryCollection dc =
        new DirectoryCollection(dirs, conf.getFloat(
          YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE,
          YarnConfiguration.DEFAULT_NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE));
    FsPermission defaultPerm = FsPermission.getDefault()
        .applyUMask(new FsPermission((short)FsPermission.DEFAULT_UMASK));
    boolean createResult = dc.createNonExistentDirs(localFs, defaultPerm);
    Assert.assertTrue(createResult);

    FileStatus status = localFs.getFileStatus(new Path(dirA));
    Assert.assertEquals("local dir parent not created with proper permissions",
        defaultPerm, status.getPermission());
    status = localFs.getFileStatus(new Path(dirB));
    Assert.assertEquals("local dir not created with proper permissions",
        defaultPerm, status.getPermission());
    status = localFs.getFileStatus(pathC);
    Assert.assertEquals("existing local directory permissions modified",
        permDirC, status.getPermission());
  }
  
  @Test
  public void testDiskSpaceUtilizationLimit() throws IOException {

    String dirA = new File(testDir, "dirA").getPath();
    String[] dirs = { dirA };
    DirectoryCollection dc = new DirectoryCollection(dirs, 0.0F);
    dc.checkDirs();
    Assert.assertEquals(0, dc.getGoodDirs().size());
    Assert.assertEquals(0, dc.getErroredDirs().size());
    Assert.assertEquals(1, dc.getFailedDirs().size());
    Assert.assertEquals(1, dc.getFullDirs().size());
    Assert.assertNotNull(dc.getDirectoryErrorInfo(dirA));
    Assert.assertEquals(DirectoryCollection.DiskErrorCause.DISK_FULL, dc.getDirectoryErrorInfo(dirA).cause);

    // no good dirs
    Assert.assertEquals(0, dc.getGoodDirsDiskUtilizationPercentage());

    dc = new DirectoryCollection(dirs, 100.0F);
    int utilizedSpacePerc =
        (int) ((testDir.getTotalSpace() - testDir.getUsableSpace()) * 100 /
            testDir.getTotalSpace());
    dc.checkDirs();
    Assert.assertEquals(1, dc.getGoodDirs().size());
    Assert.assertEquals(0, dc.getErroredDirs().size());
    Assert.assertEquals(0, dc.getFailedDirs().size());
    Assert.assertEquals(0, dc.getFullDirs().size());
    Assert.assertNull(dc.getDirectoryErrorInfo(dirA));

    Assert.assertEquals(utilizedSpacePerc,
      dc.getGoodDirsDiskUtilizationPercentage());

    dc = new DirectoryCollection(dirs, testDir.getTotalSpace() / (1024 * 1024));
    dc.checkDirs();
    Assert.assertEquals(0, dc.getGoodDirs().size());
    Assert.assertEquals(0, dc.getErroredDirs().size());
    Assert.assertEquals(1, dc.getFailedDirs().size());
    Assert.assertEquals(1, dc.getFullDirs().size());
    Assert.assertNotNull(dc.getDirectoryErrorInfo(dirA));
    // no good dirs
    Assert.assertEquals(0, dc.getGoodDirsDiskUtilizationPercentage());

    dc = new DirectoryCollection(dirs, 100.0F, 100.0F, 0);
    utilizedSpacePerc =
        (int)((testDir.getTotalSpace() - testDir.getUsableSpace()) * 100 /
            testDir.getTotalSpace());
    dc.checkDirs();
    Assert.assertEquals(1, dc.getGoodDirs().size());
    Assert.assertEquals(0, dc.getErroredDirs().size());
    Assert.assertEquals(0, dc.getFailedDirs().size());
    Assert.assertEquals(0, dc.getFullDirs().size());
    Assert.assertNull(dc.getDirectoryErrorInfo(dirA));

    Assert.assertEquals(utilizedSpacePerc,
      dc.getGoodDirsDiskUtilizationPercentage());
  }

  @Test
  public void testDiskLimitsCutoffSetters() throws IOException {

    String[] dirs = { "dir" };
    DirectoryCollection dc = new DirectoryCollection(dirs, 0.0F, 0.0F, 100);
    float testValue = 57.5F;
    float delta = 0.1F;
    dc.setDiskUtilizationPercentageCutoff(testValue, 50.0F);
    Assert.assertEquals(testValue, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    Assert.assertEquals(50.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);

    testValue = -57.5F;
    dc.setDiskUtilizationPercentageCutoff(testValue, testValue);
    Assert.assertEquals(0.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    Assert.assertEquals(0.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);

    testValue = 157.5F;
    dc.setDiskUtilizationPercentageCutoff(testValue, testValue);
    Assert.assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    Assert.assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);

    long spaceValue = 57;
    dc.setDiskUtilizationSpaceCutoff(spaceValue);
    Assert.assertEquals(spaceValue, dc.getDiskUtilizationSpaceCutoff());
    spaceValue = -57;
    dc.setDiskUtilizationSpaceCutoff(spaceValue);
    Assert.assertEquals(0, dc.getDiskUtilizationSpaceCutoff());
  }

  @Test
  public void testFailedDisksBecomingGoodAgain() throws Exception {

    String dirA = new File(testDir, "dirA").getPath();
    String[] dirs = { dirA };
    DirectoryCollection dc = new DirectoryCollection(dirs, 0.0F);
    dc.checkDirs();
    Assert.assertEquals(0, dc.getGoodDirs().size());
    Assert.assertEquals(1, dc.getFailedDirs().size());
    Assert.assertEquals(1, dc.getFullDirs().size());
    Assert.assertEquals(0, dc.getErroredDirs().size());
    Assert.assertNotNull(dc.getDirectoryErrorInfo(dirA));
    Assert.assertEquals(DirectoryCollection.DiskErrorCause.DISK_FULL, dc.getDirectoryErrorInfo(dirA).cause);

    dc.setDiskUtilizationPercentageCutoff(100.0F, 100.0F);
    dc.checkDirs();
    Assert.assertEquals(1, dc.getGoodDirs().size());
    Assert.assertEquals(0, dc.getFailedDirs().size());
    Assert.assertEquals(0, dc.getFullDirs().size());
    Assert.assertEquals(0, dc.getErroredDirs().size());
    Assert.assertNull(dc.getDirectoryErrorInfo(dirA));

    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");

    String dirB = new File(testDir, "dirB").getPath();
    Path pathB = new Path(dirB);
    FsPermission permDirB = new FsPermission((short) 0400);

    localFs.mkdir(pathB, null, true);
    localFs.setPermission(pathB, permDirB);

    String[] dirs2 = { dirB };

    dc = new DirectoryCollection(dirs2, 100.0F);
    dc.checkDirs();
    Assert.assertEquals(0, dc.getGoodDirs().size());
    Assert.assertEquals(1, dc.getFailedDirs().size());
    Assert.assertEquals(0, dc.getFullDirs().size());
    Assert.assertEquals(1, dc.getErroredDirs().size());
    Assert.assertNotNull(dc.getDirectoryErrorInfo(dirB));
    Assert.assertEquals(DirectoryCollection.DiskErrorCause.OTHER, dc.getDirectoryErrorInfo(dirB).cause);

    permDirB = new FsPermission((short) 0700);
    localFs.setPermission(pathB, permDirB);
    dc.checkDirs();
    Assert.assertEquals(1, dc.getGoodDirs().size());
    Assert.assertEquals(0, dc.getFailedDirs().size());
    Assert.assertEquals(0, dc.getFullDirs().size());
    Assert.assertEquals(0, dc.getErroredDirs().size());
    Assert.assertNull(dc.getDirectoryErrorInfo(dirA));
  }

  @Test
  public void testConstructors() {

    String[] dirs = { "dir" };
    float delta = 0.1F;
    DirectoryCollection dc = new DirectoryCollection(dirs);
    Assert.assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    Assert.assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    Assert.assertEquals(0, dc.getDiskUtilizationSpaceCutoff());

    dc = new DirectoryCollection(dirs, 57.5F);
    Assert.assertEquals(57.5F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    Assert.assertEquals(57.5F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    Assert.assertEquals(0, dc.getDiskUtilizationSpaceCutoff());

    dc = new DirectoryCollection(dirs, 57);
    Assert.assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    Assert.assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    Assert.assertEquals(57, dc.getDiskUtilizationSpaceCutoff());

    dc = new DirectoryCollection(dirs, 57.5F, 50.5F, 67);
    Assert.assertEquals(57.5F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    Assert.assertEquals(50.5F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    Assert.assertEquals(67, dc.getDiskUtilizationSpaceCutoff());

    dc = new DirectoryCollection(dirs, -57.5F, -57.5F, -67);
    Assert.assertEquals(0.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    Assert.assertEquals(0.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    Assert.assertEquals(0, dc.getDiskUtilizationSpaceCutoff());

    dc = new DirectoryCollection(dirs, 157.5F, 157.5F, -67);
    Assert.assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    Assert.assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    Assert.assertEquals(0, dc.getDiskUtilizationSpaceCutoff());
  }

  @Test
  public void testDirsChangeListener() {
    DirsChangeListenerTest listener1 = new DirsChangeListenerTest();
    DirsChangeListenerTest listener2 = new DirsChangeListenerTest();
    DirsChangeListenerTest listener3 = new DirsChangeListenerTest();

    String dirA = new File(testDir, "dirA").getPath();
    String[] dirs = { dirA };
    DirectoryCollection dc = new DirectoryCollection(dirs, 0.0F);
    Assert.assertEquals(1, dc.getGoodDirs().size());
    Assert.assertEquals(listener1.num, 0);
    Assert.assertEquals(listener2.num, 0);
    Assert.assertEquals(listener3.num, 0);
    dc.registerDirsChangeListener(listener1);
    dc.registerDirsChangeListener(listener2);
    dc.registerDirsChangeListener(listener3);
    Assert.assertEquals(listener1.num, 1);
    Assert.assertEquals(listener2.num, 1);
    Assert.assertEquals(listener3.num, 1);

    dc.deregisterDirsChangeListener(listener3);
    dc.checkDirs();
    Assert.assertEquals(0, dc.getGoodDirs().size());
    Assert.assertEquals(listener1.num, 2);
    Assert.assertEquals(listener2.num, 2);
    Assert.assertEquals(listener3.num, 1);

    dc.deregisterDirsChangeListener(listener2);
    dc.setDiskUtilizationPercentageCutoff(100.0F, 100.0F);
    dc.checkDirs();
    Assert.assertEquals(1, dc.getGoodDirs().size());
    Assert.assertEquals(listener1.num, 3);
    Assert.assertEquals(listener2.num, 2);
    Assert.assertEquals(listener3.num, 1);
  }

  static class DirsChangeListenerTest implements DirsChangeListener {
    public int num = 0;
    public DirsChangeListenerTest() {
    }
    @Override
    public void onDirsChanged() {
      num++;
    }
  }
}
