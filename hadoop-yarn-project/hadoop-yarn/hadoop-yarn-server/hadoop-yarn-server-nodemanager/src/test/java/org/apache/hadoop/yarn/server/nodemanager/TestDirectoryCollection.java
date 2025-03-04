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
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.DirectoryCollection.DirsChangeListener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDirectoryCollection {

  private File testDir;
  private File testFile;

  private Configuration conf;
  private FileContext localFs;

  @BeforeEach
  public void setupForTests() throws IOException {
    conf = new Configuration();
    localFs = FileContext.getLocalFSFileContext(conf);
    testDir = Files.createTempDirectory(TestDirectoryCollection.class.getName()).toFile();
    testFile = new File(testDir, "testfile");
    testFile.createNewFile();
  }

  @AfterEach
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
    assertTrue(dc.checkDirs(),
        "checkDirs did not remove test file from directory list");

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
    assertTrue(createResult);

    FileStatus status = localFs.getFileStatus(new Path(dirA));
    assertEquals(defaultPerm, status.getPermission(),
        "local dir parent not created with proper permissions");
    status = localFs.getFileStatus(new Path(dirB));
    assertEquals(defaultPerm, status.getPermission(),
        "local dir not created with proper permissions");
    status = localFs.getFileStatus(pathC);
    assertEquals(permDirC, status.getPermission(),
        "existing local directory permissions modified");
  }
  
  @Test
  public void testDiskSpaceUtilizationLimit() throws IOException {

    String dirA = new File(testDir, "dirA").getPath();
    String[] dirs = { dirA };
    DirectoryCollection dc = new DirectoryCollection(dirs, 0.0F);
    dc.checkDirs();
    assertEquals(0, dc.getGoodDirs().size());
    assertEquals(0, dc.getErroredDirs().size());
    assertEquals(1, dc.getFailedDirs().size());
    assertEquals(1, dc.getFullDirs().size());
    assertNotNull(dc.getDirectoryErrorInfo(dirA));
    assertEquals(DirectoryCollection.DiskErrorCause.DISK_FULL,
        dc.getDirectoryErrorInfo(dirA).cause);

    // no good dirs
    assertEquals(0, dc.getGoodDirsDiskUtilizationPercentage());

    dc = new DirectoryCollection(dirs, 100.0F);
    int utilizedSpacePerc =
        (int) ((testDir.getTotalSpace() - testDir.getUsableSpace()) * 100 /
            testDir.getTotalSpace());
    dc.checkDirs();
    assertEquals(1, dc.getGoodDirs().size());
    assertEquals(0, dc.getErroredDirs().size());
    assertEquals(0, dc.getFailedDirs().size());
    assertEquals(0, dc.getFullDirs().size());
    assertNull(dc.getDirectoryErrorInfo(dirA));

    assertEquals(utilizedSpacePerc,
      dc.getGoodDirsDiskUtilizationPercentage());

    dc = new DirectoryCollection(dirs, testDir.getTotalSpace() / (1024 * 1024));
    dc.checkDirs();
    assertEquals(0, dc.getGoodDirs().size());
    assertEquals(0, dc.getErroredDirs().size());
    assertEquals(1, dc.getFailedDirs().size());
    assertEquals(1, dc.getFullDirs().size());
    assertNotNull(dc.getDirectoryErrorInfo(dirA));
    // no good dirs
    assertEquals(0, dc.getGoodDirsDiskUtilizationPercentage());

    dc = new DirectoryCollection(dirs, 100.0F, 100.0F, 0);
    utilizedSpacePerc =
        (int)((testDir.getTotalSpace() - testDir.getUsableSpace()) * 100 /
            testDir.getTotalSpace());
    dc.checkDirs();
    assertEquals(1, dc.getGoodDirs().size());
    assertEquals(0, dc.getErroredDirs().size());
    assertEquals(0, dc.getFailedDirs().size());
    assertEquals(0, dc.getFullDirs().size());
    assertNull(dc.getDirectoryErrorInfo(dirA));

    assertEquals(utilizedSpacePerc,
      dc.getGoodDirsDiskUtilizationPercentage());
  }

  @Test
  public void testDiskSpaceUtilizationThresholdEnabled() throws IOException {

    String dirA = new File(testDir, "dirA").getPath();
    String[] dirs = {dirA};
    DirectoryCollection dc = new DirectoryCollection(dirs, 0.0F);

    // Disable disk utilization threshold.
    dc.setDiskUtilizationThresholdEnabled(false);
    assertFalse(dc.getDiskUtilizationThresholdEnabled());

    dc.checkDirs();
    assertEquals(1, dc.getGoodDirs().size());
    assertEquals(0, dc.getErroredDirs().size());
    assertEquals(0, dc.getFailedDirs().size());
    assertEquals(0, dc.getFullDirs().size());
    assertNull(dc.getDirectoryErrorInfo(dirA));

    // Enable disk utilization threshold.
    dc.setDiskUtilizationThresholdEnabled(true);
    assertTrue(dc.getDiskUtilizationThresholdEnabled());

    dc.checkDirs();
    assertEquals(0, dc.getGoodDirs().size());
    assertEquals(0, dc.getErroredDirs().size());
    assertEquals(1, dc.getFailedDirs().size());
    assertEquals(1, dc.getFullDirs().size());
    assertNotNull(dc.getDirectoryErrorInfo(dirA));
    assertEquals(DirectoryCollection.DiskErrorCause.DISK_FULL,
        dc.getDirectoryErrorInfo(dirA).cause);

    // no good dirs
    assertEquals(0,
        dc.getGoodDirsDiskUtilizationPercentage());

    dc = new DirectoryCollection(dirs, 100.0F);
    int utilizedSpacePerc =
        (int) ((testDir.getTotalSpace() - testDir.getUsableSpace()) * 100 /
            testDir.getTotalSpace());
    dc.checkDirs();
    assertEquals(1, dc.getGoodDirs().size());
    assertEquals(0, dc.getErroredDirs().size());
    assertEquals(0, dc.getFailedDirs().size());
    assertEquals(0, dc.getFullDirs().size());
    assertNull(dc.getDirectoryErrorInfo(dirA));

    assertEquals(utilizedSpacePerc,
        dc.getGoodDirsDiskUtilizationPercentage());

    dc = new DirectoryCollection(dirs,
        testDir.getTotalSpace() / (1024 * 1024));

    // Disable disk utilization threshold.
    dc.setDiskUtilizationThresholdEnabled(false);
    assertFalse(dc.getDiskUtilizationThresholdEnabled());

    // Disable disk free space threshold.
    dc.setDiskFreeSpaceThresholdEnabled(false);
    assertFalse(dc.getDiskFreeSpaceThresholdEnabled());
    dc.checkDirs();

    assertEquals(1, dc.getGoodDirs().size());
    assertEquals(0, dc.getErroredDirs().size());
    assertEquals(0, dc.getFailedDirs().size());
    assertEquals(0, dc.getFullDirs().size());
    assertNull(dc.getDirectoryErrorInfo(dirA));

    dc = new DirectoryCollection(dirs,
        testDir.getTotalSpace() / (1024 * 1024));

    // Enable disk free space threshold.
    dc.setDiskFreeSpaceThresholdEnabled(true);
    assertTrue(dc.getDiskFreeSpaceThresholdEnabled());

    dc.checkDirs();

    assertEquals(0, dc.getGoodDirs().size());
    assertEquals(0, dc.getErroredDirs().size());
    assertEquals(1, dc.getFailedDirs().size());
    assertEquals(1, dc.getFullDirs().size());
    assertNotNull(dc.getDirectoryErrorInfo(dirA));
    // no good dirs
    assertEquals(0, dc.getGoodDirsDiskUtilizationPercentage());

    dc = new DirectoryCollection(dirs, 100.0F, 100.0F, 0);
    utilizedSpacePerc =
        (int)((testDir.getTotalSpace() - testDir.getUsableSpace()) * 100 /
            testDir.getTotalSpace());
    dc.checkDirs();
    assertEquals(1, dc.getGoodDirs().size());
    assertEquals(0, dc.getErroredDirs().size());
    assertEquals(0, dc.getFailedDirs().size());
    assertEquals(0, dc.getFullDirs().size());
    assertNull(dc.getDirectoryErrorInfo(dirA));

    assertEquals(utilizedSpacePerc,
        dc.getGoodDirsDiskUtilizationPercentage());
  }

  @Test
  public void testDiskLimitsCutoffSetters() throws IOException {

    String[] dirs = { "dir" };
    DirectoryCollection dc = new DirectoryCollection(dirs, 0.0F, 0.0F, 100);
    float testValue = 57.5F;
    float delta = 0.1F;
    dc.setDiskUtilizationPercentageCutoff(testValue, 50.0F);
    assertEquals(testValue, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(50.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);

    testValue = -57.5F;
    dc.setDiskUtilizationPercentageCutoff(testValue, testValue);
    assertEquals(0.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(0.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);

    testValue = 157.5F;
    dc.setDiskUtilizationPercentageCutoff(testValue, testValue);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);

    long lowSpaceValue = 57;
    dc.setDiskUtilizationSpaceCutoff(lowSpaceValue);
    assertEquals(lowSpaceValue, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(lowSpaceValue, dc.getDiskUtilizationSpaceCutoffHigh());
    long highSpaceValue = 73;
    dc.setDiskUtilizationSpaceCutoff(lowSpaceValue, highSpaceValue);
    assertEquals(lowSpaceValue, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(highSpaceValue, dc.getDiskUtilizationSpaceCutoffHigh());
    lowSpaceValue = -57;
    dc.setDiskUtilizationSpaceCutoff(lowSpaceValue);
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffHigh());
    dc.setDiskUtilizationSpaceCutoff(lowSpaceValue, highSpaceValue);
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(highSpaceValue, dc.getDiskUtilizationSpaceCutoffHigh());
    highSpaceValue = -10;
    dc.setDiskUtilizationSpaceCutoff(lowSpaceValue, highSpaceValue);
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffHigh());
    lowSpaceValue = 33;
    dc.setDiskUtilizationSpaceCutoff(lowSpaceValue, highSpaceValue);
    assertEquals(lowSpaceValue, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(lowSpaceValue, dc.getDiskUtilizationSpaceCutoffHigh());

  }

  @Test
  public void testFailedDisksBecomingGoodAgain() throws Exception {

    String dirA = new File(testDir, "dirA").getPath();
    String[] dirs = { dirA };
    DirectoryCollection dc = new DirectoryCollection(dirs, 0.0F);
    dc.checkDirs();
    assertEquals(0, dc.getGoodDirs().size());
    assertEquals(1, dc.getFailedDirs().size());
    assertEquals(1, dc.getFullDirs().size());
    assertEquals(0, dc.getErroredDirs().size());
    assertNotNull(dc.getDirectoryErrorInfo(dirA));
    assertEquals(DirectoryCollection.DiskErrorCause.DISK_FULL,
        dc.getDirectoryErrorInfo(dirA).cause);

    dc.setDiskUtilizationPercentageCutoff(100.0F, 100.0F);
    dc.checkDirs();
    assertEquals(1, dc.getGoodDirs().size());
    assertEquals(0, dc.getFailedDirs().size());
    assertEquals(0, dc.getFullDirs().size());
    assertEquals(0, dc.getErroredDirs().size());
    assertNull(dc.getDirectoryErrorInfo(dirA));

    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");

    String dirB = new File(testDir, "dirB").getPath();
    Path pathB = new Path(dirB);
    FsPermission permDirB = new FsPermission((short) 0400);

    localFs.mkdir(pathB, null, true);
    localFs.setPermission(pathB, permDirB);

    String[] dirs2 = { dirB };

    dc = new DirectoryCollection(dirs2, 100.0F);
    dc.checkDirs();
    assertEquals(0, dc.getGoodDirs().size());
    assertEquals(1, dc.getFailedDirs().size());
    assertEquals(0, dc.getFullDirs().size());
    assertEquals(1, dc.getErroredDirs().size());
    assertNotNull(dc.getDirectoryErrorInfo(dirB));
    assertEquals(DirectoryCollection.DiskErrorCause.OTHER, dc.getDirectoryErrorInfo(dirB).cause);

    permDirB = new FsPermission((short) 0700);
    localFs.setPermission(pathB, permDirB);
    dc.checkDirs();
    assertEquals(1, dc.getGoodDirs().size());
    assertEquals(0, dc.getFailedDirs().size());
    assertEquals(0, dc.getFullDirs().size());
    assertEquals(0, dc.getErroredDirs().size());
    assertNull(dc.getDirectoryErrorInfo(dirA));
  }

  @Test
  public void testConstructors() {

    String[] dirs = { "dir" };
    float delta = 0.1F;
    DirectoryCollection dc = new DirectoryCollection(dirs);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffHigh());

    dc = new DirectoryCollection(dirs, 57.5F);
    assertEquals(57.5F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(57.5F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffHigh());

    dc = new DirectoryCollection(dirs, 57);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    assertEquals(57, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(57, dc.getDiskUtilizationSpaceCutoffHigh());

    dc = new DirectoryCollection(dirs, 57, 73);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    assertEquals(57, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(73, dc.getDiskUtilizationSpaceCutoffHigh());

    dc = new DirectoryCollection(dirs, 57, 33);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    assertEquals(57, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(57, dc.getDiskUtilizationSpaceCutoffHigh());

    dc = new DirectoryCollection(dirs, 57, -33);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    assertEquals(57, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(57, dc.getDiskUtilizationSpaceCutoffHigh());

    dc = new DirectoryCollection(dirs, -57, -33);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffHigh());

    dc = new DirectoryCollection(dirs, -57, 33);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(33, dc.getDiskUtilizationSpaceCutoffHigh());

    dc = new DirectoryCollection(dirs, 57.5F, 50.5F, 67);
    assertEquals(57.5F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(50.5F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    assertEquals(67, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(67, dc.getDiskUtilizationSpaceCutoffHigh());

    dc = new DirectoryCollection(dirs, -57.5F, -57.5F, -67);
    assertEquals(0.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(0.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffHigh());

    dc = new DirectoryCollection(dirs, 157.5F, 157.5F, -67);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(0, dc.getDiskUtilizationSpaceCutoffHigh());

    dc = new DirectoryCollection(dirs, 157.5F, 157.5F, 5, 10);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffHigh(),
        delta);
    assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoffLow(),
        delta);
    assertEquals(5, dc.getDiskUtilizationSpaceCutoffLow());
    assertEquals(10, dc.getDiskUtilizationSpaceCutoffHigh());
  }

  @Test
  public void testDirsChangeListener() {
    DirsChangeListenerTest listener1 = new DirsChangeListenerTest();
    DirsChangeListenerTest listener2 = new DirsChangeListenerTest();
    DirsChangeListenerTest listener3 = new DirsChangeListenerTest();

    String dirA = new File(testDir, "dirA").getPath();
    String[] dirs = { dirA };
    DirectoryCollection dc = new DirectoryCollection(dirs, 0.0F);
    assertEquals(1, dc.getGoodDirs().size());
    assertEquals(listener1.num, 0);
    assertEquals(listener2.num, 0);
    assertEquals(listener3.num, 0);
    dc.registerDirsChangeListener(listener1);
    dc.registerDirsChangeListener(listener2);
    dc.registerDirsChangeListener(listener3);
    assertEquals(listener1.num, 1);
    assertEquals(listener2.num, 1);
    assertEquals(listener3.num, 1);

    dc.deregisterDirsChangeListener(listener3);
    dc.checkDirs();
    assertEquals(0, dc.getGoodDirs().size());
    assertEquals(listener1.num, 2);
    assertEquals(listener2.num, 2);
    assertEquals(listener3.num, 1);

    dc.deregisterDirsChangeListener(listener2);
    dc.setDiskUtilizationPercentageCutoff(100.0F, 100.0F);
    dc.checkDirs();
    assertEquals(1, dc.getGoodDirs().size());
    assertEquals(listener1.num, 3);
    assertEquals(listener2.num, 2);
    assertEquals(listener3.num, 1);
  }

  @Test
  public void testNonAccessibleSub() throws IOException {
    Files.setPosixFilePermissions(testDir.toPath(),
        PosixFilePermissions.fromString("rwx------"));
    Files.setPosixFilePermissions(testFile.toPath(),
        PosixFilePermissions.fromString("-w--w--w-"));
    DirectoryCollection dc = new DirectoryCollection(new String[]{testDir.toString()});
    dc.setSubAccessibilityValidationEnabled(true);
    Map<String, DirectoryCollection.DiskErrorInformation> diskErrorInformationMap =
        dc.testDirs(Collections.singletonList(testDir.toString()), Collections.emptySet());
    assertEquals(1, diskErrorInformationMap.size());
    assertTrue(diskErrorInformationMap.values().iterator().next()
        .message.contains(testFile.getName()));
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
