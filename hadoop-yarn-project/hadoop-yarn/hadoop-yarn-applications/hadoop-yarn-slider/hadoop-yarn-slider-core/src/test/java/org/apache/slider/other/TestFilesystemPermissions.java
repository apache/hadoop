/*
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

package org.apache.slider.other;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.slider.utils.YarnMiniClusterTestBase;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This test class exists to look at permissions of the filesystem, especially
 * that created by Mini YARN clusters. On some windows jenkins machines,
 * YARN actions were failing as the directories had the wrong permissions
 * (i.e. too lax)
 */
public class TestFilesystemPermissions extends YarnMiniClusterTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestFilesystemPermissions.class);

  private List<File> filesToDelete = new ArrayList<>();

  @After
  public void deleteFiles() {
    for (File f : filesToDelete) {
      FileUtil.fullyDelete(f, true);
    }
  }

  //@Test
  public void testJavaFSOperations() throws Throwable {
    assertNativeLibrariesPresent();
    File subdir = testDir();
    subdir.mkdir();
    assertTrue(subdir.isDirectory());
    assertTrue(FileUtil.canRead(subdir));
    assertTrue(FileUtil.canWrite(subdir));
    assertTrue(FileUtil.canExecute(subdir));
  }

  //@Test
  public void testDiskCheckerOperations() throws Throwable {
    assertNativeLibrariesPresent();
    File subdir = testDir();
    subdir.mkdir();
    DiskChecker checker = new DiskChecker();
    checker.checkDir(subdir);
  }

  //@Test
  public void testDiskCheckerMkdir() throws Throwable {
    assertNativeLibrariesPresent();
    File subdir = testDir();
    subdir.mkdirs();
    DiskChecker checker = new DiskChecker();
    checker.checkDir(subdir);
  }

  /**
   * Get a test dir for this method; one that will be deleted on teardown.
   * @return a filename unique to this test method
   */
  File testDir() {
    File parent = new File("target/testfspermissions");
    parent.mkdir();
    File testdir = new File(parent, methodName.getMethodName());
    filesToDelete.add(testdir);
    return testdir;
  }


  //@Test
  public void testPermsMap() throws Throwable {
    File dir = testDir();
    String diruri = dir.toURI().toString();
    FileContext lfs = createLocalFS(dir, getConfiguration());
    getLocalDirsPathPermissionsMap(lfs, diruri);
  }

  //@Test
  public void testInitLocaldir() throws Throwable {
    File dir = testDir();
    String diruri = dir.toURI().toString();
    FileContext lfs = createLocalFS(dir, getConfiguration());
    initializeLocalDir(lfs, diruri);
    List<String> localDirs = getInitializedLocalDirs(lfs, Arrays.asList(
        diruri));
    assertEquals(1, localDirs.size());
  }


  //@Test
  public void testValidateMiniclusterPerms() throws Throwable {
    int numLocal = 1;
    String cluster = createMiniCluster("", getConfiguration(), 1, numLocal, 1,
        false);
    File workDir = getMiniCluster().getTestWorkDir();
    List<File> localdirs = new ArrayList<>();
    for (File file : workDir.listFiles()) {
      if (file.isDirectory() && file.getAbsolutePath().contains("-local")) {
        // local dir
        localdirs.add(file);
      }
    }
    assertEquals(numLocal, localdirs.size());
    FileContext lfs = createLocalFS(workDir, getConfiguration());
    for (File file : localdirs) {
      checkLocalDir(lfs, file.toURI().toString());
    }
  }

  FileContext createLocalFS(File dir, Configuration conf)
      throws UnsupportedFileSystemException {
    return FileContext.getFileContext(dir.toURI(), conf);
  }

  /**
   * Extracted from ResourceLocalizationService.
   * @param lfs
   * @param localDir
   * @return perms map
   * @see ResourceLocalizationService
   */
  private Map<Path, FsPermission> getLocalDirsPathPermissionsMap(
      FileContext lfs,
      String localDir) {
    Map<Path, FsPermission> localDirPathFsPermissionsMap = new HashMap<>();

    FsPermission defaultPermission =
        FsPermission.getDirDefault().applyUMask(lfs.getUMask());
    FsPermission nmPrivatePermission =
        ResourceLocalizationService.NM_PRIVATE_PERM.applyUMask(lfs.getUMask());

    Path userDir = new Path(localDir, ContainerLocalizer.USERCACHE);
    Path fileDir = new Path(localDir, ContainerLocalizer.FILECACHE);
    Path sysDir = new Path(
        localDir,
        ResourceLocalizationService.NM_PRIVATE_DIR);

    localDirPathFsPermissionsMap.put(userDir, defaultPermission);
    localDirPathFsPermissionsMap.put(fileDir, defaultPermission);
    localDirPathFsPermissionsMap.put(sysDir, nmPrivatePermission);
    return localDirPathFsPermissionsMap;
  }

  private boolean checkLocalDir(FileContext lfs, String localDir)
      throws IOException {

    Map<Path, FsPermission> pathPermissionMap =
        getLocalDirsPathPermissionsMap(lfs, localDir);

    for (Map.Entry<Path, FsPermission> entry : pathPermissionMap.entrySet()) {
      FileStatus status;
      status = lfs.getFileStatus(entry.getKey());

      if (!status.getPermission().equals(entry.getValue())) {
        String msg =
            "Permissions incorrectly set for dir " + entry.getKey() +
                ", should be " + entry.getValue() + ", actual value = " +
                status.getPermission();
        throw new YarnRuntimeException(msg);
      }
    }
    return true;
  }


  private void initializeLocalDir(FileContext lfs, String localDir)
      throws IOException {

    Map<Path, FsPermission> pathPermissionMap =
        getLocalDirsPathPermissionsMap(lfs, localDir);
    for (Map.Entry<Path, FsPermission> entry : pathPermissionMap.entrySet()) {
      FileStatus status;
      try {
        status = lfs.getFileStatus(entry.getKey());
      } catch (FileNotFoundException fs) {
        status = null;
      }

      if (status == null) {
        lfs.mkdir(entry.getKey(), entry.getValue(), true);
        status = lfs.getFileStatus(entry.getKey());
      }
      FsPermission perms = status.getPermission();
      if (!perms.equals(entry.getValue())) {
        lfs.setPermission(entry.getKey(), entry.getValue());
      }
    }
  }

  synchronized private List<String> getInitializedLocalDirs(FileContext lfs,
      List<String> dirs) throws IOException {
    List<String> checkFailedDirs = new ArrayList<String>();
    for (String dir : dirs) {
      try {
        checkLocalDir(lfs, dir);
      } catch (YarnRuntimeException e) {
        checkFailedDirs.add(dir);
      }
    }
    for (String dir : checkFailedDirs) {
      LOG.info("Attempting to initialize " + dir);
      initializeLocalDir(lfs, dir);
      checkLocalDir(lfs, dir);
    }
    return dirs;
  }


  private void createDir(FileContext localFs, Path dir, FsPermission perm)
  throws IOException {
    if (dir == null) {
      return;
    }
    try {
      localFs.getFileStatus(dir);
    } catch (FileNotFoundException e) {
      createDir(localFs, dir.getParent(), perm);
      localFs.mkdir(dir, perm, false);
      if (!perm.equals(perm.applyUMask(localFs.getUMask()))) {
        localFs.setPermission(dir, perm);
      }
    }
  }
}
