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
package org.apache.hadoop.fs.viewfs;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TestTrash;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;
import static org.apache.hadoop.fs.viewfs.Constants.*;
import static org.junit.Assert.*;

public class TestViewFsTrash {
  FileSystem fsTarget;  // the target file system - the mount will point here
  FileSystem fsView;
  Configuration conf;
  private FileSystemTestHelper fileSystemTestHelper;

  @Before
  public void setUp() throws Exception {
    Configuration targetFSConf = new Configuration();
    targetFSConf.setClass("fs.file.impl", TestTrash.TestLFS.class, FileSystem.class);

    fsTarget = FileSystem.getLocal(targetFSConf);
    fileSystemTestHelper = new FileSystemTestHelper(fsTarget.getHomeDirectory().toUri().getPath());

    conf = ViewFileSystemTestSetup.createConfig();
    fsView = ViewFileSystemTestSetup.setupForViewFileSystem(conf, fileSystemTestHelper, fsTarget);
    conf.set("fs.defaultFS", FsConstants.VIEWFS_URI.toString());

    /*
     * Need to set the fs.file.impl to TestViewFsTrash.TestLFS. Otherwise, it will load
     * LocalFileSystem implementation which uses System.getProperty("user.home") for homeDirectory.
     */
    conf.setClass("fs.file.impl", TestTrash.TestLFS.class, FileSystem.class);

  }
 
  @After
  public void tearDown() throws Exception {
    ViewFileSystemTestSetup.tearDown(fileSystemTestHelper, fsTarget);
    fsTarget.delete(new Path(fsTarget.getHomeDirectory(), ".Trash/Current"),
        true);
  }
  
  @Test
  public void testTrash() throws Exception {
    TestTrash.trashShell(conf, fileSystemTestHelper.getTestRootPath(fsView),
        fsView, new Path(fileSystemTestHelper.getTestRootPath(fsView), ".Trash/Current"));
  }

  @Test
  public void testLocalizedTrashInMoveToAppropriateTrash() throws IOException {
    Configuration conf2 = new Configuration(conf);
    Path testFile = new Path("/data/testfile.txt");

    // Enable moveToTrash and add a mount point for /data
    conf2.setLong(FS_TRASH_INTERVAL_KEY, 1);
    ConfigUtil.addLink(conf2, "/data", new Path(fileSystemTestHelper.getAbsoluteTestRootPath(fsTarget), "data").toUri());

    // Default case. file should be moved to fsTarget.getTrashRoot()/resolvedPath
    conf2.setBoolean(CONFIG_VIEWFS_TRASH_FORCE_INSIDE_MOUNT_POINT, false);
    try (FileSystem fsView2 = FileSystem.get(conf2)) {
      FileSystemTestHelper.createFile(fsView2, testFile);
      Path resolvedFile = fsView2.resolvePath(testFile);

      Trash.moveToAppropriateTrash(fsView2, testFile, conf2);
      Trash trash = new Trash(fsTarget, conf2);
      Path movedPath = Path.mergePaths(trash.getCurrentTrashDir(testFile), resolvedFile);
      ContractTestUtils.assertPathExists(fsTarget, "File not in trash", movedPath);
    }

    // Turn on localized trash. File should be moved to viewfs:/data/.Trash/{user}/Current.
    conf2.setBoolean(CONFIG_VIEWFS_TRASH_FORCE_INSIDE_MOUNT_POINT, true);
    try (FileSystem fsView2 = FileSystem.get(conf2)) {
      FileSystemTestHelper.createFile(fsView2, testFile);

      Trash.moveToAppropriateTrash(fsView2, testFile, conf2);
      Trash trash = new Trash(fsView2, conf2);
      Path movedPath = Path.mergePaths(trash.getCurrentTrashDir(testFile), testFile);
      ContractTestUtils.assertPathExists(fsView2, "File not in localized trash", movedPath);
    }
  }
}
