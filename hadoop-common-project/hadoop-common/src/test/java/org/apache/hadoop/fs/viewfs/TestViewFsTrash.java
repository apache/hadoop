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


import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TestTrash;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;


public class TestViewFsTrash {
  private static final Logger LOG = LoggerFactory.getLogger(TestViewFsTrash.class);

  FileSystem fsTarget;  // the target file system - the mount will point here
  FileSystem fsView;
  Configuration conf;
  static FileSystemTestHelper fileSystemTestHelper = new FileSystemTestHelper();

  static class TestLFS extends LocalFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(TestLFS.class);
    Path home;
    TestLFS() throws IOException {
      this(new Path(fileSystemTestHelper.getTestRootDir()));
    }
    TestLFS(Path home) throws IOException {
      super();
      this.home = home;
    }
    @Override
    public Path getHomeDirectory() {
      return home;
    }

    /*
     * We need to override getTrashRoot and getTrashRoots.
     * Otherwise, RawLocalFileSystem.getTrashRoot() will be called, which in turn calls
     * FileSystem.getTrashRoot(), which will call RawLocalFileSystem.getHomeDirectory(),
     * which returns the value from System.property("user.home").
     */
    @Override
    public Path getTrashRoot(Path path) {
      return this.makeQualified(new Path(getHomeDirectory().toUri().getPath(), TRASH_PREFIX));
    }

    @Override
    public Collection<FileStatus> getTrashRoots(boolean allUsers) {
      Path userHome = new Path(getHomeDirectory().toUri().getPath());
      List<FileStatus> ret = new ArrayList<>();
      try {
        if (!allUsers) {
          Path userTrash = new Path(userHome, TRASH_PREFIX);
          if (exists(userTrash)) {
            ret.add(getFileStatus(userTrash));
          }
        } else {
          Path homeParent = userHome.getParent();
          if (exists(homeParent)) {
            FileStatus[] candidates = listStatus(homeParent);
            for (FileStatus candidate : candidates) {
              Path userTrash = new Path(candidate.getPath(), TRASH_PREFIX);
              if (exists(userTrash)) {
                candidate.setPath(userTrash);
                ret.add(candidate);
              }
            }
          }
        }
      } catch (IOException e) {
        LOG.warn("Cannot get all trash roots", e);
      }
      return ret;
    }
  }

  @Before
  public void setUp() throws Exception {
    Configuration targetFSConf = new Configuration();
    // Trash with 12 second deletes and 6 seconds checkpoints
    targetFSConf.set(FS_TRASH_INTERVAL_KEY, "0.2"); // 12 seconds
    targetFSConf.set(FS_TRASH_CHECKPOINT_INTERVAL_KEY, "0.1"); // 6 seconds
    targetFSConf.setClass("fs.file.impl", TestViewFsTrash.TestLFS.class, FileSystem.class);

    fsTarget = FileSystem.getLocal(targetFSConf);

    conf = ViewFileSystemTestSetup.createConfig();
    fsView = ViewFileSystemTestSetup.setupForViewFileSystem(conf, fileSystemTestHelper, fsTarget);
    conf.set("fs.defaultFS", FsConstants.VIEWFS_URI.toString());

    /*
     * Need to set the fs.file.impl to TestViewFsTrash.TestLFS. Otherwise, it will load
     * LocalFileSystem implementation which uses System.getProperty("user.home") for homeDirectory.
     */
    conf.setClass("fs.file.impl", TestViewFsTrash.TestLFS.class, FileSystem.class);

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
  
}
