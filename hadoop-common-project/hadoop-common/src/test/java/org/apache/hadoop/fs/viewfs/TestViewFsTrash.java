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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TestTrash;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
  
}
