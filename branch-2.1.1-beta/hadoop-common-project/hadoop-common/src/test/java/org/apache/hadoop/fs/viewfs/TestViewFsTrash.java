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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TestTrash;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestViewFsTrash {
  FileSystem fsTarget;  // the target file system - the mount will point here
  FileSystem fsView;
  Configuration conf;
  FileSystemTestHelper fileSystemTestHelper = new FileSystemTestHelper();

  class TestLFS extends LocalFileSystem {
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
  }

  @Before
  public void setUp() throws Exception {
    fsTarget = FileSystem.getLocal(new Configuration());
    fsTarget.mkdirs(new Path(fileSystemTestHelper.
        getTestRootPath(fsTarget), "dir1"));
    conf = ViewFileSystemTestSetup.createConfig();
    fsView = ViewFileSystemTestSetup.setupForViewFileSystem(conf, fileSystemTestHelper, fsTarget);
    conf.set("fs.defaultFS", FsConstants.VIEWFS_URI.toString());
  }
 
  @After
  public void tearDown() throws Exception {
    ViewFileSystemTestSetup.tearDown(fileSystemTestHelper, fsTarget);
    fsTarget.delete(new Path(fsTarget.getHomeDirectory(), ".Trash/Current"),
        true);
  }
  
  @Test
  public void testTrash() throws IOException {
    TestTrash.trashShell(conf, fileSystemTestHelper.getTestRootPath(fsView),
        fsTarget, new Path(fsTarget.getHomeDirectory(), ".Trash/Current"));
  }
  
}
