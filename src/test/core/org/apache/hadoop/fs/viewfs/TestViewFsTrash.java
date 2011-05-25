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
  Path targetTestRoot;
  Configuration conf;

  static class TestLFS extends LocalFileSystem {
    Path home;
    TestLFS() throws IOException {
      this(new Path(FileSystemTestHelper.TEST_ROOT_DIR));
    }
    TestLFS(Path home) throws IOException {
      super();
      this.home = home;
    }
    public Path getHomeDirectory() {
      return home;
    }
  }

  @Before
  public void setUp() throws Exception {
    fsTarget = FileSystem.getLocal(new Configuration());
    targetTestRoot = FileSystemTestHelper.getAbsoluteTestRootPath(fsTarget);
    // In case previous test was killed before cleanup
    fsTarget.delete(targetTestRoot, true);
    
    fsTarget.mkdirs(targetTestRoot);
    fsTarget.mkdirs(new Path(targetTestRoot,"dir1"));
    
    
    // Now we use the mount fs to set links to user and dir
    // in the test root
    
    // Set up the defaultMT in the config with our mount point links


    conf = ViewFileSystemTestSetup.configWithViewfsScheme();
    
    // create a link for home directory so that trash path works
    // set up viewfs's home dir root to point to home dir root on target
    // But home dir is differnt on linux, mac etc.
    // Figure it out by calling home dir on target
    
   String homeDir = fsTarget.getHomeDirectory().toUri().getPath();
   int indexOf2ndSlash = homeDir.indexOf('/', 1);
   String homeDirRoot = homeDir.substring(0, indexOf2ndSlash);
   ConfigUtil.addLink(conf, homeDirRoot,
       fsTarget.makeQualified(new Path(homeDirRoot)).toUri()); 
    
    fsView = ViewFileSystemTestSetup.setupForViewFs(conf, fsTarget);
    
    // set working dir so that relative paths
    //fsView.setWorkingDirectory(new Path(fsTarget.getWorkingDirectory().toUri().getPath()));
    conf.set("fs.defaultFS", FsConstants.VIEWFS_URI.toString());
  }
 

  @After
  public void tearDown() throws Exception {
  }

  
  @Test
  public void testTrash() throws IOException {
    TestTrash.trashShell(conf, FileSystemTestHelper.getTestRootPath(fsView),
        fsTarget, new Path(fsTarget.getHomeDirectory(), ".Trash/Current"));
  }
  
}
