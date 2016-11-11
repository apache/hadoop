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

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.util.Shell;
import org.eclipse.jetty.util.log.Log;


/**
 * This class is for  setup and teardown for viewFileSystem so that
 * it can be tested via the standard FileSystem tests.
 * 
 * If tests launched via ant (build.xml) the test root is absolute path
 * If tests launched via eclipse, the test root is 
 * is a test dir below the working directory. (see FileContextTestHelper)
 * 
 * We set a viewFileSystems with 3 mount points: 
 * 1) /<firstComponent>" of testdir  pointing to same in  target fs
 * 2)   /<firstComponent>" of home  pointing to same in  target fs 
 * 3)  /<firstComponent>" of wd  pointing to same in  target fs
 * (note in many cases the link may be the same - viewFileSytem handles this)
 * 
 * We also set the view file system's wd to point to the wd. 
 */
public class ViewFileSystemTestSetup {
  
  static public String ViewFSTestDir = "/testDir";

  /**
   * 
   * @param fsTarget - the target fs of the view fs.
   * @return return the ViewFS File context to be used for tests
   * @throws Exception
   */
  static public FileSystem setupForViewFileSystem(Configuration conf, FileSystemTestHelper fileSystemTestHelper, FileSystem fsTarget) throws Exception {
    /**
     * create the test root on local_fs - the  mount table will point here
     */
    Path targetOfTests = fileSystemTestHelper.getTestRootPath(fsTarget);
    // In case previous test was killed before cleanup
    fsTarget.delete(targetOfTests, true);
    fsTarget.mkdirs(targetOfTests);


    // Set up viewfs link for test dir as described above
    String testDir = fileSystemTestHelper.getTestRootPath(fsTarget).toUri()
        .getPath();
    linkUpFirstComponents(conf, testDir, fsTarget, "test dir");
    
    
    // Set up viewfs link for home dir as described above
    setUpHomeDir(conf, fsTarget);
    
    
    // the test path may be relative to working dir - we need to make that work:
    // Set up viewfs link for wd as described above
    String wdDir = fsTarget.getWorkingDirectory().toUri().getPath();
    linkUpFirstComponents(conf, wdDir, fsTarget, "working dir");


    FileSystem fsView = FileSystem.get(FsConstants.VIEWFS_URI, conf);
    fsView.setWorkingDirectory(new Path(wdDir)); // in case testdir relative to wd.
    Log.getLog().info("Working dir is: " + fsView.getWorkingDirectory());
    return fsView;
  }

  /**
   * 
   * delete the test directory in the target  fs
   */
  static public void tearDown(FileSystemTestHelper fileSystemTestHelper, FileSystem fsTarget) throws Exception {
    Path targetOfTests = fileSystemTestHelper.getTestRootPath(fsTarget);
    fsTarget.delete(targetOfTests, true);
  }
  
  public static Configuration createConfig() {
    return createConfig(true);
  }
  
  public static Configuration createConfig(boolean disableCache) {
    Configuration conf = new Configuration();
    conf.set("fs.viewfs.impl", ViewFileSystem.class.getName());
    if (disableCache) {
      conf.set("fs.viewfs.impl.disable.cache", "true");
    }
    return conf; 
  }
  
  static void setUpHomeDir(Configuration conf, FileSystem fsTarget) {
    String homeDir = fsTarget.getHomeDirectory().toUri().getPath();
    int indexOf2ndSlash = homeDir.indexOf('/', 1);
    if (indexOf2ndSlash >0) {
      linkUpFirstComponents(conf, homeDir, fsTarget, "home dir");
    } else { // home dir is at root. Just link the home dir itse
      URI linkTarget = fsTarget.makeQualified(new Path(homeDir)).toUri();
      ConfigUtil.addLink(conf, homeDir, linkTarget);
      Log.getLog().info("Added link for home dir " + homeDir + "->" + linkTarget);
    }
    // Now set the root of the home dir for viewfs
    String homeDirRoot = fsTarget.getHomeDirectory().getParent().toUri().getPath();
    ConfigUtil.setHomeDirConf(conf, homeDirRoot);
    Log.getLog().info("Home dir base for viewfs" + homeDirRoot);
  }
  
  /*
   * Set up link in config for first component of path to the same
   * in the target file system.
   */
  static void linkUpFirstComponents(Configuration conf, String path, FileSystem fsTarget, String info) {
    int indexOfEnd = path.indexOf('/', 1);
    if (Shell.WINDOWS) {
      indexOfEnd = path.indexOf('/', indexOfEnd + 1);
    }
    String firstComponent = path.substring(0, indexOfEnd);
    URI linkTarget = fsTarget.makeQualified(new Path(firstComponent)).toUri();
    ConfigUtil.addLink(conf, firstComponent, linkTarget);
    Log.getLog().info("Added link for " + info + " "
        + firstComponent + "->" + linkTarget);    
  }
}
