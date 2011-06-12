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
package org.apache.hadoop.mapred;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.mapreduce.MRConfig;
import org.junit.Test;
import org.junit.Before;

/**
 * Tests for the correct behavior of the TaskTracker starting up with
 * respect to its local-disk directories.
 */
public class TestTaskTrackerDirectories {
  private final String TEST_DIR = new File("build/test/testmapredlocaldir")
    .getAbsolutePath();
  
  @Before
  public void deleteTestDir() throws IOException {
    FileUtil.fullyDelete(new File(TEST_DIR));
    assertFalse("Could not delete " + TEST_DIR,
        new File(TEST_DIR).exists());
  }
  
  @Test
  public void testCreatesLocalDirs() throws Exception {
    Configuration conf = new Configuration();
    String[] dirs = new String[] {
        TEST_DIR + "/local1",
        TEST_DIR + "/local2"
    };
    
    conf.setStrings(MRConfig.LOCAL_DIR, dirs);
    setupTaskController(conf);

    for (String dir : dirs) {
      checkDir(dir);
    }
  }
  
  @Test
  public void testFixesLocalDirPermissions() throws Exception {
    Configuration conf = new Configuration();
    String[] dirs = new String[] {
        TEST_DIR + "/badperms"
    };
    
    new File(dirs[0]).mkdirs();
    FileUtil.chmod(dirs[0], "000");

    conf.setStrings(MRConfig.LOCAL_DIR, dirs);
    setupTaskController(conf);
    
    for (String dir : dirs) {
      checkDir(dir);
    }
  }
  
  @Test
  public void testCreatesLogDir() throws Exception {
    File dir = TaskLog.getUserLogDir();
    FileUtil.fullyDelete(dir);
    
    setupTaskController(new Configuration());
    
    checkDir(dir.getAbsolutePath());
  }
  
  /**
   * If the log dir can't be created, the TT should fail to start since
   * it will be unable to localize or run tasks.
   */
  @Test
  public void testCantCreateLogDir() throws Exception {
    File dir = TaskLog.getUserLogDir();
    FileUtil.fullyDelete(dir);
    assertTrue("Making file in place of log dir",
        dir.createNewFile());

    try {
      setupTaskController(new Configuration());
      fail("Didn't throw!");
    } catch (IOException ioe) {
      System.err.println("Got expected exception");
      ioe.printStackTrace(System.out);
    }
  }
  
  @Test
  public void testFixesLogDirPermissions() throws Exception {
    File dir = TaskLog.getUserLogDir();
    FileUtil.fullyDelete(dir);
    dir.mkdirs();
    FileUtil.chmod(dir.getAbsolutePath(), "000");
    
    setupTaskController(new Configuration());
    
    checkDir(dir.getAbsolutePath());
  }
  
  private void setupTaskController(Configuration conf) throws IOException {
    TaskController tc = new DefaultTaskController();
    tc.setConf(conf);
    tc.setup();
  }

  private void checkDir(String dir) throws IOException {
    FileSystem fs = RawLocalFileSystem.get(new Configuration());
    File f = new File(dir);
    assertTrue(dir + "should exist", f.exists());
    FileStatus stat = fs.getFileStatus(new Path(dir));
    assertEquals(dir + " has correct permissions",
        0755, stat.getPermission().toShort());
  }
}
