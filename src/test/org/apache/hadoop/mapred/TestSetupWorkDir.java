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

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Validates if TaskRunner.deleteDirContents() is properly cleaning up the
 * contents of workDir.
 */
public class TestSetupWorkDir extends TestCase {
  private static int NUM_SUB_DIRS = 3;

  /**
   * Creates subdirectories under given dir and files under those subdirs.
   * Creates dir/subDir1, dir/subDir1/file, dir/subDir2, dir/subDir2/file, etc.
   */
  static void createSubDirs(JobConf jobConf, Path dir)
       throws IOException {
    for (int i = 1; i <= NUM_SUB_DIRS; i++) {
      Path subDir = new Path(dir, "subDir" + i);
      FileSystem fs = FileSystem.getLocal(jobConf);
      fs.mkdirs(subDir);
      Path p = new Path(subDir, "file");
      DataOutputStream out = fs.create(p);
      out.writeBytes("dummy input");
      out.close();
    }
  }

  /**
   * Validates if TaskRunner.deleteDirContents() is properly cleaning up the
   * contents of workDir.
   */
  public void testSetupWorkDir() throws IOException {
    Path rootDir = new Path(System.getProperty("test.build.data",  "/tmp"),
                            "testSetupWorkDir");
    Path myWorkDir = new Path(rootDir, "./work");
    JobConf jConf = new JobConf();
    FileSystem fs = FileSystem.getLocal(jConf);
    if (fs.exists(myWorkDir)) {
      fs.delete(myWorkDir, true);
    }
    if (!fs.mkdirs(myWorkDir)) {
      throw new IOException("Unable to create workDir " + myWorkDir);
    }

    // create subDirs under work dir
    createSubDirs(jConf, myWorkDir);

    assertTrue("createDirAndSubDirs() did not create subdirs under "
        + myWorkDir, fs.listStatus(myWorkDir).length == NUM_SUB_DIRS);
    
    TaskRunner.deleteDirContents(jConf, new File(myWorkDir.toUri().getPath()));
    
    assertTrue("Contents of " + myWorkDir + " are not cleaned up properly.",
        fs.listStatus(myWorkDir).length == 0);
    
    // cleanup
    fs.delete(rootDir, true);
  }
}
