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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class TestSetupWorkDir extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestSetupWorkDir.class);

  /**
   * Create a file in the given dir and set permissions r_xr_xr_x sothat no one
   * can delete it directly(without doing chmod).
   * Creates dir/subDir and dir/subDir/file
   */
  static void createFileAndSetPermissions(JobConf jobConf, Path dir)
       throws IOException {
    Path subDir = new Path(dir, "subDir");
    FileSystem fs = FileSystem.getLocal(jobConf);
    fs.mkdirs(subDir);
    Path p = new Path(subDir, "file");
    DataOutputStream out = fs.create(p);
    out.writeBytes("dummy input");
    out.close();
    // no write permission for subDir and subDir/file
    try {
      int ret = 0;
      if((ret = FileUtil.chmod(subDir.toUri().getPath(), "a=rx", true)) != 0) {
        LOG.warn("chmod failed for " + subDir + ";retVal=" + ret);
      }
    } catch(InterruptedException e) {
      LOG.warn("Interrupted while doing chmod for " + subDir);
    }
  }

  /**
   * Validates if setupWorkDir is properly cleaning up contents of workDir.
   * TODO: other things of TaskRunner.setupWorkDir() related to distributed
   * cache need to be validated.
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

    // create {myWorkDir}/subDir/file and set 555 perms for subDir and file
    createFileAndSetPermissions(jConf, myWorkDir);

    TaskRunner.deleteDirContents(jConf, new File(myWorkDir.toUri().getPath()));
    
    assertTrue("Contents of " + myWorkDir + " are not cleaned up properly.",
        fs.listStatus(myWorkDir).length == 0);
    
    // cleanup
    fs.delete(rootDir, true);
  }
}
