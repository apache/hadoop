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
package org.apache.hadoop.hdfs.web;


import static org.apache.hadoop.fs.FileSystemTestHelper.exists;
import static org.apache.hadoop.fs.FileSystemTestHelper.getDefaultBlockSize;
import static org.apache.hadoop.fs.FileSystemTestHelper.getTestRootPath;

import java.io.IOException;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.resources.ExceptionHandler;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

public class TestFSMainOperationsWebHdfs extends FSMainOperationsBaseTest {
  {
    ((Log4JLogger)ExceptionHandler.LOG).getLogger().setLevel(Level.ALL);
  }

  private static final MiniDFSCluster cluster;
  private static final Path defaultWorkingDirectory;

  static {
    Configuration conf = new Configuration();
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      fSys = cluster.getWebHdfsFileSystem();
      defaultWorkingDirectory = fSys.getWorkingDirectory();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Path getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  /** Override the following method without using position read. */
  @Override
  protected void writeReadAndDelete(int len) throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");
    fSys.mkdirs(path.getParent());

    FSDataOutputStream out = 
      fSys.create(path, false, 4096, (short) 1, getDefaultBlockSize() );
    out.write(data, 0, len);
    out.close();

    Assert.assertTrue("Exists", exists(fSys, path));
    Assert.assertEquals("Length", len, fSys.getFileStatus(path).getLen());

    FSDataInputStream in = fSys.open(path);
    for (int i = 0; i < len; i++) {
      final int b  = in.read();
      Assert.assertEquals("Position " + i, data[i], b);
    }
    in.close();
    Assert.assertTrue("Deleted", fSys.delete(path, false));
    Assert.assertFalse("No longer exists", exists(fSys, path));
  }

  //copied from trunk.
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = getTestRootPath(fSys, "test/hadoop");
    Assert.assertFalse(exists(fSys, testDir));
    fSys.mkdirs(testDir);
    Assert.assertTrue(exists(fSys, testDir));
    
    createFile(getTestRootPath(fSys, "test/hadoop/file"));
    
    Path testSubDir = getTestRootPath(fSys, "test/hadoop/file/subdir");
    try {
      fSys.mkdirs(testSubDir);
      Assert.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    Assert.assertFalse(exists(fSys, testSubDir));
    
    Path testDeepSubDir = getTestRootPath(fSys, "test/hadoop/file/deep/sub/dir");
    try {
      fSys.mkdirs(testDeepSubDir);
      Assert.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    Assert.assertFalse(exists(fSys, testDeepSubDir));
  }
}
