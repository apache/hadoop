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

package org.apache.hadoop.fs;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFsShellCopy {  
  static Configuration conf;
  static FsShell shell; 
  static LocalFileSystem lfs;
  static Path testRootDir, srcPath, dstPath;
  
  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration();
    shell = new FsShell(conf);
    lfs = FileSystem.getLocal(conf);
    testRootDir = new Path(
        System.getProperty("test.build.data","test/build/data"), "testShellCopy");
    lfs.mkdirs(testRootDir);    
    srcPath = new Path(testRootDir, "srcFile");
    dstPath = new Path(testRootDir, "dstFile");
  }
  
  @Before
  public void prepFiles() throws Exception {
    lfs.setVerifyChecksum(true);
    lfs.setWriteChecksum(true);
    
    lfs.delete(srcPath, true);
    lfs.delete(dstPath, true);
    FSDataOutputStream out = lfs.create(srcPath);
    out.writeChars("hi");
    out.close();
    assertTrue(lfs.exists(lfs.getChecksumFile(srcPath)));
  }

  @Test
  public void testCopyNoCrc() throws Exception {
    shellRun(0, "-get", srcPath.toString(), dstPath.toString());
    checkPath(dstPath, false);
  }

  @Test
  public void testCopyCrc() throws Exception {
    shellRun(0, "-get", "-crc", srcPath.toString(), dstPath.toString());
    checkPath(dstPath, true);
  }

  
  @Test
  public void testCorruptedCopyCrc() throws Exception {
    FSDataOutputStream out = lfs.getRawFileSystem().create(srcPath);
    out.writeChars("bang");
    out.close();
    shellRun(1, "-get", srcPath.toString(), dstPath.toString());
  }

  @Test
  public void testCorruptedCopyIgnoreCrc() throws Exception {
    shellRun(0, "-get", "-ignoreCrc", srcPath.toString(), dstPath.toString());
    checkPath(dstPath, false);
  }

  private void checkPath(Path p, boolean expectChecksum) throws IOException {
    assertTrue(lfs.exists(p));
    boolean hasChecksum = lfs.exists(lfs.getChecksumFile(p));
    assertEquals(expectChecksum, hasChecksum);
  }

  private void shellRun(int n, String ... args) throws Exception {
    assertEquals(n, shell.run(args));
  }
}
