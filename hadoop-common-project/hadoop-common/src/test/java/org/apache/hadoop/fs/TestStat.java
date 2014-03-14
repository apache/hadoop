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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStat extends FileSystemTestHelper {
  static{
    FileSystem.enableSymlinks();
  }
  private static Stat stat;

  @BeforeClass
  public static void setup() throws Exception {
    stat = new Stat(new Path("/dummypath"),
        4096l, false, FileSystem.get(new Configuration()));
  }

  private class StatOutput {
    final String doesNotExist;
    final String directory;
    final String file;
    final String symlink;
    final String stickydir;

    StatOutput(String doesNotExist, String directory, String file,
        String symlink, String stickydir) {
      this.doesNotExist = doesNotExist;
      this.directory = directory;
      this.file = file;
      this.symlink = symlink;
      this.stickydir = stickydir;
    }

    void test() throws Exception {
      BufferedReader br;
      FileStatus status;

      try {
        br = new BufferedReader(new StringReader(doesNotExist));
        stat.parseExecResult(br);
      } catch (FileNotFoundException e) {
        // expected
      }

      br = new BufferedReader(new StringReader(directory));
      stat.parseExecResult(br);
      status = stat.getFileStatusForTesting();
      assertTrue(status.isDirectory());

      br = new BufferedReader(new StringReader(file));
      stat.parseExecResult(br);
      status = stat.getFileStatusForTesting();
      assertTrue(status.isFile());

      br = new BufferedReader(new StringReader(symlink));
      stat.parseExecResult(br);
      status = stat.getFileStatusForTesting();
      assertTrue(status.isSymlink());

      br = new BufferedReader(new StringReader(stickydir));
      stat.parseExecResult(br);
      status = stat.getFileStatusForTesting();
      assertTrue(status.isDirectory());
      assertTrue(status.getPermission().getStickyBit());
    }
  }

  @Test(timeout=10000)
  public void testStatLinux() throws Exception {
    StatOutput linux = new StatOutput(
        "stat: cannot stat `watermelon': No such file or directory",
        "4096,directory,1373584236,1373586485,755,andrew,root,`.'",
        "0,regular empty file,1373584228,1373584228,644,andrew,andrew,`target'",
        "6,symbolic link,1373584236,1373584236,777,andrew,andrew,`link' -> `target'",
        "4096,directory,1374622334,1375124212,1755,andrew,andrew,`stickydir'");
    linux.test();
  }

  @Test(timeout=10000)
  public void testStatFreeBSD() throws Exception {
    StatOutput freebsd = new StatOutput(
        "stat: symtest/link: stat: No such file or directory",
        "512,Directory,1373583695,1373583669,40755,awang,awang,`link' -> `'",
        "0,Regular File,1373508937,1373508937,100644,awang,awang,`link' -> `'",
        "6,Symbolic Link,1373508941,1373508941,120755,awang,awang,`link' -> `target'",
        "512,Directory,1375139537,1375139537,41755,awang,awang,`link' -> `'");
    freebsd.test();
  }

  @Test(timeout=10000)
  public void testStatFileNotFound() throws Exception {
    Assume.assumeTrue(Stat.isAvailable());
    try {
      stat.getFileStatus();
      fail("Expected FileNotFoundException");
    } catch (FileNotFoundException e) {
      // expected
    }
  }

  @Test(timeout=10000)
  public void testStatEnvironment() throws Exception {
    assertEquals("C", stat.getEnvironment("LANG"));
  }

  @Test(timeout=10000)
  public void testStat() throws Exception {
    Assume.assumeTrue(Stat.isAvailable());
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path testDir = new Path(getTestRootPath(fs), "teststat");
    fs.mkdirs(testDir);
    Path sub1 = new Path(testDir, "sub1");
    Path sub2 = new Path(testDir, "sub2");
    fs.mkdirs(sub1);
    fs.createSymlink(sub1, sub2, false);
    FileStatus stat1 = new Stat(sub1, 4096l, false, fs).getFileStatus();
    FileStatus stat2 = new Stat(sub2, 0, false, fs).getFileStatus();
    assertTrue(stat1.isDirectory());
    assertFalse(stat2.isDirectory());
    fs.delete(testDir, true);
  }
}
