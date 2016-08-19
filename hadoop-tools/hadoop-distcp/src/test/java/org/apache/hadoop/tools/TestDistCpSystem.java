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

package org.apache.hadoop.tools;

import static org.apache.hadoop.test.GenericTestUtils.getMethodName;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * A JUnit test for copying files recursively.
 */

public class TestDistCpSystem {
  @Rule
  public Timeout globalTimeout = new Timeout(30000);

  private static final String SRCDAT = "srcdat";
  private static final String DSTDAT = "dstdat";

  private static MiniDFSCluster cluster;
  private static Configuration conf;

  private class FileEntry {
    String path;
    boolean isDir;
    public FileEntry(String path, boolean isDir) {
      this.path = path;
      this.isDir = isDir;
    }
    String getPath() { return path; }
    boolean isDirectory() { return isDir; }
  }
  
  private void createFiles(FileSystem fs, String topdir,
      FileEntry[] entries) throws IOException {
    for (FileEntry entry : entries) {
      Path newpath = new Path(topdir + "/" + entry.getPath());
      if (entry.isDirectory()) {
        fs.mkdirs(newpath);
      } else {
        OutputStream out = fs.create(newpath);
        try {
          out.write((topdir + "/" + entry).getBytes());
          out.write("\n".getBytes());
        } finally {
          out.close();
        }
      }
    }
  }
   
  private static FileStatus[] getFileStatus(FileSystem fs,
      String topdir, FileEntry[] files) throws IOException {
      Path root = new Path(topdir);
      List<FileStatus> statuses = new ArrayList<FileStatus>();
      
      for (int idx = 0; idx < files.length; ++idx) {
        Path newpath = new Path(root, files[idx].getPath());
        statuses.add(fs.getFileStatus(newpath));
      }
      return statuses.toArray(new FileStatus[statuses.size()]);
    }
  

  /** delete directory and everything underneath it.*/
  private static void deldir(FileSystem fs, String topdir) throws IOException {
    fs.delete(new Path(topdir), true);
  }

  private void testPreserveUserHelper(String testRoot,
                                      FileEntry[] srcEntries,
                                      FileEntry[] dstEntries,
                                      boolean createSrcDir,
                                      boolean createTgtDir,
                                      boolean update) throws Exception {
    final String testSrcRel = SRCDAT;
    final String testSrc = testRoot + "/" + testSrcRel;
    final String testDstRel = DSTDAT;
    final String testDst = testRoot + "/" + testDstRel;

    String nnUri = FileSystem.getDefaultUri(conf).toString();
    FileSystem fs = FileSystem.get(URI.create(nnUri), conf);
    fs.mkdirs(new Path(testRoot));
    if (createSrcDir) {
      fs.mkdirs(new Path(testSrc));
    }
    if (createTgtDir) {
      fs.mkdirs(new Path(testDst));
    }

    createFiles(fs, testRoot, srcEntries);
    FileStatus[] srcstats = getFileStatus(fs, testRoot, srcEntries);
    for(int i = 0; i < srcEntries.length; i++) {
      fs.setOwner(srcstats[i].getPath(), "u" + i, null);
    }
    String[] args = update? new String[]{"-pu", "-update", nnUri+testSrc,
        nnUri+testDst} : new String[]{"-pu", nnUri+testSrc, nnUri+testDst};

    ToolRunner.run(conf, new DistCp(), args);

    String realTgtPath = testDst;
    if (!createTgtDir) {
      realTgtPath = testRoot;
    }
    FileStatus[] dststat = getFileStatus(fs, realTgtPath, dstEntries);
    for(int i = 0; i < dststat.length; i++) {
      assertEquals("i=" + i, "u" + i, dststat[i].getOwner());
    }
    deldir(fs, testRoot);
  }

  @BeforeClass
  public static void beforeClass() throws IOException {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testPreserveUseNonEmptyDir() throws Exception {
    String testRoot = "/testdir." + getMethodName();
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT, true),
        new FileEntry(SRCDAT + "/a", false),
        new FileEntry(SRCDAT + "/b", true),
        new FileEntry(SRCDAT + "/b/c", false)
    };

    FileEntry[] dstfiles = {
        new FileEntry(DSTDAT, true),
        new FileEntry(DSTDAT + "/a", false),
        new FileEntry(DSTDAT + "/b", true),
        new FileEntry(DSTDAT + "/b/c", false)
    };

    testPreserveUserHelper(testRoot, srcfiles, srcfiles, false, true, false);
    testPreserveUserHelper(testRoot, srcfiles, dstfiles, false, false, false);
  }


  @Test
  public void testPreserveUserEmptyDir() throws Exception {
    String testRoot = "/testdir." + getMethodName();
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT, true)
    };
    
    FileEntry[] dstfiles = {
        new FileEntry(DSTDAT, true)
    };
    
    testPreserveUserHelper(testRoot, srcfiles, srcfiles, false, true, false);
    testPreserveUserHelper(testRoot, srcfiles, dstfiles, false, false, false);
  }

  @Test
  public void testPreserveUserSingleFile() throws Exception {
    String testRoot = "/testdir." + getMethodName();
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT, false)
    };
    FileEntry[] dstfiles = {
        new FileEntry(DSTDAT, false)
    };
    testPreserveUserHelper(testRoot, srcfiles, srcfiles, false, true, false);
    testPreserveUserHelper(testRoot, srcfiles, dstfiles, false, false, false);
  }

  @Test
  public void testPreserveUserNonEmptyDirWithUpdate() throws Exception {
    String testRoot = "/testdir." + getMethodName();
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT + "/a", false),
        new FileEntry(SRCDAT + "/b", true),
        new FileEntry(SRCDAT + "/b/c", false)
    };

    FileEntry[] dstfiles = {
        new FileEntry("a", false),
        new FileEntry("b", true),
        new FileEntry("b/c", false)
    };

    testPreserveUserHelper(testRoot, srcfiles, dstfiles, true, true, true);
  }

  @Test
  public void testSourceRoot() throws Exception {
    FileSystem fs = cluster.getFileSystem();

    String rootStr = fs.makeQualified(new Path("/")).toString();

    String testRoot = "/testdir." + getMethodName();

    // Case 1. The target does not exist.

    Path tgtPath = new Path(testRoot + "/nodir");
    String tgtStr = fs.makeQualified(tgtPath).toString();
    String[] args = new String[]{rootStr, tgtStr};
    Assert.assertThat(ToolRunner.run(conf, new DistCp(), args), is(0));

    // Case 2. The target exists.

    Path tgtPath2 = new Path(testRoot + "/dir");
    assertTrue(fs.mkdirs(tgtPath2));
    String tgtStr2 = fs.makeQualified(tgtPath2).toString();
    String[] args2 = new String[]{rootStr, tgtStr2};
    Assert.assertThat(ToolRunner.run(conf, new DistCp(), args2), is(0));
  }
}