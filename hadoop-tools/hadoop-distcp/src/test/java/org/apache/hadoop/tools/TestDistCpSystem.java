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

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.ToolRunner;

/**
 * A JUnit test for copying files recursively.
 */

public class TestDistCpSystem extends TestCase {
  
  private static final String SRCDAT = "srcdat";
  private static final String DSTDAT = "dstdat";
  
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
   
  private void testPreserveUserHelper(
      FileEntry[] srcEntries,
      FileEntry[] dstEntries,
      boolean createSrcDir,
      boolean createTgtDir,
      boolean update) throws Exception {
    Configuration conf = null;
    MiniDFSCluster cluster = null;
    try {
      final String testRoot = "/testdir";
      final String testSrcRel = SRCDAT;
      final String testSrc = testRoot + "/" + testSrcRel;
      final String testDstRel = DSTDAT;
      final String testDst = testRoot + "/" + testDstRel;

      conf = new Configuration(); 
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();

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
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testPreserveUseNonEmptyDir() throws Exception {
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

    testPreserveUserHelper(srcfiles, srcfiles, false, true, false);
    testPreserveUserHelper(srcfiles, dstfiles, false, false, false);
  }
  
 
  public void testPreserveUserEmptyDir() throws Exception {
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT, true)
    };
    
    FileEntry[] dstfiles = {
        new FileEntry(DSTDAT, true)
    };
    
    testPreserveUserHelper(srcfiles, srcfiles, false, true, false);
    testPreserveUserHelper(srcfiles, dstfiles, false, false, false);
  }

  public void testPreserveUserSingleFile() throws Exception {
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT, false)
    };
    FileEntry[] dstfiles = {
        new FileEntry(DSTDAT, false)
    };
    testPreserveUserHelper(srcfiles, srcfiles, false, true, false);
    testPreserveUserHelper(srcfiles, dstfiles, false, false, false);
  }
  
  public void testPreserveUserNonEmptyDirWithUpdate() throws Exception {
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

    testPreserveUserHelper(srcfiles, dstfiles, true, true, true);
  }

}