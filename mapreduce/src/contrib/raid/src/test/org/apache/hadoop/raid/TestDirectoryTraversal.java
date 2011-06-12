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
package org.apache.hadoop.raid;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class TestDirectoryTraversal extends TestCase {
  final static Log LOG = LogFactory.getLog(
                            "org.apache.hadoop.raid.TestDirectoryTraversal");
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();

  MiniDFSCluster dfs = null;
  FileSystem fs = null;
  Configuration conf = null;

  /**
   * Test basic enumeration.
   */
  public void testEnumeration() throws IOException {
    mySetup();

    try {
      Path topDir = new Path(TEST_DIR + "/testenumeration");

      createTestTree(topDir);

      LOG.info("Enumerating files");
      List<FileStatus> startPaths = new LinkedList<FileStatus>();
      startPaths.add(fs.getFileStatus(topDir));
      DirectoryTraversal dt = new DirectoryTraversal(fs, startPaths);

      List<FileStatus> selected = new LinkedList<FileStatus>();
      while (true) {
        FileStatus f = dt.getNextFile();
        if (f == null) break;
        assertEquals(false, f.isDir());
        LOG.info(f.getPath());
        selected.add(f);
      }
      assertEquals(5, selected.size());

      LOG.info("Enumerating directories");
      startPaths.clear();
      startPaths.add(fs.getFileStatus(topDir));
      dt = new DirectoryTraversal(fs, startPaths);
      selected.clear();
      while (true) {
        FileStatus dir = dt.getNextDirectory();
        if (dir == null) break;
        assertEquals(true, dir.isDir());
        LOG.info(dir.getPath());
        selected.add(dir);
      }
      assertEquals(4, selected.size());
    } finally {
      myTearDown();
    }
  }

  public void testSuspension() throws IOException {
    mySetup();

    try {
      Path topDir = new Path(TEST_DIR + "/testenumeration");

      createTestTree(topDir);

      String top = topDir.toString();
      List<FileStatus> startPaths = new LinkedList<FileStatus>();
      startPaths.add(fs.getFileStatus(new Path(top + "/a")));
      startPaths.add(fs.getFileStatus(new Path(top + "/b")));
      DirectoryTraversal dt = new DirectoryTraversal(fs, startPaths);

      int limit = 2;
      short targetRepl = 1;
      Path raid = new Path("/raid");
      List<FileStatus> selected = dt.selectFilesToRaid(conf, targetRepl, raid,
                                                        0, limit);
      for (FileStatus f: selected) {
        LOG.info(f.getPath());
      }
      assertEquals(limit, selected.size());

      selected = dt.selectFilesToRaid(conf, targetRepl, raid, 0, limit);
      for (FileStatus f: selected) {
        LOG.info(f.getPath());
      }
      assertEquals(limit, selected.size());
    } finally {
      myTearDown();
    }
  }

  /**
   * Creates a test directory tree.
   *            top
   *           / | \
   *          /  |  f5
   *         a   b___
   *        / \  |\  \
   *       f1 f2 f3f4 c
   */
  private void createTestTree(Path topDir) throws IOException {
    String top = topDir.toString();
    fs.delete(topDir, true);

    fs.mkdirs(topDir);
    fs.create(new Path(top + "/f5")).close();

    fs.mkdirs(new Path(top + "/a"));
    createTestFile(new Path(top + "/a/f1"));
    createTestFile(new Path(top + "/a/f2"));

    fs.mkdirs(new Path(top + "/b"));
    fs.mkdirs(new Path(top + "/b/c"));
    createTestFile(new Path(top + "/b/f3"));
    createTestFile(new Path(top + "/b/f4"));
  }

  private void createTestFile(Path file) throws IOException {
    long blockSize = 8192;
    byte[] bytes = new byte[(int)blockSize];
    FSDataOutputStream stm = fs.create(file, false, 4096, (short)1, blockSize);
    stm.write(bytes);
    stm.write(bytes);
    stm.write(bytes);
    stm.close();
    FileStatus stat = fs.getFileStatus(file);
    assertEquals(blockSize, stat.getBlockSize());
  }

  private void mySetup() throws IOException {
    conf = new Configuration();
    dfs = new MiniDFSCluster(conf, 6, true, null);
    dfs.waitActive();
    fs = dfs.getFileSystem();
  }

  private void myTearDown() {
    if (dfs != null) { dfs.shutdown(); }
  }
}
