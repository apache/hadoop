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

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestSymlinkHdfsFileSystem extends TestSymlinkHdfs {

  @BeforeClass
  public static void testSetup() throws Exception {
    wrapper = new FileSystemTestWrapper(dfs, "/tmp/TestSymlinkHdfsFileSystem");
  }

  @Override
  @Ignore("FileSystem adds missing authority in absolute URIs")
  @Test(timeout=1000)
  public void testCreateWithPartQualPathFails() throws IOException {}

  @Ignore("FileSystem#create creates parent directories," +
      " so dangling links to directories are created")
  @Override
  @Test(timeout=1000)
  public void testCreateFileViaDanglingLinkParent() throws IOException {}

  // Additional tests for DFS-only methods

  @Test(timeout=10000)
  public void testRecoverLease() throws IOException {
    Path dir  = new Path(testBaseDir1());
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "link");
    wrapper.setWorkingDirectory(dir);
    createAndWriteFile(file);
    wrapper.createSymlink(file, link, false);
    // Attempt recoverLease through a symlink
    boolean closed = dfs.recoverLease(link);
    assertTrue("Expected recoverLease to return true", closed);
  }

  @Test(timeout=10000)
  public void testIsFileClosed() throws IOException {
    Path dir  = new Path(testBaseDir1());
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "link");
    wrapper.setWorkingDirectory(dir);
    createAndWriteFile(file);
    wrapper.createSymlink(file, link, false);
    // Attempt recoverLease through a symlink
    boolean closed = dfs.isFileClosed(link);
    assertTrue("Expected isFileClosed to return true", closed);
  }

  @Test(timeout=10000)
  public void testConcat() throws Exception {
    Path dir  = new Path(testBaseDir1());
    Path link = new Path(testBaseDir1(), "link");
    Path dir2 = new Path(testBaseDir2());
    wrapper.createSymlink(dir2, link, false);
    wrapper.setWorkingDirectory(dir);
    // Concat with a target and srcs through a link
    Path target = new Path(link, "target");
    createAndWriteFile(target);
    Path[] srcs = new Path[3];
    for (int i=0; i<srcs.length; i++) {
      srcs[i] = new Path(link, "src-" + i);
      createAndWriteFile(srcs[i]);
    }
    dfs.concat(target, srcs);
  }

  @Test(timeout=10000)
  public void testSnapshot() throws Exception {
    Path dir  = new Path(testBaseDir1());
    Path link = new Path(testBaseDir1(), "link");
    Path dir2 = new Path(testBaseDir2());
    wrapper.createSymlink(dir2, link, false);
    wrapper.setWorkingDirectory(dir);
    dfs.allowSnapshot(link);
    dfs.disallowSnapshot(link);
    dfs.allowSnapshot(link);
    dfs.createSnapshot(link, "mcmillan");
    dfs.renameSnapshot(link, "mcmillan", "seaborg");
    dfs.deleteSnapshot(link, "seaborg");
  }
}
