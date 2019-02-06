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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Verify content summary is computed correctly when
 * 1. There are snapshots taken under the directory
 * 2. The given path is a snapshot path
 */
public class TestGetContentSummaryWithSnapshot {
  protected static final short REPLICATION = 3;
  protected static final long BLOCKSIZE = 1024;

  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected FSNamesystem fsn;
  protected FSDirectory fsdir;
  protected DistributedFileSystem dfs;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION).build();
    cluster.waitActive();

    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();
    dfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Calculate against a snapshot path.
   * 1. create dirs /foo/bar
   * 2. take snapshot s1 on /foo
   * 3. create a 10 byte file /foo/bar/baz
   * Make sure for "/foo/bar" and "/foo/.snapshot/s1/bar" have correct results:
   * the 1 byte file is not included in snapshot s1.
   * 4. create another snapshot, append to the file /foo/bar/baz,
   * and make sure file count, directory count and file length is good.
   * 5. delete the file, ensure contentSummary output too.
   */
  @Test
  public void testGetContentSummary() throws IOException {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    final Path baz = new Path(bar, "baz");

    dfs.mkdirs(bar);
    dfs.allowSnapshot(foo);
    dfs.createSnapshot(foo, "s1");

    DFSTestUtil.createFile(dfs, baz, 10, REPLICATION, 0L);

    ContentSummary summary = cluster.getNameNodeRpc().getContentSummary(
        bar.toString());
    Assert.assertEquals(1, summary.getDirectoryCount());
    Assert.assertEquals(1, summary.getFileCount());
    Assert.assertEquals(10, summary.getLength());

    final Path barS1 = SnapshotTestHelper.getSnapshotPath(foo, "s1", "bar");
    summary = cluster.getNameNodeRpc().getContentSummary(barS1.toString());
    Assert.assertEquals(1, summary.getDirectoryCount());
    Assert.assertEquals(0, summary.getFileCount());
    Assert.assertEquals(0, summary.getLength());

    // also check /foo and /foo/.snapshot/s1
    summary = cluster.getNameNodeRpc().getContentSummary(foo.toString());
    Assert.assertEquals(2, summary.getDirectoryCount());
    Assert.assertEquals(1, summary.getFileCount());
    Assert.assertEquals(10, summary.getLength());

    final Path fooS1 = SnapshotTestHelper.getSnapshotRoot(foo, "s1");
    summary = cluster.getNameNodeRpc().getContentSummary(fooS1.toString());
    Assert.assertEquals(2, summary.getDirectoryCount());
    Assert.assertEquals(0, summary.getFileCount());
    Assert.assertEquals(0, summary.getLength());

    // create a new snapshot s2 and update the file
    dfs.createSnapshot(foo, "s2");
    DFSTestUtil.appendFile(dfs, baz, 10);
    summary = cluster.getNameNodeRpc().getContentSummary(
        bar.toString());
    Assert.assertEquals(1, summary.getDirectoryCount());
    Assert.assertEquals(1, summary.getFileCount());
    Assert.assertEquals(20, summary.getLength());

    final Path fooS2 = SnapshotTestHelper.getSnapshotRoot(foo, "s2");
    summary = cluster.getNameNodeRpc().getContentSummary(fooS2.toString());
    Assert.assertEquals(2, summary.getDirectoryCount());
    Assert.assertEquals(1, summary.getFileCount());
    Assert.assertEquals(10, summary.getLength());

    cluster.getNameNodeRpc().delete(baz.toString(), false);

    summary = cluster.getNameNodeRpc().getContentSummary(
        foo.toString());
    Assert.assertEquals(0, summary.getSnapshotDirectoryCount());
    Assert.assertEquals(1, summary.getSnapshotFileCount());
    Assert.assertEquals(20, summary.getSnapshotLength());

    final Path bazS1 = SnapshotTestHelper.getSnapshotPath(foo, "s1", "bar/baz");
    try {
      cluster.getNameNodeRpc().getContentSummary(bazS1.toString());
      Assert.fail("should get FileNotFoundException");
    } catch (FileNotFoundException ignored) {}
  }
}
