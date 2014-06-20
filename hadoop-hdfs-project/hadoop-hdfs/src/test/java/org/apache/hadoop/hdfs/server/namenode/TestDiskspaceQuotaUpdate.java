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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDiskspaceQuotaUpdate {
  private static final int BLOCKSIZE = 1024;
  private static final short REPLICATION = 4;
  static final long seed = 0L;
  private static final Path dir = new Path("/TestQuotaUpdate");

  private Configuration conf;
  private MiniDFSCluster cluster;
  private FSDirectory fsdir;
  private DistributedFileSystem dfs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();

    fsdir = cluster.getNamesystem().getFSDirectory();
    dfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test if the quota can be correctly updated for create file
   */
  @Test (timeout=60000)
  public void testQuotaUpdateWithFileCreate() throws Exception  {
    final Path foo = new Path(dir, "foo");
    Path createdFile = new Path(foo, "created_file.data");
    dfs.mkdirs(foo);
    dfs.setQuota(foo, Long.MAX_VALUE-1, Long.MAX_VALUE-1);
    long fileLen = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    DFSTestUtil.createFile(dfs, createdFile, BLOCKSIZE / 16,
        fileLen, BLOCKSIZE, REPLICATION, seed);
    INode fnode = fsdir.getINode4Write(foo.toString());
    assertTrue(fnode.isDirectory());
    assertTrue(fnode.isQuotaSet());
    Quota.Counts cnt = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed();
    assertEquals(2, cnt.get(Quota.NAMESPACE));
    assertEquals(fileLen * REPLICATION, cnt.get(Quota.DISKSPACE));
  }

  /**
   * Test if the quota can be correctly updated for append
   */
  @Test (timeout=60000)
  public void testUpdateQuotaForAppend() throws Exception {
    final Path foo = new Path(dir ,"foo");
    final Path bar = new Path(foo, "bar");
    long currentFileLen = BLOCKSIZE;
    DFSTestUtil.createFile(dfs, bar, currentFileLen, REPLICATION, seed);
    dfs.setQuota(foo, Long.MAX_VALUE-1, Long.MAX_VALUE-1);

    // append half of the block data, the previous file length is at block
    // boundary
    DFSTestUtil.appendFile(dfs, bar, BLOCKSIZE / 2);
    currentFileLen += (BLOCKSIZE / 2);

    INodeDirectory fooNode = fsdir.getINode4Write(foo.toString()).asDirectory();
    assertTrue(fooNode.isQuotaSet());
    Quota.Counts quota = fooNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed();
    long ns = quota.get(Quota.NAMESPACE);
    long ds = quota.get(Quota.DISKSPACE);
    assertEquals(2, ns); // foo and bar
    assertEquals(currentFileLen * REPLICATION, ds);
    ContentSummary c = dfs.getContentSummary(foo);
    assertEquals(c.getSpaceConsumed(), ds);

    // append another block, the previous file length is not at block boundary
    DFSTestUtil.appendFile(dfs, bar, BLOCKSIZE);
    currentFileLen += BLOCKSIZE;

    quota = fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed();
    ns = quota.get(Quota.NAMESPACE);
    ds = quota.get(Quota.DISKSPACE);
    assertEquals(2, ns); // foo and bar
    assertEquals(currentFileLen * REPLICATION, ds);
    c = dfs.getContentSummary(foo);
    assertEquals(c.getSpaceConsumed(), ds);

    // append several blocks
    DFSTestUtil.appendFile(dfs, bar, BLOCKSIZE * 3 + BLOCKSIZE / 8);
    currentFileLen += (BLOCKSIZE * 3 + BLOCKSIZE / 8);

    quota = fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed();
    ns = quota.get(Quota.NAMESPACE);
    ds = quota.get(Quota.DISKSPACE);
    assertEquals(2, ns); // foo and bar
    assertEquals(currentFileLen * REPLICATION, ds);
    c = dfs.getContentSummary(foo);
    assertEquals(c.getSpaceConsumed(), ds);
  }

  /**
   * Test if the quota can be correctly updated when file length is updated
   * through fsync
   */
  @Test (timeout=60000)
  public void testUpdateQuotaForFSync() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(dfs, bar, BLOCKSIZE, REPLICATION, 0L);
    dfs.setQuota(foo, Long.MAX_VALUE-1, Long.MAX_VALUE-1);

    FSDataOutputStream out = dfs.append(bar);
    out.write(new byte[BLOCKSIZE / 4]);
    ((DFSOutputStream) out.getWrappedStream()).hsync(EnumSet
        .of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));

    INodeDirectory fooNode = fsdir.getINode4Write(foo.toString()).asDirectory();
    Quota.Counts quota = fooNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed();
    long ns = quota.get(Quota.NAMESPACE);
    long ds = quota.get(Quota.DISKSPACE);
    assertEquals(2, ns); // foo and bar
    assertEquals(BLOCKSIZE * 2 * REPLICATION, ds); // file is under construction

    out.write(new byte[BLOCKSIZE / 4]);
    out.close();

    fooNode = fsdir.getINode4Write(foo.toString()).asDirectory();
    quota = fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed();
    ns = quota.get(Quota.NAMESPACE);
    ds = quota.get(Quota.DISKSPACE);
    assertEquals(2, ns);
    assertEquals((BLOCKSIZE + BLOCKSIZE / 2) * REPLICATION, ds);

    // append another block
    DFSTestUtil.appendFile(dfs, bar, BLOCKSIZE);

    quota = fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed();
    ns = quota.get(Quota.NAMESPACE);
    ds = quota.get(Quota.DISKSPACE);
    assertEquals(2, ns); // foo and bar
    assertEquals((BLOCKSIZE * 2 + BLOCKSIZE / 2) * REPLICATION, ds);
  }
}
