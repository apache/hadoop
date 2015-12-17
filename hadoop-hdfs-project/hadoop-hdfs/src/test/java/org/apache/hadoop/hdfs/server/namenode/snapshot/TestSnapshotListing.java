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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSnapshotListing {

  static final long seed = 0;
  static final short REPLICATION = 3;
  static final long BLOCKSIZE = 1024;

  private final Path dir = new Path("/test.snapshot/dir");
  
  Configuration conf;
  MiniDFSCluster cluster;
  FSNamesystem fsn;
  DistributedFileSystem hdfs;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
    hdfs.mkdirs(dir);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
  
  /**
   * Test listing snapshots under a snapshottable directory
   */
  @Test (timeout=15000)
  public void testListSnapshots() throws Exception {
    final Path snapshotsPath = new Path(dir, ".snapshot");
    FileStatus[] stats = null;
    
    // special case: snapshots of root
    stats = hdfs.listStatus(new Path("/.snapshot"));
    // should be 0 since root's snapshot quota is 0
    assertEquals(0, stats.length);
    
    // list before set dir as snapshottable
    try {
      stats = hdfs.listStatus(snapshotsPath);
      fail("expect SnapshotException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "Directory is not a snapshottable directory: " + dir.toString(), e);
    }
    
    // list before creating snapshots
    hdfs.allowSnapshot(dir);
    stats = hdfs.listStatus(snapshotsPath);
    assertEquals(0, stats.length);
    
    // list while creating snapshots
    final int snapshotNum = 5;
    for (int sNum = 0; sNum < snapshotNum; sNum++) {
      hdfs.createSnapshot(dir, "s_" + sNum);
      stats = hdfs.listStatus(snapshotsPath);
      assertEquals(sNum + 1, stats.length);
      for (int i = 0; i <= sNum; i++) {
        assertEquals("s_" + i, stats[i].getPath().getName());
      }
    }
    
    // list while deleting snapshots
    for (int sNum = snapshotNum - 1; sNum > 0; sNum--) {
      hdfs.deleteSnapshot(dir, "s_" + sNum);
      stats = hdfs.listStatus(snapshotsPath);
      assertEquals(sNum, stats.length);
      for (int i = 0; i < sNum; i++) {
        assertEquals("s_" + i, stats[i].getPath().getName());
      }
    }
    
    // remove the last snapshot
    hdfs.deleteSnapshot(dir, "s_0");
    stats = hdfs.listStatus(snapshotsPath);
    assertEquals(0, stats.length);
  }
}
