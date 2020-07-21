/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED;

/** Test ordered snapshot deletion. */
public class TestOrderedSnapshotDeletion {
  static final Logger LOG = LoggerFactory.getLogger(FSDirectory.class);

  {
    SnapshotTestHelper.disableLogs();
    GenericTestUtils.setLogLevel(INode.LOG, Level.TRACE);
  }

  private final Path snapshottableDir
      = new Path("/" + getClass().getSimpleName());

  private MiniDFSCluster cluster;

  @Before
  public void setUp() throws Exception {
    final Configuration conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED, true);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
    cluster.waitActive();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test (timeout=60000)
  public void testConf() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);

    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.createSnapshot(snapshottableDir, "s1");

    final Path sub2 = new Path(snapshottableDir, "sub2");
    hdfs.mkdirs(sub2);
    hdfs.createSnapshot(snapshottableDir, "s2");

    assertDeletionDenied(snapshottableDir, "s1", hdfs);
    assertDeletionDenied(snapshottableDir, "s2", hdfs);
    hdfs.deleteSnapshot(snapshottableDir, "s0");
    assertDeletionDenied(snapshottableDir, "s2", hdfs);
    hdfs.deleteSnapshot(snapshottableDir, "s1");
    hdfs.deleteSnapshot(snapshottableDir, "s2");
  }

  static void assertDeletionDenied(Path snapshottableDir, String snapshot,
      DistributedFileSystem hdfs) throws IOException {
    try {
      hdfs.deleteSnapshot(snapshottableDir, snapshot);
      Assert.fail("deleting " +snapshot + " should fail");
    } catch (SnapshotException se) {
      LOG.info("Good, it is expected to have " + se);
    }
  }
}
