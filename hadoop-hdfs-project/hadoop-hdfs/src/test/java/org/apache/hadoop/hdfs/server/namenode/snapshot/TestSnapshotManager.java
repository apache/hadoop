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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;


/**
 * Testing snapshot manager functionality.
 */
public class TestSnapshotManager {
  private static final int testMaxSnapshotIDLimit = 7;

  /**
   * Test that the global limit on snapshot Ids is honored.
   */
  @Test (timeout=10000)
  public void testSnapshotIDLimits() throws Exception {
    testMaxSnapshotLimit(testMaxSnapshotIDLimit, "rollover",
        new Configuration(), testMaxSnapshotIDLimit);
  }

  /**
   * Tests that the global limit on snapshots is honored.
   */
  @Test (timeout=10000)
  public void testMaxSnapshotLimit() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_FILESYSTEM_LIMIT,
        testMaxSnapshotIDLimit);
    conf.setInt(DFSConfigKeys.
            DFS_NAMENODE_SNAPSHOT_MAX_LIMIT,
        testMaxSnapshotIDLimit);
    testMaxSnapshotLimit(testMaxSnapshotIDLimit,"max snapshot limit" ,
        conf, testMaxSnapshotIDLimit * 2);
  }

  private void testMaxSnapshotLimit(int maxSnapshotLimit, String errMsg,
                                    Configuration conf, int maxSnapID)
      throws IOException {
    LeaseManager leaseManager = mock(LeaseManager.class);
    INodeDirectory ids = mock(INodeDirectory.class);
    FSDirectory fsdir = mock(FSDirectory.class);
    INodesInPath iip = mock(INodesInPath.class);

    SnapshotManager sm = spy(new SnapshotManager(conf, fsdir));
    doReturn(ids).when(sm).getSnapshottableRoot(any());
    doReturn(maxSnapID).when(sm).getMaxSnapshotID();

    // Create testMaxSnapshotLimit snapshots. These should all succeed.
    //
    for (Integer i = 0; i < maxSnapshotLimit; ++i) {
      sm.createSnapshot(leaseManager, iip, "dummy", i.toString(), Time.now());
    }

    // Attempt to create one more snapshot. This should fail due to snapshot
    // ID rollover.
    //
    try {
      sm.createSnapshot(leaseManager, iip, "dummy", "shouldFailSnapshot",
          Time.now());
      Assert.fail("Expected SnapshotException not thrown");
    } catch (SnapshotException se) {
      Assert.assertTrue(
          StringUtils.toLowerCase(se.getMessage()).contains(errMsg));
    }

    // Delete a snapshot to free up a slot.
    //
    sm.deleteSnapshot(iip, "", mock(INode.ReclaimContext.class), Time.now());

    // Attempt to create a snapshot again. It should still fail due
    // to snapshot ID rollover.
    //

    try {
      sm.createSnapshot(leaseManager, iip, "dummy", "shouldFailSnapshot2",
          Time.now());
      // in case the snapshot ID limit is hit, further creation of snapshots
      // even post deletions of snapshots won't succeed
      if (maxSnapID < maxSnapshotLimit) {
        Assert.fail("CreateSnapshot should succeed");
      }
    } catch (SnapshotException se) {
      Assert.assertTrue(
          StringUtils.toLowerCase(se.getMessage()).contains(errMsg));
    }
  }
  /**
   *  Snapshot is identified by INODE CURRENT_STATE_ID.
   *  So maximum allowable snapshotID should be less than CURRENT_STATE_ID
   */
  @Test
  public void testValidateSnapshotIDWidth() throws Exception {
    FSDirectory fsdir = mock(FSDirectory.class);
    SnapshotManager snapshotManager = new SnapshotManager(new Configuration(),
        fsdir);
    Assert.assertTrue(snapshotManager.
        getMaxSnapshotID() < Snapshot.CURRENT_STATE_ID);
  }

}
