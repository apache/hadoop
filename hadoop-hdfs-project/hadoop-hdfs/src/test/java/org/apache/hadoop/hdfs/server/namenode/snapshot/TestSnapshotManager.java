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

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;

import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.junit.Assert;
import org.junit.Test;


/**
 * Testing snapshot manager functionality.
 */
public class TestSnapshotManager {
  private static final int testMaxSnapshotLimit = 7;

  /**
   * Test that the global limit on snapshots is honored.
   */
  @Test (timeout=10000)
  public void testSnapshotLimits() throws Exception {
    // Setup mock objects for SnapshotManager.createSnapshot.
    //
    INodeDirectory ids = mock(INodeDirectory.class);
    FSDirectory fsdir = mock(FSDirectory.class);

    SnapshotManager sm = spy(new SnapshotManager(fsdir));
    doReturn(ids).when(sm).getSnapshottableRoot(anyString());
    doReturn(testMaxSnapshotLimit).when(sm).getMaxSnapshotID();

    // Create testMaxSnapshotLimit snapshots. These should all succeed.
    //
    for (Integer i = 0; i < testMaxSnapshotLimit; ++i) {
      sm.createSnapshot("dummy", i.toString());
    }

    // Attempt to create one more snapshot. This should fail due to snapshot
    // ID rollover.
    //
    try {
      sm.createSnapshot("dummy", "shouldFailSnapshot");
      Assert.fail("Expected SnapshotException not thrown");
    } catch (SnapshotException se) {
      Assert.assertTrue(
          se.getMessage().toLowerCase().contains("rollover"));
    }

    // Delete a snapshot to free up a slot.
    //
    sm.deleteSnapshot("", "", mock(INode.BlocksMapUpdateInfo.class), new ArrayList<INode>());

    // Attempt to create a snapshot again. It should still fail due
    // to snapshot ID rollover.
    //
    try {
      sm.createSnapshot("dummy", "shouldFailSnapshot2");
      Assert.fail("Expected SnapshotException not thrown");
    } catch (SnapshotException se) {
      Assert.assertTrue(
          se.getMessage().toLowerCase().contains("rollover"));
    }
  }
}
