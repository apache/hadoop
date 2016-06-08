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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class TestLeaseManager {
  @Rule
  public Timeout timeout = new Timeout(300000);

  public static long maxLockHoldToReleaseLeaseMs = 100;

  @Test
  public void testRemoveLeases() throws Exception {
    FSNamesystem fsn = mock(FSNamesystem.class);
    LeaseManager lm = new LeaseManager(fsn);
    ArrayList<Long> ids = Lists.newArrayList(INodeId.ROOT_INODE_ID + 1,
            INodeId.ROOT_INODE_ID + 2, INodeId.ROOT_INODE_ID + 3,
            INodeId.ROOT_INODE_ID + 4);
    for (long id : ids) {
      lm.addLease("foo", id);
    }

    assertEquals(4, lm.getINodeIdWithLeases().size());
    for (long id : ids) {
      lm.removeLease(id);
    }
    assertEquals(0, lm.getINodeIdWithLeases().size());
  }

  /** Check that LeaseManager.checkLease release some leases
   */
  @Test
  public void testCheckLease() {
    LeaseManager lm = new LeaseManager(makeMockFsNameSystem());

    long numLease = 100;

    //Make sure the leases we are going to add exceed the hard limit
    lm.setLeasePeriod(0, 0);

    for (long i = 0; i <= numLease - 1; i++) {
      //Add some leases to the LeaseManager
      lm.addLease("holder"+i, INodeId.ROOT_INODE_ID + i);
    }
    assertEquals(numLease, lm.countLease());

    //Initiate a call to checkLease. This should exit within the test timeout
    lm.checkLeases();
    assertTrue(lm.countLease() < numLease);
  }

  @Test
  public void testCountPath() {
    LeaseManager lm = new LeaseManager(makeMockFsNameSystem());

    lm.addLease("holder1", 1);
    assertThat(lm.countPath(), is(1L));

    lm.addLease("holder2", 2);
    assertThat(lm.countPath(), is(2L));
    lm.addLease("holder2", 2);                   // Duplicate addition
    assertThat(lm.countPath(), is(2L));

    assertThat(lm.countPath(), is(2L));

    // Remove a couple of non-existing leases. countPath should not change.
    lm.removeLease("holder2", stubInodeFile(3));
    lm.removeLease("InvalidLeaseHolder", stubInodeFile(1));
    assertThat(lm.countPath(), is(2L));

    INodeFile file = stubInodeFile(1);
    lm.reassignLease(lm.getLease(file), file, "holder2");
    assertThat(lm.countPath(), is(2L));          // Count unchanged on reassign

    lm.removeLease("holder2", stubInodeFile(2)); // Remove existing
    assertThat(lm.countPath(), is(1L));
  }

  private static FSNamesystem makeMockFsNameSystem() {
    FSDirectory dir = mock(FSDirectory.class);
    FSNamesystem fsn = mock(FSNamesystem.class);
    when(fsn.isRunning()).thenReturn(true);
    when(fsn.hasWriteLock()).thenReturn(true);
    when(fsn.getFSDirectory()).thenReturn(dir);
    when(fsn.getMaxLockHoldToReleaseLeaseMs()).thenReturn(maxLockHoldToReleaseLeaseMs);
    return fsn;
  }

  private static INodeFile stubInodeFile(long inodeId) {
    PermissionStatus p = new PermissionStatus(
        "dummy", "dummy", new FsPermission((short) 0777));
    return new INodeFile(
        inodeId, "/foo".getBytes(), p, 0L, 0L,
        BlockInfo.EMPTY_ARRAY, (short) 1, 1L);
  }
}
