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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;

import static org.mockito.Mockito.*;

public class TestLeaseManager {
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
    synchronized (lm) {
      lm.removeLeases(ids);
    }
    assertEquals(0, lm.getINodeIdWithLeases().size());
  }

  /** Check that even if LeaseManager.checkLease is not able to relinquish
   * leases, the Namenode does't enter an infinite loop while holding the FSN
   * write lock and thus become unresponsive
   */
  @Test (timeout=1000)
  public void testCheckLeaseNotInfiniteLoop() {
    FSDirectory dir = Mockito.mock(FSDirectory.class);
    FSNamesystem fsn = Mockito.mock(FSNamesystem.class);
    Mockito.when(fsn.isRunning()).thenReturn(true);
    Mockito.when(fsn.hasWriteLock()).thenReturn(true);
    Mockito.when(fsn.getFSDirectory()).thenReturn(dir);
    LeaseManager lm = new LeaseManager(fsn);

    //Make sure the leases we are going to add exceed the hard limit
    lm.setLeasePeriod(0, 0);

    //Add some leases to the LeaseManager
    lm.addLease("holder1", INodeId.ROOT_INODE_ID + 1);
    lm.addLease("holder2", INodeId.ROOT_INODE_ID + 2);
    lm.addLease("holder3", INodeId.ROOT_INODE_ID + 3);
    assertEquals(lm.countLease(), 3);

    //Initiate a call to checkLease. This should exit within the test timeout
    lm.checkLeases();
  }
}
