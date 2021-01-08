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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Test {@link FSDirAttrOp}.
 */
public class TestFSDirAttrOp {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestFSDirAttrOp.class);

  private boolean unprotectedSetTimes(long atime, long atime0, long precision,
      long mtime, boolean force) throws QuotaExceededException {
    FSNamesystem fsn = Mockito.mock(FSNamesystem.class);
    SnapshotManager ssMgr = Mockito.mock(SnapshotManager.class);
    FSDirectory fsd = Mockito.mock(FSDirectory.class);
    INodesInPath iip = Mockito.mock(INodesInPath.class);
    INode inode = Mockito.mock(INode.class);

    when(fsd.getFSNamesystem()).thenReturn(fsn);
    when(fsn.getSnapshotManager()).thenReturn(ssMgr);
    when(ssMgr.getSkipCaptureAccessTimeOnlyChange()).thenReturn(false);
    when(fsd.getAccessTimePrecision()).thenReturn(precision);
    when(fsd.hasWriteLock()).thenReturn(Boolean.TRUE);
    when(iip.getLastINode()).thenReturn(inode);
    when(iip.getLatestSnapshotId()).thenReturn(Mockito.anyInt());
    when(inode.getAccessTime()).thenReturn(atime0);

    return FSDirAttrOp.unprotectedSetTimes(fsd, iip, mtime, atime, force);
  }

  private boolean unprotectedSetAttributes(short currPerm, short newPerm)
      throws Exception {
    return unprotectedSetAttributes(currPerm, newPerm, "user1", "user1",
        false);
  }

  private boolean unprotectedSetAttributes(short currPerm, short newPerm,
      String currUser, String newUser, boolean testChangeOwner)
      throws Exception {
    String groupName = "testGroup";
    FsPermission originalPerm = new FsPermission(currPerm);
    FsPermission updatedPerm = new FsPermission(newPerm);
    FSNamesystem fsn = Mockito.mock(FSNamesystem.class);
    SnapshotManager ssMgr = Mockito.mock(SnapshotManager.class);
    FSDirectory fsd = Mockito.mock(FSDirectory.class);
    INodesInPath iip = Mockito.mock(INodesInPath.class);
    when(fsd.getFSNamesystem()).thenReturn(fsn);
    when(fsn.getSnapshotManager()).thenReturn(ssMgr);
    when(ssMgr.getSkipCaptureAccessTimeOnlyChange()).thenReturn(false);
    when(fsd.getAccessTimePrecision()).thenReturn(1000L);
    when(fsd.hasWriteLock()).thenReturn(Boolean.TRUE);
    when(iip.getLatestSnapshotId()).thenReturn(0);
    INode inode = new INodeDirectory(1000, DFSUtil.string2Bytes(""),
        new PermissionStatus(currUser, "testGroup", originalPerm), 0L);
    when(iip.getLastINode()).thenReturn(inode);
    return testChangeOwner ? FSDirAttrOp.unprotectedSetOwner(fsd, iip, newUser,
        groupName) : FSDirAttrOp.unprotectedSetPermission(fsd, iip,
        updatedPerm);
  }

  @Test
  public void testUnprotectedSetPermissions() throws Exception {
    assertTrue("setPermissions return true for updated permissions",
        unprotectedSetAttributes((short) 0777, (short) 0));
    assertFalse("setPermissions should return false for same permissions",
        unprotectedSetAttributes((short) 0777, (short) 0777));
  }

  @Test
  public void testUnprotectedSetOwner() throws Exception {
    assertTrue("SetOwner should return true for a new user",
        unprotectedSetAttributes((short) 0777, (short) 0777, "user1",
            "user2", true));
    assertFalse("SetOwner should return false for same user",
        unprotectedSetAttributes((short) 0777, (short) 0777, "user1",
            "user1", true));
  }

  @Test
  public void testUnprotectedSetTimes() throws Exception {
    // atime < access time + precision
    assertFalse("SetTimes should not update access time "
          + "because it's within the last precision interval",
        unprotectedSetTimes(100, 0, 1000, -1, false));

    // atime = access time + precision
    assertFalse("SetTimes should not update access time "
          + "because it's within the last precision interval",
        unprotectedSetTimes(1000, 0, 1000, -1, false));

    // atime > access time + precision
    assertTrue("SetTimes should update access time",
        unprotectedSetTimes(1011, 10, 1000, -1, false));

    // atime < access time + precision, but force is set
    assertTrue("SetTimes should update access time",
        unprotectedSetTimes(100, 0, 1000, -1, true));

    // atime < access time + precision, but mtime is set
    assertTrue("SetTimes should update access time",
        unprotectedSetTimes(100, 0, 1000, 1, false));
  }
}
