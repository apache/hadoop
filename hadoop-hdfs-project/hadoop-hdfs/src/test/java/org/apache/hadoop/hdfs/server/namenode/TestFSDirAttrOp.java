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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Test {@link FSDirAttrOp}.
 */
public class TestFSDirAttrOp {
  public static final Log LOG = LogFactory.getLog(TestFSDirAttrOp.class);

  private boolean unprotectedSetTimes(long atime, long atime0, long precision,
      long mtime, boolean force) throws QuotaExceededException,
      UnresolvedLinkException {
    FSDirectory fsd = Mockito.mock(FSDirectory.class);
    FSNamesystem fsn = Mockito.mock(FSNamesystem.class);
    INodesInPath iip = Mockito.mock(INodesInPath.class);
    INode inode = Mockito.mock(INode.class);

    when(fsd.getFSNamesystem()).thenReturn(fsn);
    when(fsn.getAccessTimePrecision()).thenReturn(precision);
    when(fsd.getINodesInPath("", true)).thenReturn(iip);
    when(fsd.hasWriteLock()).thenReturn(Boolean.TRUE);
    when(iip.getLastINode()).thenReturn(inode);
    when(iip.getLatestSnapshotId()).thenReturn(Mockito.anyInt());
    when(inode.getAccessTime()).thenReturn(atime0);

    return FSDirAttrOp.unprotectedSetTimes(fsd, "", mtime, atime, force);
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
