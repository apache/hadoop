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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.util.Time.now;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGetBlockLocations {
  private static final String FILE_NAME = "foo";
  private static final String FILE_PATH = "/" + FILE_NAME;
  private static final long MOCK_INODE_ID = 16386;
  private static final String RESERVED_PATH =
      "/.reserved/.inodes/" + MOCK_INODE_ID;

  @Test(timeout = 30000)
  public void testResolveReservedPath() throws IOException {
    FSNamesystem fsn = setupFileSystem();
    FSEditLog editlog = fsn.getEditLog();
    fsn.getBlockLocations("dummy", RESERVED_PATH, 0, 1024);
    verify(editlog).logTimes(eq(FILE_PATH), anyLong(), anyLong());
    fsn.close();
  }

  @Test(timeout = 30000)
  public void testGetBlockLocationsRacingWithDelete() throws IOException {
    FSNamesystem fsn = spy(setupFileSystem());
    final FSDirectory fsd = fsn.getFSDirectory();
    FSEditLog editlog = fsn.getEditLog();

    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        INodesInPath iip = fsd.getINodesInPath(FILE_PATH, DirOp.READ);
        FSDirDeleteOp.delete(fsd, iip, new INode.BlocksMapUpdateInfo(),
                             new ArrayList<INode>(), new ArrayList<Long>(),
                             now());
        invocation.callRealMethod();
        return null;
      }
    }).when(fsn).writeLock();
    fsn.getBlockLocations("dummy", RESERVED_PATH, 0, 1024);

    verify(editlog, never()).logTimes(anyString(), anyLong(), anyLong());
    fsn.close();
  }

  @Test(timeout = 30000)
  public void testGetBlockLocationsRacingWithRename() throws IOException {
    FSNamesystem fsn = spy(setupFileSystem());
    final FSDirectory fsd = fsn.getFSDirectory();
    FSEditLog editlog = fsn.getEditLog();
    final String DST_PATH = "/bar";
    final boolean[] renamed = new boolean[1];

    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        invocation.callRealMethod();
        if (!renamed[0]) {
          FSDirRenameOp.renameTo(fsd, fsd.getPermissionChecker(), FILE_PATH,
                                 DST_PATH, new INode.BlocksMapUpdateInfo(),
                                 false);
          renamed[0] = true;
        }
        return null;
      }
    }).when(fsn).writeLock();
    fsn.getBlockLocations("dummy", RESERVED_PATH, 0, 1024);

    verify(editlog).logTimes(eq(DST_PATH), anyLong(), anyLong());
    fsn.close();
  }

  private static FSNamesystem setupFileSystem() throws IOException {
    Configuration conf = new Configuration();
    conf.setLong(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 1L);
    FSEditLog editlog = mock(FSEditLog.class);
    FSImage image = mock(FSImage.class);
    when(image.getEditLog()).thenReturn(editlog);
    final FSNamesystem fsn = new FSNamesystem(conf, image, true);

    final FSDirectory fsd = fsn.getFSDirectory();
    INodesInPath iip = fsd.getINodesInPath("/", DirOp.READ);
    PermissionStatus perm = new PermissionStatus(
        "hdfs", "supergroup",
        FsPermission.createImmutable((short) 0x1ff));
    final INodeFile file = new INodeFile(
        MOCK_INODE_ID, FILE_NAME.getBytes(StandardCharsets.UTF_8),
        perm, 1, 1, new BlockInfo[] {}, (short) 1,
        DFS_BLOCK_SIZE_DEFAULT);
    fsn.getFSDirectory().addINode(iip, file, null);
    return fsn;
  }

}
