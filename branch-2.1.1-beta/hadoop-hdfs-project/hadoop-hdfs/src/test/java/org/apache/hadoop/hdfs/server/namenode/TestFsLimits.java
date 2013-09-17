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

import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.junit.Before;
import org.junit.Test;

public class TestFsLimits {
  static Configuration conf;
  static INode[] inodes;
  static FSDirectory fs;
  static boolean fsIsReady;
  
  static PermissionStatus perms
    = new PermissionStatus("admin", "admin", FsPermission.getDefault());

  static INodeDirectoryWithQuota rootInode;

  static private FSNamesystem getMockNamesystem() {
    FSNamesystem fsn = mock(FSNamesystem.class);
    when(
        fsn.createFsOwnerPermissions((FsPermission)anyObject())
    ).thenReturn(
         new PermissionStatus("root", "wheel", FsPermission.getDefault())
    );
    return fsn;
  }
  
  private static class MockFSDirectory extends FSDirectory {
    public MockFSDirectory() throws IOException {
      super(new FSImage(conf), getMockNamesystem(), conf);
      setReady(fsIsReady);
    }
  }

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
             fileAsURI(new File(MiniDFSCluster.getBaseDirectory(),
                                "namenode")).toString());

    rootInode = new INodeDirectoryWithQuota(getMockNamesystem()
        .allocateNewInodeId(), INodeDirectory.ROOT_NAME, perms);
    inodes = new INode[]{ rootInode, null };
    fs = null;
    fsIsReady = true;
  }

  @Test
  public void testDefaultMaxComponentLength() {
    int maxComponentLength = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);
    assertEquals(0, maxComponentLength);
  }
  
  @Test
  public void testDefaultMaxDirItems() {
    int maxDirItems = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT);
    assertEquals(0, maxDirItems);
  }

  @Test
  public void testNoLimits() throws Exception {
    addChildWithName("1", null);
    addChildWithName("22", null);
    addChildWithName("333", null);
    addChildWithName("4444", null);
    addChildWithName("55555", null);
    addChildWithName(HdfsConstants.DOT_SNAPSHOT_DIR,
        HadoopIllegalArgumentException.class);
  }

  @Test
  public void testMaxComponentLength() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 2);
    
    addChildWithName("1", null);
    addChildWithName("22", null);
    addChildWithName("333", PathComponentTooLongException.class);
    addChildWithName("4444", PathComponentTooLongException.class);
  }

  @Test
  public void testMaxDirItems() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    
    addChildWithName("1", null);
    addChildWithName("22", null);
    addChildWithName("333", MaxDirectoryItemsExceededException.class);
    addChildWithName("4444", MaxDirectoryItemsExceededException.class);
  }

  @Test
  public void testMaxComponentsAndMaxDirItems() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 3);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    
    addChildWithName("1", null);
    addChildWithName("22", null);
    addChildWithName("333", MaxDirectoryItemsExceededException.class);
    addChildWithName("4444", PathComponentTooLongException.class);
  }

  @Test
  public void testDuringEditLogs() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 3);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    fsIsReady = false;
    
    addChildWithName(HdfsConstants.DOT_SNAPSHOT_DIR,
        HadoopIllegalArgumentException.class);
    addChildWithName("1", null);
    addChildWithName("22", null);
    addChildWithName("333", null);
    addChildWithName("4444", null);
  }

  private void addChildWithName(String name, Class<?> expected)
  throws Exception {
    // have to create after the caller has had a chance to set conf values
    if (fs == null) fs = new MockFSDirectory();

    INode child = new INodeDirectory(getMockNamesystem().allocateNewInodeId(),
        DFSUtil.string2Bytes(name), perms, 0L);
    
    Class<?> generated = null;
    try {
      fs.verifyMaxComponentLength(child.getLocalNameBytes(), inodes, 1);
      fs.verifyMaxDirItems(inodes, 1);
      fs.verifyINodeName(child.getLocalNameBytes());

      rootInode.addChild(child);
    } catch (Throwable e) {
      generated = e.getClass();
    }
    assertEquals(expected, generated);
  }
}
