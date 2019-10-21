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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Validate FSTreeWalk specific behavior.
 */
public class TestFSTreeWalk {
  /**
   * Verify that the ACLs are fetched when configured.
   */
  @Test
  public void testImportAcl() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_PROVIDED_ACLS_IMPORT_ENABLED, true);

    FileSystem fs = mock(FileSystem.class);
    Path root = mock(Path.class);
    when(root.getFileSystem(conf)).thenReturn(fs);

    Map<Path, FileStatus> expectedChildren = new HashMap<>();
    FileStatus child1 = new FileStatus(0, true, 0, 0, 1, new Path("/a"));
    FileStatus child2 = new FileStatus(0, true, 0, 0, 1, new Path("/b"));
    expectedChildren.put(child1.getPath(), child1);
    expectedChildren.put(child2.getPath(), child2);
    when(fs.listStatus(root))
        .thenReturn(expectedChildren.values().toArray(new FileStatus[1]));

    AclStatus expectedAcls = mock(AclStatus.class);
    when(fs.getAclStatus(any(Path.class))).thenReturn(expectedAcls);

    FSTreeWalk fsTreeWalk = new FSTreeWalk(root, conf);

    FileStatus rootFileStatus = new FileStatus(0, true, 0, 0, 1, root);
    TreePath treePath = new TreePath(rootFileStatus, 1, null);

    Iterable<TreePath> result = fsTreeWalk.getChildren(treePath, 1, null);
    for (TreePath path : result) {
      FileStatus expectedChildStatus
          = expectedChildren.remove(path.getFileStatus().getPath());
      assertNotNull(expectedChildStatus);

      AclStatus childAcl = path.getAclStatus();
      assertEquals(expectedAcls, childAcl);
    }

    assertEquals(0, expectedChildren.size());
  }

  /**
   * Verify ACL enabled TreeWalk iterator throws an error if the external file
   * system does not support ACLs.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testACLNotSupported() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_PROVIDED_ACLS_IMPORT_ENABLED, true);

    FileSystem fs = mock(FileSystem.class);
    when(fs.getAclStatus(any())).thenThrow(new UnsupportedOperationException());
    Path root = mock(Path.class);
    when(root.getFileSystem(conf)).thenReturn(fs);
    FileStatus rootFileStatus = new FileStatus(0, true, 0, 0, 1, root);
    when(fs.getFileStatus(root)).thenReturn(rootFileStatus);

    FSTreeWalk fsTreeWalk = new FSTreeWalk(root, conf);
    TreeWalk.TreeIterator iter = fsTreeWalk.iterator();
    fail("Unexpected successful creation of iter: " + iter);
  }

  /**
   * Verify creation of INode for ACL enabled TreePath throws an error.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testToINodeACLNotSupported() throws Exception {
    BlockResolver blockResolver = new FixedBlockResolver();
    Path root = new Path("/");
    FileStatus rootFileStatus = new FileStatus(0, false, 0, 0, 1, root);

    AclStatus acls = mock(AclStatus.class);
    TreePath treePath = new TreePath(rootFileStatus, 1, null, null, acls);

    UGIResolver ugiResolver = mock(UGIResolver.class);
    when(ugiResolver.getPermissionsProto(null, acls)).thenReturn(1L);
    treePath.toINode(ugiResolver, blockResolver, null);
  }
}
