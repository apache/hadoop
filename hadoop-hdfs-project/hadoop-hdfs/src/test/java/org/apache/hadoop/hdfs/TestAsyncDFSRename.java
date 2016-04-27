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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAsyncDFSRename {
  final Path asyncRenameDir = new Path("/test/async_rename/");
  public static final Log LOG = LogFactory.getLog(TestAsyncDFSRename.class);
  final private static Configuration CONF = new HdfsConfiguration();

  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String USER1_NAME = "user1";
  private static final UserGroupInformation USER1;

  private MiniDFSCluster gCluster;

  static {
    // explicitly turn on permission checking
    CONF.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);

    // create fake mapping for the groups
    Map<String, String[]> u2g_map = new HashMap<String, String[]>(1);
    u2g_map.put(USER1_NAME, new String[] { GROUP1_NAME, GROUP2_NAME });
    DFSTestUtil.updateConfWithFakeGroupMapping(CONF, u2g_map);

    // Initiate all four users
    USER1 = UserGroupInformation.createUserForTesting(USER1_NAME, new String[] {
        GROUP1_NAME, GROUP2_NAME });
  }

  @Before
  public void setUp() throws IOException {
    gCluster = new MiniDFSCluster.Builder(CONF).numDataNodes(3).build();
    gCluster.waitActive();
  }

  @After
  public void tearDown() throws IOException {
    if (gCluster != null) {
      gCluster.shutdown();
      gCluster = null;
    }
  }

  static int countLease(MiniDFSCluster cluster) {
    return TestDFSRename.countLease(cluster);
  }

  void list(DistributedFileSystem dfs, String name) throws IOException {
    FileSystem.LOG.info("\n\n" + name);
    for (FileStatus s : dfs.listStatus(asyncRenameDir)) {
      FileSystem.LOG.info("" + s.getPath());
    }
  }

  static void createFile(DistributedFileSystem dfs, Path f) throws IOException {
    DataOutputStream a_out = dfs.create(f);
    a_out.writeBytes("something");
    a_out.close();
  }

  /**
   * Check the blocks of dst file are cleaned after rename with overwrite
   * Restart NN to check the rename successfully
   */
  @Test
  public void testAsyncRenameWithOverwrite() throws Exception {
    final short replFactor = 2;
    final long blockSize = 512;
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
        replFactor).build();
    cluster.waitActive();
    DistributedFileSystem dfs = cluster.getFileSystem();
    AsyncDistributedFileSystem adfs = dfs.getAsyncDistributedFileSystem();

    try {

      long fileLen = blockSize * 3;
      String src = "/foo/src";
      String dst = "/foo/dst";
      String src2 = "/foo/src2";
      String dst2 = "/foo/dst2";
      Path srcPath = new Path(src);
      Path dstPath = new Path(dst);
      Path srcPath2 = new Path(src2);
      Path dstPath2 = new Path(dst2);

      DFSTestUtil.createFile(dfs, srcPath, fileLen, replFactor, 1);
      DFSTestUtil.createFile(dfs, dstPath, fileLen, replFactor, 1);
      DFSTestUtil.createFile(dfs, srcPath2, fileLen, replFactor, 1);
      DFSTestUtil.createFile(dfs, dstPath2, fileLen, replFactor, 1);

      LocatedBlocks lbs = NameNodeAdapter.getBlockLocations(
          cluster.getNameNode(), dst, 0, fileLen);
      LocatedBlocks lbs2 = NameNodeAdapter.getBlockLocations(
          cluster.getNameNode(), dst2, 0, fileLen);
      BlockManager bm = NameNodeAdapter.getNamesystem(cluster.getNameNode())
          .getBlockManager();
      assertTrue(bm.getStoredBlock(lbs.getLocatedBlocks().get(0).getBlock()
          .getLocalBlock()) != null);
      assertTrue(bm.getStoredBlock(lbs2.getLocatedBlocks().get(0).getBlock()
          .getLocalBlock()) != null);

      Future<Void> retVal1 = adfs.rename(srcPath, dstPath, Rename.OVERWRITE);
      Future<Void> retVal2 = adfs.rename(srcPath2, dstPath2, Rename.OVERWRITE);
      retVal1.get();
      retVal2.get();

      assertTrue(bm.getStoredBlock(lbs.getLocatedBlocks().get(0).getBlock()
          .getLocalBlock()) == null);
      assertTrue(bm.getStoredBlock(lbs2.getLocatedBlocks().get(0).getBlock()
          .getLocalBlock()) == null);

      // Restart NN and check the rename successfully
      cluster.restartNameNodes();
      assertFalse(dfs.exists(srcPath));
      assertTrue(dfs.exists(dstPath));
      assertFalse(dfs.exists(srcPath2));
      assertTrue(dfs.exists(dstPath2));
    } finally {
      if (dfs != null) {
        dfs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testConcurrentAsyncRenameWithOverwrite() throws Exception {
    final short replFactor = 2;
    final long blockSize = 512;
    final Path renameDir = new Path(
        "/test/concurrent_reanme_with_overwrite_dir/");
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .build();
    cluster.waitActive();
    DistributedFileSystem dfs = cluster.getFileSystem();
    AsyncDistributedFileSystem adfs = dfs.getAsyncDistributedFileSystem();
    int count = 1000;

    try {
      long fileLen = blockSize * 3;
      assertTrue(dfs.mkdirs(renameDir));

      Map<Integer, Future<Void>> returnFutures = new HashMap<Integer, Future<Void>>();

      // concurrently invoking many rename
      for (int i = 0; i < count; i++) {
        Path src = new Path(renameDir, "src" + i);
        Path dst = new Path(renameDir, "dst" + i);
        DFSTestUtil.createFile(dfs, src, fileLen, replFactor, 1);
        DFSTestUtil.createFile(dfs, dst, fileLen, replFactor, 1);
        Future<Void> returnFuture = adfs.rename(src, dst, Rename.OVERWRITE);
        returnFutures.put(i, returnFuture);
      }

      // wait for completing the calls
      for (int i = 0; i < count; i++) {
        returnFutures.get(i).get();
      }

      // Restart NN and check the rename successfully
      cluster.restartNameNodes();

      // very the src dir should not exist, dst should
      for (int i = 0; i < count; i++) {
        Path src = new Path(renameDir, "src" + i);
        Path dst = new Path(renameDir, "dst" + i);
        assertFalse(dfs.exists(src));
        assertTrue(dfs.exists(dst));
      }
    } finally {
      dfs.delete(renameDir, true);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testAsyncRenameWithException() throws Exception {
    FileSystem rootFs = FileSystem.get(CONF);
    final Path renameDir = new Path("/test/async_rename_exception/");
    final Path src = new Path(renameDir, "src");
    final Path dst = new Path(renameDir, "dst");
    rootFs.mkdirs(src);

    AsyncDistributedFileSystem adfs = USER1
        .doAs(new PrivilegedExceptionAction<AsyncDistributedFileSystem>() {
          @Override
          public AsyncDistributedFileSystem run() throws Exception {
            return gCluster.getFileSystem().getAsyncDistributedFileSystem();
          }
        });

    try {
      Future<Void> returnFuture = adfs.rename(src, dst, Rename.OVERWRITE);
      returnFuture.get();
    } catch (ExecutionException e) {
      checkPermissionDenied(e, src);
    }
  }

  private void checkPermissionDenied(final Exception e, final Path dir) {
    assertTrue(e.getCause() instanceof ExecutionException);
    assertTrue("Permission denied messages must carry AccessControlException",
        e.getMessage().contains("AccessControlException"));
    assertTrue("Permission denied messages must carry the username", e
        .getMessage().contains(USER1_NAME));
    assertTrue("Permission denied messages must carry the path parent", e
        .getMessage().contains(dir.getParent().toUri().getPath()));
  }
}