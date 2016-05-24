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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.TestDFSPermission.PermissionGenerator;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.ipc.AsyncCallLimitExceededException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.Test;

public class TestAsyncDFSRename {
  public static final Log LOG = LogFactory.getLog(TestAsyncDFSRename.class);
  private final long seed = Time.now();
  private final Random r = new Random(seed);
  private final PermissionGenerator permGenerator = new PermissionGenerator(r);
  private final short replFactor = 2;
  private final long blockSize = 512;
  private long fileLen = blockSize * 3;

  /**
   * Check the blocks of dst file are cleaned after rename with overwrite
   * Restart NN to check the rename successfully
   */
  @Test(timeout = 60000)
  public void testAsyncRenameWithOverwrite() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
        replFactor).build();
    cluster.waitActive();
    DistributedFileSystem dfs = cluster.getFileSystem();
    AsyncDistributedFileSystem adfs = dfs.getAsyncDistributedFileSystem();

    try {
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

  @Test(timeout = 60000)
  public void testCallGetReturnValueMultipleTimes() throws Exception {
    final Path renameDir = new Path(
        "/test/testCallGetReturnValueMultipleTimes/");
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_KEY, 200);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2).build();
    cluster.waitActive();
    final DistributedFileSystem dfs = cluster.getFileSystem();
    final AsyncDistributedFileSystem adfs = dfs.getAsyncDistributedFileSystem();
    final int count = 100;
    final Map<Integer, Future<Void>> returnFutures = new HashMap<Integer, Future<Void>>();

    assertTrue(dfs.mkdirs(renameDir));

    try {
      // concurrently invoking many rename
      for (int i = 0; i < count; i++) {
        Path src = new Path(renameDir, "src" + i);
        Path dst = new Path(renameDir, "dst" + i);
        DFSTestUtil.createFile(dfs, src, fileLen, replFactor, 1);
        DFSTestUtil.createFile(dfs, dst, fileLen, replFactor, 1);
        Future<Void> returnFuture = adfs.rename(src, dst, Rename.OVERWRITE);
        returnFutures.put(i, returnFuture);
      }

      for (int i = 0; i < 5; i++) {
        verifyCallGetReturnValueMultipleTimes(returnFutures, count, cluster,
            renameDir, dfs);
      }
    } finally {
      if (dfs != null) {
        dfs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void verifyCallGetReturnValueMultipleTimes(
      Map<Integer, Future<Void>> returnFutures, int count,
      MiniDFSCluster cluster, Path renameDir, DistributedFileSystem dfs)
      throws InterruptedException, ExecutionException, IOException {
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
  }

  @Test
  public void testConservativeConcurrentAsyncRenameWithOverwrite()
      throws Exception {
    internalTestConcurrentAsyncRenameWithOverwrite(100,
        "testAggressiveConcurrentAsyncRenameWithOverwrite");
  }

  @Test(timeout = 60000)
  public void testAggressiveConcurrentAsyncRenameWithOverwrite()
      throws Exception {
    internalTestConcurrentAsyncRenameWithOverwrite(10000,
        "testConservativeConcurrentAsyncRenameWithOverwrite");
  }

  private void internalTestConcurrentAsyncRenameWithOverwrite(
      final int asyncCallLimit, final String basePath) throws Exception {
    final Path renameDir = new Path(String.format("/test/%s/", basePath));
    Configuration conf = new HdfsConfiguration();
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_KEY,
        asyncCallLimit);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .build();
    cluster.waitActive();
    DistributedFileSystem dfs = cluster.getFileSystem();
    AsyncDistributedFileSystem adfs = dfs.getAsyncDistributedFileSystem();
    int count = 1000;
    int start = 0, end = 0;
    Map<Integer, Future<Void>> returnFutures = new HashMap<Integer, Future<Void>>();

    assertTrue(dfs.mkdirs(renameDir));

    try {
      // concurrently invoking many rename
      for (int i = 0; i < count; i++) {
        Path src = new Path(renameDir, "src" + i);
        Path dst = new Path(renameDir, "dst" + i);
        DFSTestUtil.createFile(dfs, src, fileLen, replFactor, 1);
        DFSTestUtil.createFile(dfs, dst, fileLen, replFactor, 1);
        for (;;) {
          try {
            LOG.info("rename #" + i);
            Future<Void> returnFuture = adfs.rename(src, dst, Rename.OVERWRITE);
            returnFutures.put(i, returnFuture);
            break;
          } catch (AsyncCallLimitExceededException e) {
            /**
             * reached limit of async calls, fetch results of finished async
             * calls to let follow-on calls go
             */
            LOG.error(e);
            start = end;
            end = i;
            LOG.info(String.format("start=%d, end=%d, i=%d", start, end, i));
            waitForReturnValues(returnFutures, start, end);
          }
        }
      }

      // wait for completing the calls
      for (int i = start; i < count; i++) {
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
      if (dfs != null) {
        dfs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void waitForReturnValues(
      final Map<Integer, Future<Void>> returnFutures, final int start,
      final int end) throws InterruptedException, ExecutionException {
    LOG.info(String.format("calling waitForReturnValues [%d, %d)", start, end));
    for (int i = start; i < end; i++) {
      LOG.info("calling Future#get #" + i);
      returnFutures.get(i).get();
    }
  }

  @Test
  public void testConservativeConcurrentAsyncAPI() throws Exception {
    internalTestConcurrentAsyncAPI(100, "testConservativeConcurrentAsyncAPI");
  }

  @Test(timeout = 60000)
  public void testAggressiveConcurrentAsyncAPI() throws Exception {
    internalTestConcurrentAsyncAPI(10000, "testAggressiveConcurrentAsyncAPI");
  }

  private void internalTestConcurrentAsyncAPI(final int asyncCallLimit,
      final String basePath) throws Exception {
    Configuration conf = new HdfsConfiguration();
    String group1 = "group1";
    String group2 = "group2";
    String user1 = "user1";
    int count = 500;

    // explicitly turn on permission checking
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
    // set the limit of max async calls
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_KEY,
        asyncCallLimit);

    // create fake mapping for the groups
    Map<String, String[]> u2gMap = new HashMap<String, String[]>(1);
    u2gMap.put(user1, new String[] {group1, group2});
    DFSTestUtil.updateConfWithFakeGroupMapping(conf, u2gMap);

    // start mini cluster
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();
    cluster.waitActive();
    AsyncDistributedFileSystem adfs = cluster.getFileSystem()
        .getAsyncDistributedFileSystem();

    // prepare for test
    FileSystem rootFs = FileSystem.get(conf);
    final Path parent = new Path(String.format("/test/%s/", basePath));
    final Path[] srcs = new Path[count];
    final Path[] dsts = new Path[count];
    short[] permissions = new short[count];
    for (int i = 0; i < count; i++) {
      srcs[i] = new Path(parent, "src" + i);
      dsts[i] = new Path(parent, "dst" + i);
      DFSTestUtil.createFile(rootFs, srcs[i], fileLen, replFactor, 1);
      DFSTestUtil.createFile(rootFs, dsts[i], fileLen, replFactor, 1);
      assertTrue(rootFs.exists(srcs[i]));
      assertTrue(rootFs.getFileStatus(srcs[i]).isFile());
      assertTrue(rootFs.exists(dsts[i]));
      assertTrue(rootFs.getFileStatus(dsts[i]).isFile());
      permissions[i] = permGenerator.next();
    }

    Map<Integer, Future<Void>> renameRetFutures =
        new HashMap<Integer, Future<Void>>();
    Map<Integer, Future<Void>> permRetFutures =
        new HashMap<Integer, Future<Void>>();
    Map<Integer, Future<Void>> ownerRetFutures =
        new HashMap<Integer, Future<Void>>();
    int start = 0, end = 0;
    // test rename
    for (int i = 0; i < count; i++) {
      for (;;) {
        try {
          Future<Void> returnFuture = adfs.rename(srcs[i], dsts[i],
              Rename.OVERWRITE);
          renameRetFutures.put(i, returnFuture);
          break;
        } catch (AsyncCallLimitExceededException e) {
          start = end;
          end = i;
          waitForReturnValues(renameRetFutures, start, end);
        }
      }
    }

    // wait for completing the calls
    for (int i = start; i < count; i++) {
      renameRetFutures.get(i).get();
    }

    // Restart NN and check the rename successfully
    cluster.restartNameNodes();

    // very the src should not exist, dst should
    for (int i = 0; i < count; i++) {
      assertFalse(rootFs.exists(srcs[i]));
      assertTrue(rootFs.exists(dsts[i]));
    }

    // test permissions
    try {
      for (int i = 0; i < count; i++) {
        for (;;) {
          try {
            Future<Void> retFuture = adfs.setPermission(dsts[i],
                new FsPermission(permissions[i]));
            permRetFutures.put(i, retFuture);
            break;
          } catch (AsyncCallLimitExceededException e) {
            start = end;
            end = i;
            waitForReturnValues(permRetFutures, start, end);
          }
        }
      }
      // wait for completing the calls
      for (int i = start; i < count; i++) {
        permRetFutures.get(i).get();
      }

      // Restart NN and check permission then
      cluster.restartNameNodes();

      // verify the permission
      for (int i = 0; i < count; i++) {
        assertTrue(rootFs.exists(dsts[i]));
        FsPermission fsPerm = new FsPermission(permissions[i]);
        checkAccessPermissions(rootFs.getFileStatus(dsts[i]),
            fsPerm.getUserAction());
      }

      // test setOwner
      start = 0;
      end = 0;
      for (int i = 0; i < count; i++) {
        for (;;) {
          try {
            Future<Void> retFuture = adfs.setOwner(dsts[i], "user1",
                "group2");
            ownerRetFutures.put(i, retFuture);
            break;
          } catch (AsyncCallLimitExceededException e) {
            start = end;
            end = i;
            waitForReturnValues(ownerRetFutures, start, end);
          }
        }
      }
      // wait for completing the calls
      for (int i = start; i < count; i++) {
        ownerRetFutures.get(i).get();
      }

      // Restart NN and check owner then
      cluster.restartNameNodes();

      // verify the owner
      for (int i = 0; i < count; i++) {
        assertTrue(rootFs.exists(dsts[i]));
        assertTrue(
            "user1".equals(rootFs.getFileStatus(dsts[i]).getOwner()));
        assertTrue(
            "group2".equals(rootFs.getFileStatus(dsts[i]).getGroup()));
      }
    } catch (AccessControlException ace) {
      throw ace;
    } finally {
      if (rootFs != null) {
        rootFs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  static void checkAccessPermissions(FileStatus stat, FsAction mode)
      throws IOException {
    checkAccessPermissions(UserGroupInformation.getCurrentUser(), stat, mode);
  }

  static void checkAccessPermissions(final UserGroupInformation ugi,
      FileStatus stat, FsAction mode) throws IOException {
    FsPermission perm = stat.getPermission();
    String user = ugi.getShortUserName();
    List<String> groups = Arrays.asList(ugi.getGroupNames());

    if (user.equals(stat.getOwner())) {
      if (perm.getUserAction().implies(mode)) {
        return;
      }
    } else if (groups.contains(stat.getGroup())) {
      if (perm.getGroupAction().implies(mode)) {
        return;
      }
    } else {
      if (perm.getOtherAction().implies(mode)) {
        return;
      }
    }
    throw new AccessControlException(String.format(
        "Permission denied: user=%s, path=\"%s\":%s:%s:%s%s", user, stat
            .getPath(), stat.getOwner(), stat.getGroup(),
        stat.isDirectory() ? "d" : "-", perm));
  }

  @Test(timeout = 60000)
  public void testAsyncAPIWithException() throws Exception {
    Configuration conf = new HdfsConfiguration();
    String group1 = "group1";
    String group2 = "group2";
    String user1 = "user1";
    UserGroupInformation ugi1;

    // explicitly turn on permission checking
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);

    // create fake mapping for the groups
    Map<String, String[]> u2gMap = new HashMap<String, String[]>(1);
    u2gMap.put(user1, new String[] {group1, group2});
    DFSTestUtil.updateConfWithFakeGroupMapping(conf, u2gMap);

    // Initiate all four users
    ugi1 = UserGroupInformation.createUserForTesting(user1, new String[] {
        group1, group2 });

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();
    cluster.waitActive();

    FileSystem rootFs = FileSystem.get(conf);
    final Path renameDir = new Path("/test/async_api_exception/");
    final Path src = new Path(renameDir, "src");
    final Path dst = new Path(renameDir, "dst");
    rootFs.mkdirs(src);

    AsyncDistributedFileSystem adfs = ugi1
        .doAs(new PrivilegedExceptionAction<AsyncDistributedFileSystem>() {
          @Override
          public AsyncDistributedFileSystem run() throws Exception {
            return cluster.getFileSystem().getAsyncDistributedFileSystem();
          }
        });

    Future<Void> retFuture;
    try {
      retFuture = adfs.rename(src, dst, Rename.OVERWRITE);
      retFuture.get();
    } catch (ExecutionException e) {
      TestAsyncDFS.checkPermissionDenied(e, src, user1);
      assertTrue("Permission denied messages must carry the path parent", e
          .getMessage().contains(src.getParent().toUri().getPath()));
    }

    FsPermission fsPerm = new FsPermission(permGenerator.next());
    try {
      retFuture = adfs.setPermission(src, fsPerm);
      retFuture.get();
    } catch (ExecutionException e) {
      TestAsyncDFS.checkPermissionDenied(e, src, user1);
      assertTrue("Permission denied messages must carry the name of the path",
          e.getMessage().contains(src.getName()));
    }

    try {
      retFuture = adfs.setOwner(src, "user1", "group2");
      retFuture.get();
    } catch (ExecutionException e) {
      TestAsyncDFS.checkPermissionDenied(e, src, user1);
      assertTrue("Permission denied messages must carry the name of the path",
          e.getMessage().contains(src.getName()));
    } finally {
      if (rootFs != null) {
        rootFs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}