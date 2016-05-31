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

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.MASK;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.NONE;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.TestDFSPermission.PermissionGenerator;
import org.apache.hadoop.hdfs.server.namenode.AclTestHelpers;
import org.apache.hadoop.hdfs.server.namenode.FSAclBaseTest;
import org.apache.hadoop.ipc.AsyncCallLimitExceededException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for asynchronous distributed filesystem.
 * */
public class TestAsyncDFS {
  public static final Log LOG = LogFactory.getLog(TestAsyncDFS.class);
  private final short replFactor = 1;
  private final long blockSize = 512;
  private long fileLen = 0;
  private final long seed = Time.now();
  private final Random r = new Random(seed);
  private final PermissionGenerator permGenerator = new PermissionGenerator(r);
  private static final int NUM_TESTS = 50;
  private static final int NUM_NN_HANDLER = 10;
  private static final int ASYNC_CALL_LIMIT = 1000;

  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private AsyncDistributedFileSystem adfs;

  @Before
  public void setup() throws IOException {
    conf = new HdfsConfiguration();
    // explicitly turn on acl
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    // explicitly turn on permission checking
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
    // set the limit of max async calls
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_KEY,
        ASYNC_CALL_LIMIT);
    // set server handlers
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY, NUM_NN_HANDLER);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    adfs = fs.getAsyncDistributedFileSystem();
  }

  @After
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout=60000)
  public void testBatchAsyncAcl() throws Exception {
    final String basePath = "testBatchAsyncAcl";
    final Path parent = new Path(String.format("/test/%s/", basePath));

    // prepare test
    final Path[] paths = new Path[NUM_TESTS];
    for (int i = 0; i < NUM_TESTS; i++) {
      paths[i] = new Path(parent, "acl" + i);
      FileSystem.mkdirs(fs, paths[i],
          FsPermission.createImmutable((short) 0750));
      assertTrue(fs.exists(paths[i]));
      assertTrue(fs.getFileStatus(paths[i]).isDirectory());
    }

    final List<AclEntry> aclSpec = getAclSpec();
    final AclEntry[] expectedAclSpec = getExpectedAclSpec();
    Map<Integer, Future<Void>> setAclRetFutures =
        new HashMap<Integer, Future<Void>>();
    Map<Integer, Future<AclStatus>> getAclRetFutures =
        new HashMap<Integer, Future<AclStatus>>();
    int start = 0, end = 0;
    try {
      // test setAcl
      for (int i = 0; i < NUM_TESTS; i++) {
        for (;;) {
          try {
            Future<Void> retFuture = adfs.setAcl(paths[i], aclSpec);
            setAclRetFutures.put(i, retFuture);
            break;
          } catch (AsyncCallLimitExceededException e) {
            start = end;
            end = i;
            waitForAclReturnValues(setAclRetFutures, start, end);
          }
        }
      }
      waitForAclReturnValues(setAclRetFutures, end, NUM_TESTS);

      // test getAclStatus
      start = 0;
      end = 0;
      for (int i = 0; i < NUM_TESTS; i++) {
        for (;;) {
          try {
            Future<AclStatus> retFuture = adfs.getAclStatus(paths[i]);
            getAclRetFutures.put(i, retFuture);
            break;
          } catch (AsyncCallLimitExceededException e) {
            start = end;
            end = i;
            waitForAclReturnValues(getAclRetFutures, start, end, paths,
                expectedAclSpec);
          }
        }
      }
      waitForAclReturnValues(getAclRetFutures, end, NUM_TESTS, paths,
          expectedAclSpec);
    } catch (Exception e) {
      throw e;
    }
  }

  static void waitForReturnValues(final Map<Integer, Future<Void>> retFutures,
      final int start, final int end)
      throws InterruptedException, ExecutionException {
    LOG.info(String.format("calling waitForReturnValues [%d, %d)", start, end));
    for (int i = start; i < end; i++) {
      LOG.info("calling Future#get #" + i);
      retFutures.get(i).get();
    }
  }

  private void waitForAclReturnValues(
      final Map<Integer, Future<Void>> aclRetFutures, final int start,
      final int end) throws InterruptedException, ExecutionException {
    for (int i = start; i < end; i++) {
      aclRetFutures.get(i).get();
    }
  }

  private void waitForAclReturnValues(
      final Map<Integer, Future<AclStatus>> aclRetFutures, final int start,
      final int end, final Path[] paths, final AclEntry[] expectedAclSpec)
      throws InterruptedException, ExecutionException, IOException {
    for (int i = start; i < end; i++) {
      AclStatus aclStatus = aclRetFutures.get(i).get();
      verifyGetAcl(aclStatus, expectedAclSpec, paths[i]);
    }
  }

  private void verifyGetAcl(final AclStatus aclStatus,
      final AclEntry[] expectedAclSpec, final Path path) throws IOException {
    if (aclStatus == null) {
      return;
    }

    // verify permission and acl
    AclEntry[] returned = aclStatus.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expectedAclSpec, returned);
    assertPermission(path, (short) 010770);
    FSAclBaseTest.assertAclFeature(cluster, path, true);
  }

  private List<AclEntry> getAclSpec() {
    return Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "foo", ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, "foo", ALL));
  }

  private AclEntry[] getExpectedAclSpec() {
    return new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, "foo", ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, NONE) };
  }

  private void assertPermission(final Path pathToCheck, final short perm)
      throws IOException {
    AclTestHelpers.assertPermission(fs, pathToCheck, perm);
  }

  @Test(timeout=60000)
  public void testAsyncAPIWithException() throws Exception {
    String group1 = "group1";
    String group2 = "group2";
    String user1 = "user1";
    UserGroupInformation ugi1;

    // create fake mapping for the groups
    Map<String, String[]> u2gMap = new HashMap<String, String[]>(1);
    u2gMap.put(user1, new String[] {group1, group2});
    DFSTestUtil.updateConfWithFakeGroupMapping(conf, u2gMap);

    // Initiate all four users
    ugi1 = UserGroupInformation.createUserForTesting(user1, new String[] {
        group1, group2 });

    final Path parent = new Path("/test/async_api_exception/");
    final Path aclDir = new Path(parent, "aclDir");
    final Path src = new Path(parent, "src");
    final Path dst = new Path(parent, "dst");
    fs.mkdirs(aclDir, FsPermission.createImmutable((short) 0700));
    fs.mkdirs(src);

    AsyncDistributedFileSystem adfs1 = ugi1
        .doAs(new PrivilegedExceptionAction<AsyncDistributedFileSystem>() {
          @Override
          public AsyncDistributedFileSystem run() throws Exception {
            return cluster.getFileSystem().getAsyncDistributedFileSystem();
          }
        });

    Future<Void> retFuture;
    // test rename
    try {
      retFuture = adfs1.rename(src, dst, Rename.OVERWRITE);
      retFuture.get();
    } catch (ExecutionException e) {
      checkPermissionDenied(e, src, user1);
      assertTrue("Permission denied messages must carry the path parent", e
          .getMessage().contains(src.getParent().toUri().getPath()));
    }

    // test setPermission
    FsPermission fsPerm = new FsPermission(permGenerator.next());
    try {
      retFuture = adfs1.setPermission(src, fsPerm);
      retFuture.get();
    } catch (ExecutionException e) {
      checkPermissionDenied(e, src, user1);
    }

    // test setOwner
    try {
      retFuture = adfs1.setOwner(src, "user1", "group2");
      retFuture.get();
    } catch (ExecutionException e) {
      checkPermissionDenied(e, src, user1);
    }

    // test setAcl
    try {
      retFuture = adfs1.setAcl(aclDir,
          Lists.newArrayList(aclEntry(ACCESS, USER, ALL)));
      retFuture.get();
      fail("setAcl should fail with permission denied");
    } catch (ExecutionException e) {
      checkPermissionDenied(e, aclDir, user1);
    }

    // test getAclStatus
    try {
      Future<AclStatus> aclRetFuture = adfs1.getAclStatus(aclDir);
      aclRetFuture.get();
      fail("getAclStatus should fail with permission denied");
    } catch (ExecutionException e) {
      checkPermissionDenied(e, aclDir, user1);
    }
  }

  public static void checkPermissionDenied(final Exception e, final Path dir,
      final String user) {
    assertTrue(e.getCause() instanceof RemoteException);
    assertTrue("Permission denied messages must carry AccessControlException",
        e.getMessage().contains("AccessControlException"));
    assertTrue("Permission denied messages must carry the username", e
        .getMessage().contains(user));
    assertTrue("Permission denied messages must carry the name of the path",
        e.getMessage().contains(dir.getName()));
  }


  @Test(timeout = 120000)
  public void testConcurrentAsyncAPI() throws Exception {
    String group1 = "group1";
    String group2 = "group2";
    String user1 = "user1";

    // create fake mapping for the groups
    Map<String, String[]> u2gMap = new HashMap<String, String[]>(1);
    u2gMap.put(user1, new String[] {group1, group2});
    DFSTestUtil.updateConfWithFakeGroupMapping(conf, u2gMap);

    // prepare for test
    final Path parent = new Path(
        String.format("/test/%s/", "testConcurrentAsyncAPI"));
    final Path[] srcs = new Path[NUM_TESTS];
    final Path[] dsts = new Path[NUM_TESTS];
    short[] permissions = new short[NUM_TESTS];
    for (int i = 0; i < NUM_TESTS; i++) {
      srcs[i] = new Path(parent, "src" + i);
      dsts[i] = new Path(parent, "dst" + i);
      DFSTestUtil.createFile(fs, srcs[i], fileLen, replFactor, 1);
      DFSTestUtil.createFile(fs, dsts[i], fileLen, replFactor, 1);
      assertTrue(fs.exists(srcs[i]));
      assertTrue(fs.getFileStatus(srcs[i]).isFile());
      assertTrue(fs.exists(dsts[i]));
      assertTrue(fs.getFileStatus(dsts[i]).isFile());
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
    for (int i = 0; i < NUM_TESTS; i++) {
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
    waitForAclReturnValues(renameRetFutures, end, NUM_TESTS);

    // verify the src should not exist, dst should
    for (int i = 0; i < NUM_TESTS; i++) {
      assertFalse(fs.exists(srcs[i]));
      assertTrue(fs.exists(dsts[i]));
    }

    // test permissions
    for (int i = 0; i < NUM_TESTS; i++) {
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
    waitForAclReturnValues(permRetFutures, end, NUM_TESTS);

    // verify the permission
    for (int i = 0; i < NUM_TESTS; i++) {
      assertTrue(fs.exists(dsts[i]));
      FsPermission fsPerm = new FsPermission(permissions[i]);
      fs.access(dsts[i], fsPerm.getUserAction());
    }

    // test setOwner
    start = 0;
    end = 0;
    for (int i = 0; i < NUM_TESTS; i++) {
      for (;;) {
        try {
          Future<Void> retFuture = adfs.setOwner(dsts[i], "user1", "group2");
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
    waitForAclReturnValues(ownerRetFutures, end, NUM_TESTS);

    // verify the owner
    for (int i = 0; i < NUM_TESTS; i++) {
      assertTrue(fs.exists(dsts[i]));
      assertTrue("user1".equals(fs.getFileStatus(dsts[i]).getOwner()));
      assertTrue("group2".equals(fs.getFileStatus(dsts[i]).getGroup()));
    }
  }

  @Test
  public void testAsyncWithoutRetry() throws Exception {
    TestAsyncHDFSWithHA.runTestAsyncWithoutRetry(conf, cluster, fs);
  }
}
