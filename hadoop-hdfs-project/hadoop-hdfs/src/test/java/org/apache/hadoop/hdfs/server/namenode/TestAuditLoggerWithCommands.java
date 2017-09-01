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
import org.apache.hadoop.fs.BatchedRemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.doThrow;

public class TestAuditLoggerWithCommands {

  static final int NUM_DATA_NODES = 2;
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  private static MiniDFSCluster cluster = null;
  private static FileSystem fileSys = null;
  private static FileSystem fs2 = null;
  private static FileSystem fs = null;
  private static LogCapturer auditlog;
  static Configuration conf;
  static UserGroupInformation user1;
  static UserGroupInformation user2;
  private static NamenodeProtocols proto;

  @BeforeClass
  public static void initialize() throws Exception {
    // start a cluster
    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY,true);
    conf.setBoolean(DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).build();
    cluster.waitActive();
    user1 =
        UserGroupInformation.createUserForTesting("theDoctor",
            new String[]{"tardis"});
    user2 =
        UserGroupInformation.createUserForTesting("theEngineer",
            new String[]{"hadoop"});
    auditlog = LogCapturer.captureLogs(FSNamesystem.auditLog);
    proto = cluster.getNameNodeRpc();
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    fs2 = DFSTestUtil.getFileSystemAs(user2, conf);
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    fs.close();
    fs2.close();
    fileSys.close();
    cluster.shutdown();
  }

  @Test
  public void testGetContentSummary() throws IOException {
    Path dir1 = new Path("/dir1");
    Path dir2 = new Path("/dir2");
    String acePattern =
        ".*allowed=false.*ugi=theEngineer.*cmd=contentSummary.*";
    fs.mkdirs(dir1,new FsPermission((short)0600));
    fs.mkdirs(dir2,new FsPermission((short)0600));
    fs.setOwner(dir1, user1.getUserName(), user1.getPrimaryGroupName());
    fs.setOwner(dir2, user2.getUserName(), user2.getPrimaryGroupName());
    try {
      fs2.getContentSummary(new Path("/"));
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    int length = verifyAuditLogs(acePattern);
    try {
      fs2.getContentSummary(new Path("/dir3"));
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    assertTrue("Unexpected log from getContentSummary",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testSetQuota() throws Exception {
    Path path = new Path("/testdir/testdir1");
    fs.mkdirs(path);
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    try {
      ((DistributedFileSystem)fileSys).setQuota(path, 10l, 10l);
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    String acePattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=setQuota.*";
    int length = verifyAuditLogs(acePattern);
    fileSys.close();
    try {
      ((DistributedFileSystem)fileSys).setQuota(path, 10l, 10l);
      fail("The operation should have failed with IOException");
    } catch (IOException ace) {
    }
    assertTrue("Unexpected log from getContentSummary",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testConcat() throws Exception {
    Path file1 = new Path("/file1");
    Path file2 = new Path("/file2");
    Path targetDir = new Path("/target");
    String acePattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=concat.*";
    fs.createNewFile(file1);
    fs.createNewFile(file2);
    fs.mkdirs(targetDir);
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    try {
      fileSys.concat(targetDir, new Path[]{file1, file2});
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    int length = verifyAuditLogs(acePattern);
    fileSys.close();
    try {
      fileSys.concat(targetDir, new Path[]{file1, file2});
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    assertTrue("Unexpected log from Concat",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testCreateRenameSnapShot() throws Exception {
    Path srcDir = new Path("/src");
    String aceCreatePattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=createSnapshot.*";
    String aceRenamePattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=renameSnapshot.*";
    fs.mkdirs(srcDir);
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    cluster.getNamesystem().allowSnapshot(srcDir.toString());
    try {
      fileSys.createSnapshot(srcDir);
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    verifyAuditLogs(aceCreatePattern);
    try {
      Path s1 = fs.createSnapshot(srcDir);
      fileSys.renameSnapshot(srcDir, s1.getName(), "test");
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    int length = auditlog.getOutput().split("\n").length;
    verifyAuditLogs(aceRenamePattern);
    try {
      fs.createSnapshot(new Path("/test1"));
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    try {
      fs.renameSnapshot(new Path("/test1"), "abc", "test2");
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    assertTrue("Unexpected log!",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testDeleteSnapshot() throws Exception {
    Path srcDir = new Path("/src");
    Path s1;
    fs.mkdirs(srcDir);
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    cluster.getNamesystem().allowSnapshot(srcDir.toString());
    try {
      s1 = fs.createSnapshot(srcDir);
      fileSys.deleteSnapshot(srcDir, s1.getName());
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    String aceDeletePattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=deleteSnapshot.*";
    int length = verifyAuditLogs(aceDeletePattern);
    fileSys.close();
    try {
      s1 = fs.createSnapshot(srcDir);
      fileSys.deleteSnapshot(srcDir, s1.getName());
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    assertTrue("Unexpected log!",
        length+1 == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testAddCacheDirective() throws Exception {
    removeExistingCachePools(null);
    proto.addCachePool(new CachePoolInfo("pool1").
        setMode(new FsPermission((short) 0)));
    CacheDirectiveInfo alpha = new CacheDirectiveInfo.Builder().
        setPath(new Path("/alpha")).
        setPool("pool1").
        build();
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    try {
      ((DistributedFileSystem)fileSys).addCacheDirective(alpha);
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    String aceAddCachePattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=addCache.*";
    int length = verifyAuditLogs(aceAddCachePattern);
    try {
      fileSys.close();
      ((DistributedFileSystem)fileSys).addCacheDirective(alpha);
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    assertTrue("Unexpected log!",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testModifyCacheDirective() throws Exception {
    removeExistingCachePools(null);
    proto.addCachePool(new CachePoolInfo("pool1").
        setMode(new FsPermission((short) 0)));
    CacheDirectiveInfo alpha = new CacheDirectiveInfo.Builder().
        setPath(new Path("/alpha")).
        setPool("pool1").
        build();
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    Long id =
        ((DistributedFileSystem)fs).addCacheDirective(alpha);
    try {
      ((DistributedFileSystem)fileSys).modifyCacheDirective(
          new CacheDirectiveInfo.Builder().
              setId(id).
              setReplication((short) 1).
              build());
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    String aceModifyCachePattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=modifyCache.*";
    verifyAuditLogs(aceModifyCachePattern);
    fileSys.close();
    try {
      ((DistributedFileSystem)fileSys).modifyCacheDirective(
          new CacheDirectiveInfo.Builder().
              setId(id).
              setReplication((short) 1).
              build());
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
  }

  @Test
  public void testRemoveCacheDirective() throws Exception {
    removeExistingCachePools(null);
    proto.addCachePool(new CachePoolInfo("pool1").
        setMode(new FsPermission((short) 0)));
    CacheDirectiveInfo alpha = new CacheDirectiveInfo.Builder().
        setPath(new Path("/alpha")).
        setPool("pool1").
        build();
    String aceRemoveCachePattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=removeCache.*";
    int length = -1;
        Long id =((DistributedFileSystem)fs).addCacheDirective(alpha);
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    try {
      ((DistributedFileSystem) fileSys).removeCacheDirective(id);
      fail("It should have failed with an AccessControlException");
    } catch (AccessControlException ace) {
      length = verifyAuditLogs(aceRemoveCachePattern);
    }
    try {
      fileSys.close();
      ((DistributedFileSystem)fileSys).removeCacheDirective(id);
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    assertTrue("Unexpected log!",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testGetSnapshotDiffReport() throws Exception {
    Path snapshotDirPath = new Path("/test");
    fs.mkdirs(snapshotDirPath, new FsPermission((short) 0));
    cluster.getNamesystem().allowSnapshot(snapshotDirPath.toString());
    Path s1 = fs.createSnapshot(snapshotDirPath);
    Path s2 = fs.createSnapshot(snapshotDirPath);
    int length;
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    try {
      ((DistributedFileSystem) fileSys).getSnapshotDiffReport(snapshotDirPath,
          s1.getName(), s2.getName());
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    String aceSnapshotDiffPattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=computeSnapshotDiff.*";
    length = verifyAuditLogs(aceSnapshotDiffPattern);
    try {
      fileSys.close();
      ((DistributedFileSystem) fileSys).getSnapshotDiffReport(snapshotDirPath,
          s1.getName(), s2.getName());
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    assertTrue("Unexpected log!",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testGetQuotaUsage() throws Exception {
    Path path = new Path("/test");
    fs.mkdirs(path, new FsPermission((short) 0));
    String aceGetQuotaUsagePattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=quotaUsage.*";
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    try {
      fileSys.getQuotaUsage(path);
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    int length = verifyAuditLogs(aceGetQuotaUsagePattern);
    fileSys.close();
    try {
      fileSys.getQuotaUsage(path);
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    assertTrue("Unexpected log!",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testAddCachePool() throws Exception {
    removeExistingCachePools(null);
    CachePoolInfo cacheInfo = new CachePoolInfo("pool1").
        setMode(new FsPermission((short) 0));
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    try {
      ((DistributedFileSystem) fileSys).addCachePool(cacheInfo);
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    String aceAddCachePoolPattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=addCachePool.*";
    int length = verifyAuditLogs(aceAddCachePoolPattern);
    try {
      fileSys.close();
      ((DistributedFileSystem) fileSys).addCachePool(cacheInfo);
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    assertTrue("Unexpected log!",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testModifyCachePool() throws Exception {
    removeExistingCachePools(null);
    CachePoolInfo cacheInfo = new CachePoolInfo("pool1").
        setMode(new FsPermission((short) 0));
      ((DistributedFileSystem) fs).addCachePool(cacheInfo);
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    try {
      ((DistributedFileSystem) fileSys).modifyCachePool(cacheInfo);
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    String aceModifyCachePoolPattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=modifyCachePool.*";
    int length = verifyAuditLogs(aceModifyCachePoolPattern);
    try {
      fileSys.close();
      ((DistributedFileSystem) fileSys).modifyCachePool(cacheInfo);
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    assertTrue("Unexpected log!",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testRemoveCachePool() throws Exception {
    removeExistingCachePools(null);
    CachePoolInfo cacheInfo = new CachePoolInfo("pool1").
        setMode(new FsPermission((short) 0));
    ((DistributedFileSystem) fs).addCachePool(cacheInfo);
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    try {
      ((DistributedFileSystem) fileSys).removeCachePool("pool1");
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    String aceRemoveCachePoolPattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=removeCachePool.*";
    int length = verifyAuditLogs(aceRemoveCachePoolPattern);
    assertTrue("Unexpected log!",
        length == auditlog.getOutput().split("\n").length);
    try {
      fileSys.close();
      ((DistributedFileSystem) fileSys).removeCachePool("pool1");
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    assertTrue("Unexpected log!",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testGetEZForPath() throws Exception {
    Path path = new Path("/test");
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    fs.mkdirs(path,new FsPermission((short)0));
    String aceGetEzForPathPattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=getEZForPath.*";
    try {
      ((DistributedFileSystem) fileSys).getEZForPath(path);
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace){
    }
    int length = verifyAuditLogs(aceGetEzForPathPattern);
    fileSys.close();
    try {
      ((DistributedFileSystem) fileSys).getEZForPath(path);
      fail("The operation should have failed with IOException");
    } catch (IOException e){
    }
    assertTrue("Unexpected log!",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testRenameTo() throws Exception {
    Path path = new Path("/test");
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    fs.mkdirs(path,new FsPermission((short)0));
    String aceRenameToPattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=rename.*";
    try {
      fileSys.rename(path, path);
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    int length = verifyAuditLogs(aceRenameToPattern);
    fileSys.close();
    try {
      fileSys.rename(path, path);
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    assertTrue("Unexpected log!",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testGetXattrs() throws Exception {
    Path path = new Path("/test");
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    fs.mkdirs(path,new FsPermission((short)0));
    String aceGetXattrsPattern =
        ".*allowed=false.*ugi=theDoctor.*cmd=getXAttrs.*";
    try {
      fileSys.getXAttrs(path);
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    int length = verifyAuditLogs(aceGetXattrsPattern);
    fileSys.close();
    try {
      fileSys.getXAttrs(path);
      fail("The operation should have failed with IOException");
    } catch (IOException e) {
    }
    assertTrue("Unexpected log!",
        length == auditlog.getOutput().split("\n").length);
  }

  @Test
  public void testListXattrs() throws Exception {
    Path path = new Path("/test");
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    fs.mkdirs(path);
    fs.setOwner(path,user1.getUserName(),user1.getPrimaryGroupName());
    String aceListXattrsPattern =
    ".*allowed=true.*ugi=theDoctor.*cmd=listXAttrs.*";
    fileSys.listXAttrs(path);
    verifyAuditLogs(aceListXattrsPattern);
  }

  @Test
  public void testGetAclStatus() throws Exception {
    Path path = new Path("/test");
    fs.mkdirs(path);
    fileSys = DFSTestUtil.getFileSystemAs(user1, conf);
    fs.setOwner(path, user1.getUserName(), user1.getPrimaryGroupName());
    final FSDirectory dir = cluster.getNamesystem().getFSDirectory();
    final FSDirectory mockedDir = Mockito.spy(dir);
    AccessControlException ex = new AccessControlException();
    doThrow(ex).when(mockedDir).getPermissionChecker();
    cluster.getNamesystem().setFSDirectory(mockedDir);
    String aceGetAclStatus =
        ".*allowed=false.*ugi=theDoctor.*cmd=getAclStatus.*";
    try {
      fileSys.getAclStatus(path);
      fail("The operation should have failed with AccessControlException");
    } catch (AccessControlException ace) {
    }
    int length = verifyAuditLogs(aceGetAclStatus);
    fileSys.close();
    try {
      fileSys.getAclStatus(path);
      verifyAuditLogs(aceGetAclStatus);
      fail("The operation should have failed with IOException");
    } catch (IOException ace) {
    }
    assertTrue("Unexpected log!",
        length == auditlog.getOutput().split("\n").length);
    cluster.getNamesystem().setFSDirectory(dir);
  }

  @Test
  public void testDelegationTokens() throws Exception {
    Token dt = fs.getDelegationToken("foo");
    final String getDT =
        ".*src=HDFS_DELEGATION_TOKEN token 1.*with renewer foo.*";
    verifyAuditLogs(true, ".*cmd=getDelegationToken" + getDT);

    // renew
    final UserGroupInformation foo =
        UserGroupInformation.createUserForTesting("foo", new String[] {});
    foo.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        dt.renew(conf);
        return null;
      }
    });
    verifyAuditLogs(true, ".*cmd=renewDelegationToken" + getDT);
    try {
      dt.renew(conf);
      fail("Renewing a token with non-renewer should fail");
    } catch (AccessControlException expected) {
    }
    verifyAuditLogs(false, ".*cmd=renewDelegationToken" + getDT);

    // cancel
    final UserGroupInformation bar =
        UserGroupInformation.createUserForTesting("bar", new String[] {});
    try {
      bar.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          dt.cancel(conf);
          return null;
        }
      });
      fail("Canceling a token with non-renewer should fail");
    } catch (AccessControlException expected) {
    }
    verifyAuditLogs(false, ".*cmd=cancelDelegationToken" + getDT);
    dt.cancel(conf);
    verifyAuditLogs(true, ".*cmd=cancelDelegationToken" + getDT);
  }

  private int verifyAuditLogs(final boolean allowed, final String pattern) {
    return verifyAuditLogs(".*allowed=" + allowed + pattern);
  }

  private int verifyAuditLogs(String pattern) {
    int length = auditlog.getOutput().split("\n").length;
    String lastAudit = auditlog.getOutput().split("\n")[length - 1];
    assertTrue("Unexpected log!", lastAudit.matches(pattern));
    return length;
  }

  private void removeExistingCachePools(String prevPool) throws Exception {
    BatchedRemoteIterator.BatchedEntries<CachePoolEntry> entries =
        proto.listCachePools(prevPool);
    for(int i =0;i < entries.size();i++) {
      proto.removeCachePool(entries.get(i).getInfo().getPoolName());
    }
  }
}

