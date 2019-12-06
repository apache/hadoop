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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import java.security.PrivilegedExceptionAction;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
  public void testDelegationTokens() throws Exception {
    final Token dt = fs.getDelegationToken("foo");
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
