/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
import org.apache.hadoop.util.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.crypto.CryptoProtocolVersion.ENCRYPTION_ZONES;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.NONE;
import static org.apache.hadoop.fs.permission.FsAction.READ;
import static org.apache.hadoop.fs.permission.FsAction.READ_WRITE;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.syncReturn;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestRouterClientSideTranslatorPB {
  private static MiniDFSCluster cluster = null;
  private static InetSocketAddress nnAddress = null;
  private static Configuration conf = null;
  private static RouterClientProtocolTranslatorPB clientProtocolTranslatorPB;
  private static RouterGetUserMappingsProtocolTranslatorPB getUserMappingsProtocolTranslatorPB;
  private static RouterNamenodeProtocolTranslatorPB namenodeProtocolTranslatorPB;
  private static RouterRefreshUserMappingsProtocolTranslatorPB
      refreshUserMappingsProtocolTranslatorPB;
  private static final String TEST_DIR_PATH = "/test";
  private boolean mode;

  @BeforeClass
  public static void setUp() throws Exception {
    AsyncRpcProtocolPBUtil.setWorker(ForkJoinPool.commonPool());
    conf = new HdfsConfiguration();
    cluster = (new MiniDFSCluster.Builder(conf))
        .numDataNodes(1).build();
    cluster.waitClusterUp();
    nnAddress = cluster.getNameNode().getNameNodeAddress();
    clientProtocolTranslatorPB = new RouterClientProtocolTranslatorPB(
        createProxy(ClientNamenodeProtocolPB.class));
    getUserMappingsProtocolTranslatorPB = new RouterGetUserMappingsProtocolTranslatorPB(
        createProxy(GetUserMappingsProtocolPB.class));
    namenodeProtocolTranslatorPB = new RouterNamenodeProtocolTranslatorPB(
        createProxy(NamenodeProtocolPB.class));
    refreshUserMappingsProtocolTranslatorPB = new RouterRefreshUserMappingsProtocolTranslatorPB(
        createProxy(RefreshUserMappingsProtocolPB.class));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (clientProtocolTranslatorPB != null) {
      clientProtocolTranslatorPB.close();
    }
    if (getUserMappingsProtocolTranslatorPB != null) {
      getUserMappingsProtocolTranslatorPB.close();
    }
    if (namenodeProtocolTranslatorPB != null) {
      namenodeProtocolTranslatorPB.close();
    }
    if (refreshUserMappingsProtocolTranslatorPB != null) {
      refreshUserMappingsProtocolTranslatorPB.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setAsync() {
    mode = Client.isAsynchronousMode();
    Client.setAsynchronousMode(true);
  }

  @After
  public void unsetAsync() {
    Client.setAsynchronousMode(mode);
  }

  @Test
  public void testRouterClientProtocolTranslatorPB() throws Exception {
    clientProtocolTranslatorPB.mkdirs(TEST_DIR_PATH, new FsPermission(ALL, ALL, ALL), false);
    Boolean success = syncReturn(Boolean.class);
    assertTrue(success);

    clientProtocolTranslatorPB.setPermission(TEST_DIR_PATH,
        new FsPermission(READ_WRITE, READ, NONE));
    syncReturn(Void.class);

    clientProtocolTranslatorPB.getFileInfo(TEST_DIR_PATH);
    HdfsFileStatus hdfsFileStatus = syncReturn(HdfsFileStatus.class);
    assertEquals(hdfsFileStatus.getPermission(), new FsPermission(READ_WRITE, READ, NONE));

    List<AclEntry> aclSpec = Lists.newArrayList(aclEntry(DEFAULT, USER, "tmpUser", ALL));
    clientProtocolTranslatorPB.setAcl(TEST_DIR_PATH, aclSpec);
    syncReturn(Void.class);
    clientProtocolTranslatorPB.setOwner(TEST_DIR_PATH, "tmpUser", "tmpUserGroup");
    syncReturn(Void.class);

    clientProtocolTranslatorPB.getFileInfo(TEST_DIR_PATH);
    hdfsFileStatus = syncReturn(HdfsFileStatus.class);
    assertEquals("tmpUser", hdfsFileStatus.getOwner());
    assertEquals("tmpUserGroup", hdfsFileStatus.getGroup());

    clientProtocolTranslatorPB.create(TEST_DIR_PATH + "/testCreate.file",
        new FsPermission(ALL, ALL, ALL), "testAsyncClient",
        new EnumSetWritable<>(EnumSet.of(CreateFlag.CREATE)),
        false, (short) 1, 128 * 1024 * 1024L, new CryptoProtocolVersion[]{ENCRYPTION_ZONES},
        null, null);
    hdfsFileStatus = syncReturn(HdfsFileStatus.class);
    assertTrue(hdfsFileStatus.isFile());
    assertEquals(128 * 1024 * 1024, hdfsFileStatus.getBlockSize());

    clientProtocolTranslatorPB.getListing(TEST_DIR_PATH, new byte[1], true);
    DirectoryListing directoryListing = syncReturn(DirectoryListing.class);
    assertEquals(1, directoryListing.getPartialListing().length);

    clientProtocolTranslatorPB.getDatanodeReport(HdfsConstants.DatanodeReportType.ALL);
    DatanodeInfo[] datanodeInfos = syncReturn(DatanodeInfo[].class);
    assertEquals(1, datanodeInfos.length);

    clientProtocolTranslatorPB.createSymlink(TEST_DIR_PATH + "/testCreate.file",
        "/link/link.file", new FsPermission(ALL, ALL, ALL), true);
    syncReturn(Void.class);

    clientProtocolTranslatorPB.getFileLinkInfo("/link/link.file");
    hdfsFileStatus = syncReturn(HdfsFileStatus.class);
    assertEquals("testCreate.file", hdfsFileStatus.getSymlink().getName());

    clientProtocolTranslatorPB.rename(TEST_DIR_PATH + "/testCreate.file",
        TEST_DIR_PATH + "/testRename.file");
    success = syncReturn(boolean.class);
    assertTrue(success);

    clientProtocolTranslatorPB.delete(TEST_DIR_PATH, true);
    success = syncReturn(boolean.class);
    assertTrue(success);

    LambdaTestUtils.intercept(RemoteException.class, "Parent directory doesn't exist: /test",
        () -> {
          clientProtocolTranslatorPB.mkdirs(TEST_DIR_PATH + "/testCreate.file",
              new FsPermission(ALL, ALL, ALL), false);
          syncReturn(boolean.class);
        });
  }

  @Test
  public void testRouterGetUserMappingsProtocolTranslatorPB() throws Exception {
    getUserMappingsProtocolTranslatorPB.getGroupsForUser("root");
    String[] strings = syncReturn(String[].class);
    assertTrue(strings.length != 0);

    getUserMappingsProtocolTranslatorPB.getGroupsForUser("tmp");
    strings = syncReturn(String[].class);
    assertEquals(0, strings.length);
  }

  @Test
  public void testRouterNamenodeProtocolTranslatorPB() throws Exception {
    namenodeProtocolTranslatorPB.getTransactionID();
    Long id = syncReturn(Long.class);
    assertTrue(id > 0);

    namenodeProtocolTranslatorPB.getBlockKeys();
    ExportedBlockKeys exportedBlockKeys = syncReturn(ExportedBlockKeys.class);
    assertNotNull(exportedBlockKeys);

    namenodeProtocolTranslatorPB.rollEditLog();
    CheckpointSignature checkpointSignature = syncReturn(CheckpointSignature.class);
    assertNotNull(checkpointSignature);
  }

  @Test
  public void testRouterRefreshUserMappingsProtocolTranslatorPB() throws Exception {
    refreshUserMappingsProtocolTranslatorPB.refreshUserToGroupsMappings();
    syncReturn(Void.class);

    refreshUserMappingsProtocolTranslatorPB.refreshSuperUserGroupsConfiguration();
    syncReturn(Void.class);
  }

  public static <P> P createProxy(Class<P> protocol) throws IOException {
    RPC.setProtocolEngine(
        conf, protocol, ProtobufRpcEngine2.class);
    final long version = RPC.getProtocolVersion(protocol);
    return RPC.getProtocolProxy(
        protocol, version, nnAddress, UserGroupInformation.getCurrentUser(),
        conf,
        NetUtils.getDefaultSocketFactory(conf),
        RPC.getRpcTimeout(conf), null,
        new AtomicBoolean(false)).getProxy();
  }
}
