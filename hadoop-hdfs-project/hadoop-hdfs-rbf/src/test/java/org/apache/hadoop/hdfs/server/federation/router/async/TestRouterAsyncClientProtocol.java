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
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.RouterClientProtocol;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.util.Lists;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.crypto.CryptoProtocolVersion.ENCRYPTION_ZONES;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.NONE;
import static org.apache.hadoop.fs.permission.FsAction.READ;
import static org.apache.hadoop.fs.permission.FsAction.READ_WRITE;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Used to test the functionality of {@link RouterAsyncClientProtocol}.
 */
public class TestRouterAsyncClientProtocol extends RouterAsyncProtocolTestBase {
  private RouterAsyncClientProtocol asyncClientProtocol;
  private RouterClientProtocol clientProtocol;
  private final String testPath = TEST_DIR_PATH + "/test";

  @Before
  public void setup() throws IOException {
    asyncClientProtocol = new RouterAsyncClientProtocol(getRouterConf(), getRouterAsyncRpcServer());
    clientProtocol = new RouterClientProtocol(getRouterConf(), getRouterRpcServer());
  }

  @Test
  public void testGetServerDefaults() throws Exception {
    FsServerDefaults serverDefaults = clientProtocol.getServerDefaults();
    asyncClientProtocol.getServerDefaults();
    FsServerDefaults fsServerDefaults = syncReturn(FsServerDefaults.class);
    assertEquals(serverDefaults.getBlockSize(), fsServerDefaults.getBlockSize());
    assertEquals(serverDefaults.getReplication(), fsServerDefaults.getReplication());
    assertEquals(serverDefaults.getChecksumType(), fsServerDefaults.getChecksumType());
    assertEquals(
        serverDefaults.getDefaultStoragePolicyId(), fsServerDefaults.getDefaultStoragePolicyId());
  }

  @Test
  public void testClientProtocolRpc() throws Exception {
    asyncClientProtocol.mkdirs(testPath, new FsPermission(ALL, ALL, ALL), false);
    Boolean success = syncReturn(Boolean.class);
    assertTrue(success);

    asyncClientProtocol.setPermission(testPath, new FsPermission(READ_WRITE, READ, NONE));
    syncReturn(Void.class);

    asyncClientProtocol.getFileInfo(testPath);
    HdfsFileStatus hdfsFileStatus = syncReturn(HdfsFileStatus.class);
    assertEquals(hdfsFileStatus.getPermission(), new FsPermission(READ_WRITE, READ, NONE));

    List<AclEntry> aclSpec = Lists.newArrayList(aclEntry(DEFAULT, USER, "tmpUser", ALL));
    asyncClientProtocol.setAcl(testPath, aclSpec);
    syncReturn(Void.class);
    asyncClientProtocol.setOwner(testPath, "tmpUser", "tmpUserGroup");
    syncReturn(Void.class);

    asyncClientProtocol.getFileInfo(testPath);
    hdfsFileStatus = syncReturn(HdfsFileStatus.class);
    assertEquals("tmpUser", hdfsFileStatus.getOwner());
    assertEquals("tmpUserGroup", hdfsFileStatus.getGroup());

    asyncClientProtocol.create(testPath + "/testCreate.file",
        new FsPermission(ALL, ALL, ALL), "testAsyncClient",
        new EnumSetWritable<>(EnumSet.of(CreateFlag.CREATE)),
        false, (short) 1, 128 * 1024 * 1024L,
        new CryptoProtocolVersion[]{ENCRYPTION_ZONES},
        null, null);
    hdfsFileStatus = syncReturn(HdfsFileStatus.class);
    assertTrue(hdfsFileStatus.isFile());
    assertEquals(128 * 1024 * 1024, hdfsFileStatus.getBlockSize());

    asyncClientProtocol.getFileRemoteLocation(testPath);
    RemoteLocation remoteLocation = syncReturn(RemoteLocation.class);
    assertNotNull(remoteLocation);
    assertEquals(getNs0(), remoteLocation.getNameserviceId());
    assertEquals(testPath, remoteLocation.getSrc());

    asyncClientProtocol.getListing(testPath, new byte[1], true);
    DirectoryListing directoryListing = syncReturn(DirectoryListing.class);
    assertEquals(1, directoryListing.getPartialListing().length);

    asyncClientProtocol.getDatanodeReport(HdfsConstants.DatanodeReportType.ALL);
    DatanodeInfo[] datanodeInfos = syncReturn(DatanodeInfo[].class);
    assertEquals(3, datanodeInfos.length);

    asyncClientProtocol.createSymlink(testPath + "/testCreate.file",
        "/link/link.file", new FsPermission(ALL, ALL, ALL), true);
    syncReturn(Void.class);

    asyncClientProtocol.getFileLinkInfo("/link/link.file");
    hdfsFileStatus = syncReturn(HdfsFileStatus.class);
    assertEquals("testCreate.file", hdfsFileStatus.getSymlink().getName());

    asyncClientProtocol.rename(testPath + "/testCreate.file",
        testPath + "/testRename.file");
    success = syncReturn(boolean.class);
    assertTrue(success);

    asyncClientProtocol.delete(testPath, true);
    success = syncReturn(boolean.class);
    assertTrue(success);
  }
}
