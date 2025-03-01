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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterClientProtocol;
import org.apache.hadoop.hdfs.server.federation.router.TestRouterMountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test a router end-to-end including the MountTable using async rpc.
 */
public class TestRouterAsyncMountTable extends TestRouterMountTable {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterAsyncMountTable.class.getName());

  @BeforeClass
  public static void globalSetUp() throws Exception {
    startTime = Time.now();

    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    conf.setInt(RBFConfigKeys.DFS_ROUTER_ADMIN_MAX_COMPONENT_LENGTH_KEY, 20);
    conf.setBoolean(DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    // Get the end points
    nnContext0 = cluster.getNamenode("ns0", null);
    nnContext1 = cluster.getNamenode("ns1", null);
    nnFs0 = nnContext0.getFileSystem();
    nnFs1 = nnContext1.getFileSystem();
    routerContext = cluster.getRandomRouter();
    routerFs = routerContext.getFileSystem();
    Router router = routerContext.getRouter();
    routerProtocol = routerContext.getClient().getNamenode();
    mountTable = (MountTableResolver) router.getSubclusterResolver();
  }

  /**
   * Verify the getMountPointStatus result of passing in different parameters.
   */
  @Override
  @Test
  public void testGetMountPointStatus() throws IOException {
    MountTable addEntry = MountTable.newInstance("/testA/testB/testC/testD",
        Collections.singletonMap("ns0", "/testA/testB/testC/testD"));
    assertTrue(addMountTable(addEntry));
    RouterClientProtocol clientProtocol = new RouterAsyncClientProtocol(
        nnFs0.getConf(), routerContext.getRouter().getRpcServer());
    String src = "/";
    String child = "testA";
    Path childPath = new Path(src, child);
    HdfsFileStatus dirStatus;
    clientProtocol.getMountPointStatus(childPath.toString(), 0, 0);
    try {
      dirStatus = syncReturn(HdfsFileStatus.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    assertEquals(child, dirStatus.getLocalName());
    String src1 = "/testA";
    String child1 = "testB";
    Path childPath1 = new Path(src1, child1);
    HdfsFileStatus dirStatus1;
    clientProtocol.getMountPointStatus(childPath1.toString(), 0, 0);
    try {
      dirStatus1 = syncReturn(HdfsFileStatus.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    assertEquals(child1, dirStatus1.getLocalName());

    String src2 = "/testA/testB";
    String child2 = "testC";
    Path childPath2 = new Path(src2, child2);
    HdfsFileStatus dirStatus2;
    clientProtocol.getMountPointStatus(childPath2.toString(), 0, 0);
    try {
      dirStatus2 = syncReturn(HdfsFileStatus.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    assertEquals(child2, dirStatus2.getLocalName());

    HdfsFileStatus dirStatus3;
    clientProtocol.getMountPointStatus(childPath2.toString(), 0, 0, false);
    try {
      dirStatus3 = syncReturn(HdfsFileStatus.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    assertTrue(dirStatus3.isEmptyLocalName());
  }

  /**
   * Validate whether mount point name gets resolved or not. On successful
   * resolution the details returned would be the ones actually set on the mount
   * point.
   */
  @Test
  public void testMountPointResolved() throws IOException {
    MountTable addEntry = MountTable.newInstance("/testdir",
        Collections.singletonMap("ns0", "/tmp/testdir"));
    addEntry.setGroupName("group1");
    addEntry.setOwnerName("owner1");
    assertTrue(addMountTable(addEntry));
    HdfsFileStatus finfo = routerProtocol.getFileInfo("/testdir");
    FileStatus[] finfo1 = routerFs.listStatus(new Path("/"));
    assertEquals("owner1", finfo.getOwner());
    assertEquals("owner1", finfo1[0].getOwner());
    assertEquals("group1", finfo.getGroup());
    assertEquals("group1", finfo1[0].getGroup());
  }
  
  @Override
  @Test
  public void testListNonExistPath() throws Exception {
    mountTable.setDefaultNSEnable(false);
    LambdaTestUtils.intercept(FileNotFoundException.class,
        "File /base does not exist.",
        "Expect FileNotFoundException.",
        () -> routerFs.listStatus(new Path("/base")));
  }

}
