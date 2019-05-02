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
package org.apache.hadoop.hdfs.server.federation.router;

import static java.util.Arrays.asList;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createMountTableEntry;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.getFileSystem;
import static org.apache.hadoop.hdfs.server.federation.MockNamenode.registerSubclusters;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.getStateStoreConfiguration;
import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.MockNamenode;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test the behavior when listing a mount point mapped to multiple subclusters
 * and one of the subclusters is missing it.
 */
public class TestRouterMissingFolderMulti {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterMissingFolderMulti.class);

  /** Number of files to create for testing. */
  private static final int NUM_FILES = 10;

  /** Namenodes for the test per name service id (subcluster). */
  private Map<String, MockNamenode> namenodes = new HashMap<>();
  /** Routers for the test. */
  private Router router;


  @Before
  public void setup() throws Exception {
    LOG.info("Start the Namenodes");
    Configuration nnConf = new HdfsConfiguration();
    nnConf.setInt(DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY, 10);
    for (final String nsId : asList("ns0", "ns1")) {
      MockNamenode nn = new MockNamenode(nsId, nnConf);
      nn.transitionToActive();
      nn.addFileSystemMock();
      namenodes.put(nsId, nn);
    }

    LOG.info("Start the Routers");
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    routerConf.set(RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY, "0.0.0.0:0");
    routerConf.set(RBFConfigKeys.DFS_ROUTER_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    routerConf.set(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY, "0.0.0.0:0");

    Configuration stateStoreConf = getStateStoreConfiguration();
    stateStoreConf.setClass(
        RBFConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        MembershipNamenodeResolver.class, ActiveNamenodeResolver.class);
    stateStoreConf.setClass(
        RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        MultipleDestinationMountTableResolver.class,
        FileSubclusterResolver.class);
    routerConf.addResource(stateStoreConf);

    routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_ALLOW_PARTIAL_LIST, false);

    router = new Router();
    router.init(routerConf);
    router.start();

    LOG.info("Registering the subclusters in the Routers");
    registerSubclusters(router, namenodes.values());
  }

  @After
  public void cleanup() throws Exception {
    LOG.info("Stopping the cluster");
    for (final MockNamenode nn : namenodes.values()) {
      nn.stop();
    }
    namenodes.clear();

    if (router != null) {
      router.stop();
      router = null;
    }
  }

  @Test
  public void testSuccess() throws Exception {
    FileSystem fs = getFileSystem(router);
    String mountPoint = "/test-success";
    createMountTableEntry(router, mountPoint,
        DestinationOrder.HASH_ALL, namenodes.keySet());
    Path folder = new Path(mountPoint, "folder-all");
    for (int i = 0; i < NUM_FILES; i++) {
      Path file = new Path(folder, "file-" + i + ".txt");
      FSDataOutputStream os = fs.create(file);
      os.close();
    }
    FileStatus[] files = fs.listStatus(folder);
    assertEquals(NUM_FILES, files.length);
    ContentSummary contentSummary = fs.getContentSummary(folder);
    assertEquals(NUM_FILES, contentSummary.getFileCount());
  }

  @Test
  public void testFileNotFound() throws Exception {
    FileSystem fs = getFileSystem(router);
    String mountPoint = "/test-non-existing";
    createMountTableEntry(router,
        mountPoint, DestinationOrder.HASH_ALL, namenodes.keySet());
    Path path = new Path(mountPoint, "folder-all");
    LambdaTestUtils.intercept(FileNotFoundException.class,
        () -> fs.listStatus(path));
    LambdaTestUtils.intercept(FileNotFoundException.class,
        () -> fs.getContentSummary(path));
  }

  @Test
  public void testOneMissing() throws Exception  {
    FileSystem fs = getFileSystem(router);
    String mountPoint = "/test-one-missing";
    createMountTableEntry(router, mountPoint,
        DestinationOrder.HASH_ALL, namenodes.keySet());

    // Create the folders directly in only one of the Namenodes
    MockNamenode nn = namenodes.get("ns0");
    int nnRpcPort = nn.getRPCPort();
    FileSystem nnFs = getFileSystem(nnRpcPort);
    Path folder = new Path(mountPoint, "folder-all");
    for (int i = 0; i < NUM_FILES; i++) {
      Path file = new Path(folder, "file-" + i + ".txt");
      FSDataOutputStream os = nnFs.create(file);
      os.close();
    }

    FileStatus[] files = fs.listStatus(folder);
    assertEquals(NUM_FILES, files.length);
    ContentSummary summary = fs.getContentSummary(folder);
    assertEquals(NUM_FILES, summary.getFileAndDirectoryCount());
  }
}
