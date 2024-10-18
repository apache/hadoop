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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createMountTableEntry;
import static org.apache.hadoop.test.GenericTestUtils.getMethodName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRouterListOpenFiles {
  final private static int NUM_SUBCLUSTERS = 2;
  final private static int BATCH_SIZE = 3;
  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static RouterClientProtocol routerProtocol;
  private static DFSClient client0;
  private static DFSClient client1;
  private static DFSClient routerClient;

  @BeforeClass
  public static void setup() throws Exception {
    cluster = new StateStoreDFSCluster(false, NUM_SUBCLUSTERS,
        MultipleDestinationMountTableResolver.class);
    Configuration conf = new RouterConfigBuilder().stateStore().heartbeat().admin().rpc().build();
    conf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE, "ns0,ns1");
    conf.setBoolean(RBFConfigKeys.MOUNT_TABLE_CACHE_UPDATE, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES, BATCH_SIZE);
    cluster.addRouterOverrides(conf);
    cluster.startCluster(conf);
    cluster.startRouters();
    cluster.waitClusterUp();
    routerContext = cluster.getRandomRouter();
    routerProtocol = routerContext.getRouterRpcServer().getClientProtocolModule();
    routerClient = routerContext.getClient();
    client0 = cluster.getNamenode("ns0", null).getClient();
    client1 = cluster.getNamenode("ns1", null).getClient();
  }

  @AfterClass
  public static void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Before
  public void resetInodeId() {
    cluster.getNamenode("ns0", null).getNamenode().getNamesystem().getFSDirectory()
        .resetLastInodeIdWithoutChecking(12345);
    cluster.getNamenode("ns1", null).getNamenode().getNamesystem().getFSDirectory()
        .resetLastInodeIdWithoutChecking(12345);
  }

  @Test
  public void testSingleDestination() throws Exception {
    String testPath = "/" + getMethodName();
    createMountTableEntry(routerContext.getRouter(), testPath, DestinationOrder.HASH,
        Collections.singletonList("ns0"));

    // Create 2 dirs with the same name on 2 different nss
    client0.mkdirs(testPath);
    client1.mkdirs(testPath);
    // Open 2 files with different names
    OutputStream os0 = client0.create(testPath + "/file0", true);
    OutputStream os1 = client1.create(testPath + "/file1", true);

    BatchedRemoteIterator.BatchedEntries<OpenFileEntry> result =
        routerProtocol.listOpenFiles(0, EnumSet.of(OpenFilesIterator.OpenFilesType.ALL_OPEN_FILES),
            testPath);
    // Should list only the entry on ns0
    assertEquals(1, result.size());
    assertEquals(testPath + "/file0", result.get(0).getFilePath());
    os0.close();
    os1.close();
  }

  @Test
  public void testMultipleDestinations() throws Exception {
    String testPath = "/" + getMethodName();
    createMountTableEntry(routerContext.getRouter(), testPath, DestinationOrder.HASH_ALL,
        cluster.getNameservices());

    // Create 2 dirs with the same name on 2 different nss
    client0.mkdirs(testPath);
    client1.mkdirs(testPath);

    // Open 2 files with different names
    OutputStream os0 = client0.create(testPath + "/file0", true);
    OutputStream os1 = client1.create(testPath + "/file1", true);
    BatchedRemoteIterator.BatchedEntries<OpenFileEntry> result =
        routerProtocol.listOpenFiles(0, EnumSet.of(OpenFilesIterator.OpenFilesType.ALL_OPEN_FILES),
            testPath);
    // Should list both entries on ns0 and ns1
    assertEquals(2, result.size());
    assertEquals(testPath + "/file0", result.get(0).getFilePath());
    assertEquals(testPath + "/file1", result.get(1).getFilePath());
    RemoteIterator<OpenFileEntry> ite = routerClient.listOpenFiles(testPath);
    while (ite.hasNext()) {
      OpenFileEntry ofe = ite.next();
      assertTrue(ofe.getFilePath().equals(testPath + "/file0") || ofe.getFilePath()
          .equals(testPath + "/file1"));
    }
    os0.close();
    os1.close();

    // Open 2 files with same name
    os0 = client0.create(testPath + "/file2", true);
    os1 = client1.create(testPath + "/file2", true);
    result =
        routerProtocol.listOpenFiles(0, EnumSet.of(OpenFilesIterator.OpenFilesType.ALL_OPEN_FILES),
            testPath);
    // Should list both entries
    assertEquals(2, result.size());
    assertEquals(testPath + "/file2", result.get(0).getFilePath());
    assertEquals(testPath + "/file2", result.get(1).getFilePath());
    ite = routerClient.listOpenFiles(testPath);
    while (ite.hasNext()) {
      OpenFileEntry ofe = ite.next();
      assertTrue(ofe.getFilePath().equals(testPath + "/file2"));
    }
    os0.close();
    os1.close();
  }

  @Test
  public void testMultipleDestinationsMultipleBatches() throws Exception {
    String testPath = "/" + getMethodName();
    createMountTableEntry(routerContext.getRouter(), testPath, DestinationOrder.HASH_ALL,
        cluster.getNameservices());

    // Make ns1 have a much bigger inodeid than ns0
    cluster.getNamenode("ns0", null).getNamenode().getNamesystem().getFSDirectory()
        .resetLastInodeIdWithoutChecking((long) 1E6);
    cluster.getNamenode("ns1", null).getNamenode().getNamesystem().getFSDirectory()
        .resetLastInodeIdWithoutChecking((long) 2E6);
    runBatchListOpenFilesTest(testPath);

    // Rerun the test with ns0 having a much bigger inodeid than ns1
    cluster.getNamenode("ns0", null).getNamenode().getNamesystem().getFSDirectory()
        .resetLastInodeIdWithoutChecking((long) 4E6);
    cluster.getNamenode("ns1", null).getNamenode().getNamesystem().getFSDirectory()
        .resetLastInodeIdWithoutChecking((long) 3E6);
    runBatchListOpenFilesTest(testPath);
  }

  private static void runBatchListOpenFilesTest(String testPath) throws IOException {
    // Create 2 dirs with the same name on 2 different nss
    client0.mkdirs(testPath);
    client1.mkdirs(testPath);
    // Open 3 batches on both namespaces
    OutputStream[] oss0 = new OutputStream[3 * BATCH_SIZE];
    OutputStream[] oss1 = new OutputStream[3 * BATCH_SIZE];
    for (int i = 0; i < 3 * BATCH_SIZE; i++) {
      oss0[i] = client0.create(testPath + "/file0a_" + i, true);
      oss1[i] = client1.create(testPath + "/file1a_" + i, true);
    }
    RemoteIterator<OpenFileEntry> ite = routerClient.listOpenFiles(testPath);
    List<OpenFileEntry> allEntries = new ArrayList<>();
    while (ite.hasNext()) {
      allEntries.add(ite.next());
    }
    // All files should be reported once
    assertEquals(3 * 2 * BATCH_SIZE, allEntries.size());

    // Clean up
    for (int i = 0; i < 3 * BATCH_SIZE; i++) {
      oss0[i].close();
      oss1[i].close();
    }
    client0.delete(testPath, true);
    client1.delete(testPath, true);
  }
}
