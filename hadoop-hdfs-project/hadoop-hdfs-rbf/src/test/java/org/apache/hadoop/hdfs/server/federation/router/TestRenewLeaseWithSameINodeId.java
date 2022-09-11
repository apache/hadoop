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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Testing DFSClient renewLease with same INodeId.
 */
public class TestRenewLeaseWithSameINodeId {

  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;

  /** The first Router Context for this federated cluster. */
  private static MiniRouterDFSCluster.RouterContext routerContext;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new MiniRouterDFSCluster(false, 2);
    cluster.setNumDatanodesPerNameservice(3);
    cluster.startCluster();

    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .quota()
        .build();
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();

    routerContext = cluster.getRouters().get(0);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cluster.shutdown();
  }

  /**
   * Testing case:
   * 1. One Router DFSClient writing multi files from different namespace with same iNodeId.
   * 2. DFSClient Lease Renewer should work well.
   */
  @Test
  public void testRenewLeaseWithSameINodeId() throws IOException {
    // Add mount point "/ns0" and "/ns1"
    Router router = cluster.getRouters().get(0).getRouter();
    MockResolver resolver = (MockResolver) router.getSubclusterResolver();
    resolver.addLocation("/ns0", cluster.getNameservices().get(0), "/ns0");
    resolver.addLocation("/ns1", cluster.getNameservices().get(1), "/ns1");

    DistributedFileSystem fs = (DistributedFileSystem) routerContext.getFileSystem();

    Path path1 = new Path("/ns0/file");
    Path path2 = new Path("/ns1/file");

    try (FSDataOutputStream ignored1 = fs.create(path1);
         FSDataOutputStream ignored2 = fs.create(path2)) {
      HdfsFileStatus fileStatus1 = fs.getClient().getFileInfo(path1.toUri().getPath());
      HdfsFileStatus fileStatus2 = fs.getClient().getFileInfo(path2.toUri().getPath());

      // The fileId of the files from different new namespaces should be same.
      assertEquals(fileStatus2.getFileId(), fileStatus1.getFileId());

      // The number of fileBeingWritten of this DFSClient should be two.
      assertEquals(2, fs.getClient().getNumOfFilesBeingWritten());
    }

    // The number of fileBeingWritten of this DFSClient should be zero.
    assertEquals(0, fs.getClient().getNumOfFilesBeingWritten());
  }
}
