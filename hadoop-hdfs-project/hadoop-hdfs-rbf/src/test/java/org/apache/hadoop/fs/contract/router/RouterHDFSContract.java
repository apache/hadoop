/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.fs.contract.router;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.hdfs.HDFSContract;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.junit.Assert;

/**
 * The contract of Router-based Federated HDFS.
 */
public class RouterHDFSContract extends HDFSContract {

  public static final int BLOCK_SIZE =
      AbstractFSContractTestBase.TEST_FILE_LEN;
  private static MiniRouterDFSCluster cluster;

  public RouterHDFSContract(Configuration conf) {
    super(conf);
  }

  public static void createCluster() throws IOException {
    createCluster(false);
  }

  public static void createCluster(boolean security) throws IOException {
    createCluster(true, 2, security);
  }

  public static void createCluster(
      boolean ha, int numNameServices, boolean security) throws IOException {
    try {
      Configuration conf = null;
      if (security) {
        conf = SecurityConfUtil.initSecurity();
      }
      cluster = new MiniRouterDFSCluster(ha, numNameServices, conf);

      // Start NNs and DNs and wait until ready
      cluster.startCluster(conf);

      // Start routers with only an RPC service
      cluster.startRouters();

      // Register and verify all NNs with all routers
      cluster.registerNamenodes();
      cluster.waitNamenodeRegistration();

      // Setup the mount table
      cluster.installMockLocations();

      // Making one Namenodes active per nameservice
      if (cluster.isHighAvailability()) {
        for (String ns : cluster.getNameservices()) {
          cluster.switchToActive(ns, NAMENODES[0]);
          cluster.switchToStandby(ns, NAMENODES[1]);
        }
      }

      cluster.waitActiveNamespaces();
    } catch (Exception e) {
      destroyCluster();
      throw new IOException("Cannot start federated cluster", e);
    }
  }

  public static void destroyCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    try {
      SecurityConfUtil.destroy();
    } catch (Exception e) {
      throw new IOException("Cannot destroy security context", e);
    }
  }

  public static MiniDFSCluster getCluster() {
    return cluster.getCluster();
  }

  public static MiniRouterDFSCluster getRouterCluster() {
    return cluster;
  }

  public static FileSystem getFileSystem() throws IOException {
    //assumes cluster is not null
    Assert.assertNotNull("cluster not created", cluster);
    return cluster.getRandomRouter().getFileSystem();
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    return getFileSystem();
  }
}