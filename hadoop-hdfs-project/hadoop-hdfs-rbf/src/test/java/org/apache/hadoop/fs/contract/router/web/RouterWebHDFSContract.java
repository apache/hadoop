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
package org.apache.hadoop.fs.contract.router.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.contract.hdfs.HDFSContract;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;

/**
 * The contract of Router-based Federated HDFS
 * This changes its feature set from platform for platform -the default
 * set is updated during initialization.
 */
public class RouterWebHDFSContract extends HDFSContract {

  public static final Logger LOG =
      LoggerFactory.getLogger(WebHdfsFileSystem.class);

  public static final String CONTRACT_WEBHDFS_XML = "contract/webhdfs.xml";
  private static MiniRouterDFSCluster cluster;

  public RouterWebHDFSContract(Configuration conf) {
    super(conf);
    addConfResource(CONTRACT_WEBHDFS_XML);
  }

  public static void createCluster() throws IOException {
    try {
      HdfsConfiguration conf = new HdfsConfiguration();
      conf.addResource(CONTRACT_HDFS_XML);
      conf.addResource(CONTRACT_WEBHDFS_XML);

      cluster = new MiniRouterDFSCluster(true, 2);

      // Start NNs and DNs and wait until ready
      cluster.startCluster();

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
    } catch (Exception e) {
      cluster = null;
      throw new IOException("Cannot start federated cluster", e);
    }
  }

  public static void destroyCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  public static MiniDFSCluster getCluster() {
    return cluster.getCluster();
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    return getFileSystem();
  }

  public static FileSystem getFileSystem() throws IOException {
    //assumes cluster is not null
    Assert.assertNotNull("cluster not created", cluster);

    // Create a connection to WebHDFS
    try {
      RouterContext router = cluster.getRandomRouter();
      String uriStr =
          WebHdfsConstants.WEBHDFS_SCHEME + "://" + router.getHttpAddress();
      URI uri = new URI(uriStr);
      Configuration conf = new HdfsConfiguration();
      return FileSystem.get(uri, conf);
    } catch (URISyntaxException e) {
      LOG.error("Cannot create URI for the WebHDFS filesystem", e);
    }
    return null;
  }

  @Override
  public String getScheme() {
    return WebHdfsConstants.WEBHDFS_SCHEME;
  }
}