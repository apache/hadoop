/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.util.CheckedBiConsumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests ozone containers with Apache Ratis.
 */
public class TestOzoneContainerRatis {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestOzoneContainerRatis.class);

  static OzoneConfiguration newOzoneConfiguration() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    ContainerTestHelper.setOzoneLocalStorageRoot(
        TestOzoneContainerRatis.class, conf);
    return conf;
  }


  /** Set the timeout for every test. */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  @Test
  public void testOzoneContainerViaDataNodeRatisGrpc() throws Exception {
    runTestOzoneContainerViaDataNodeRatis(SupportedRpcType.GRPC, 1);
    runTestOzoneContainerViaDataNodeRatis(SupportedRpcType.GRPC, 3);
  }

  @Test
  public void testOzoneContainerViaDataNodeRatisNetty() throws Exception {
    runTestOzoneContainerViaDataNodeRatis(SupportedRpcType.NETTY, 1);
    runTestOzoneContainerViaDataNodeRatis(SupportedRpcType.NETTY, 3);
  }

  private static void runTestOzoneContainerViaDataNodeRatis(
      RpcType rpc, int numNodes) throws Exception {
    runTest("runTestOzoneContainerViaDataNodeRatis", rpc, numNodes,
        TestOzoneContainer::runTestOzoneContainerViaDataNode);
  }

  private static void runTest(
      String testName, RpcType rpc, int numNodes,
      CheckedBiConsumer<String, XceiverClientSpi, Exception> test)
      throws Exception {
    LOG.info(testName + "(rpc=" + rpc + ", numNodes=" + numNodes);

    final OzoneConfiguration conf = newOzoneConfiguration();
    final String containerName = OzoneUtils.getRequestID();
    final Pipeline pipeline = ContainerTestHelper.createPipeline(
        containerName, numNodes);
    final XceiverClientSpi client = RatisTestHelper.newXceiverClientRatis(
        rpc, pipeline, conf);

    final MiniOzoneCluster cluster = new MiniOzoneCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_LOCAL)
        .numDataNodes(pipeline.getMachines().size())
        .build();
    cluster.waitOzoneReady();

    try {
      test.accept(containerName, client);
    } finally {
      cluster.shutdown();
    }
  }

  private static void runTestBothGetandPutSmallFileRatis(
      RpcType rpc, int numNodes) throws Exception {
    runTest("runTestBothGetandPutSmallFileRatis", rpc, numNodes,
        TestOzoneContainer::runTestBothGetandPutSmallFile);
  }

  @Test
  public void testBothGetandPutSmallFileRatisNetty() throws Exception {
    runTestBothGetandPutSmallFileRatis(SupportedRpcType.NETTY, 1);
    runTestBothGetandPutSmallFileRatis(SupportedRpcType.NETTY, 3);
  }

  @Test
  public void testBothGetandPutSmallFileRatisGrpc() throws Exception {
    runTestBothGetandPutSmallFileRatis(SupportedRpcType.GRPC, 1);
    runTestBothGetandPutSmallFileRatis(SupportedRpcType.GRPC, 3);
  }

}
