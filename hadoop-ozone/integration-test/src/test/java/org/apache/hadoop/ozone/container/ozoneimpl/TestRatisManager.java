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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Tests ozone containers with Apache Ratis.
 */
@Ignore("Disabling Ratis tests for pipeline work.")
public class TestRatisManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestRatisManager.class);

  static OzoneConfiguration newOzoneConfiguration() {
    return new OzoneConfiguration();
  }


  /** Set the timeout for every test. */
  @Rule
  public Timeout testTimeout = new Timeout(200_000);

  @Test
  public void testTestRatisManagerGrpc() throws Exception {
    runTestRatisManager(SupportedRpcType.GRPC);
  }

  @Test
  public void testTestRatisManagerNetty() throws Exception {
    runTestRatisManager(SupportedRpcType.NETTY);
  }

  private static void runTestRatisManager(RpcType rpc) throws Exception {
    LOG.info("runTestRatisManager, rpc=" + rpc);

    // create Ozone clusters
    final OzoneConfiguration conf = newOzoneConfiguration();
    RatisTestHelper.initRatisConf(rpc, conf);
    final MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();
    try {
      cluster.waitForClusterToBeReady();

      final List<HddsDatanodeService> datanodes = cluster.getHddsDatanodes();
      final List<DatanodeDetails> datanodeDetailsSet = datanodes.stream()
          .map(HddsDatanodeService::getDatanodeDetails).collect(
              Collectors.toList());

      //final RatisManager manager = RatisManager.newRatisManager(conf);

      final int[] idIndex = {3, 4, 5};
      for (int i = 0; i < idIndex.length; i++) {
        final int previous = i == 0 ? 0 : idIndex[i - 1];
        final List<DatanodeDetails> subIds = datanodeDetailsSet.subList(
            previous, idIndex[i]);

        // Create Ratis cluster
        final String ratisId = "ratis" + i;
        //manager.createRatisCluster(ratisId, subIds);
        LOG.info("Created RatisCluster " + ratisId);

        // check Ratis cluster members
        //final List<DatanodeDetails> dns = manager.getMembers(ratisId);
        //Assert.assertEquals(subIds, dns);
      }

      // randomly close two of the clusters
      final int chosen = ThreadLocalRandom.current().nextInt(idIndex.length);
      LOG.info("chosen = " + chosen);

      for (int i = 0; i < idIndex.length; i++) {
        if (i != chosen) {
          final String ratisId = "ratis" + i;
          //manager.closeRatisCluster(ratisId);
        }
      }

      // update datanodes
      final String ratisId = "ratis" + chosen;
      //manager.updatePipeline(ratisId, allIds);

      // check Ratis cluster members
      //final List<DatanodeDetails> dns = manager.getMembers(ratisId);
      //Assert.assertEquals(allIds, dns);
    } finally {
      cluster.shutdown();
    }
  }

}
