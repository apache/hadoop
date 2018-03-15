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

package org.apache.hadoop.ozone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.web.client.OzoneRestClient;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Helpers for Ratis tests.
 */
public interface RatisTestHelper {
  Logger LOG = LoggerFactory.getLogger(RatisTestHelper.class);

  /** For testing Ozone with Ratis. */
  class RatisTestSuite implements Closeable {
    static final RpcType RPC = SupportedRpcType.NETTY;
    static final int NUM_DATANODES = 3;

    private final OzoneConfiguration conf;
    private final MiniOzoneClassicCluster cluster;

    /**
     * Create a {@link MiniOzoneClassicCluster} for testing by setting
     *   OZONE_ENABLED = true,
     *   RATIS_ENABLED = true, and
     *   OZONE_HANDLER_TYPE_KEY = "distributed".
     */
    public RatisTestSuite(final Class<?> clazz) throws IOException {
      conf = newOzoneConfiguration(clazz, RPC);
      cluster = newMiniOzoneCluster(NUM_DATANODES, conf);
    }

    public OzoneConfiguration getConf() {
      return conf;
    }

    public MiniOzoneClassicCluster getCluster() {
      return cluster;
    }

    public int getDatanodeInfoPort() {
      return cluster.getDataNodes().get(0).getInfoPort();
    }

    public OzoneRestClient newOzoneRestClient()
        throws OzoneException, URISyntaxException {
      return RatisTestHelper.newOzoneRestClient(getDatanodeOzoneRestPort());
    }

    @Override
    public void close() {
      cluster.close();
    }

    public int getDatanodeOzoneRestPort() {
      return cluster.getDataNodes().get(0).getDatanodeId().getOzoneRestPort();
    }
  }

  static OzoneConfiguration newOzoneConfiguration(
      Class<?> clazz, RpcType rpc) {
    final OzoneConfiguration conf = new OzoneConfiguration();
    ContainerTestHelper.setOzoneLocalStorageRoot(clazz, conf);
    initRatisConf(rpc, conf);
    return conf;
  }

  static void initRatisConf(RpcType rpc, Configuration conf) {
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY, true);
    conf.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY, rpc.name());
    LOG.info(OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY
        + " = " + rpc.name());
  }

  static MiniOzoneClassicCluster newMiniOzoneCluster(
      int numDatanodes, OzoneConfiguration conf) throws IOException {
    final MiniOzoneClassicCluster cluster =
        new MiniOzoneClassicCluster.Builder(conf)
        .numDataNodes(numDatanodes)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    return cluster;
  }

  static OzoneRestClient newOzoneRestClient(int port)
      throws OzoneException, URISyntaxException {
    return new OzoneRestClient("http://localhost:" + port);
  }
}
