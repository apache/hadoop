/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;

/**
 * MiniOzoneHAClusterImpl creates a complete in-process Ozone cluster
 * with OM HA suitable for running tests.  The cluster consists of a set of
 * OzoneManagers, StorageContainerManager and multiple DataNodes.
 */
public final class MiniOzoneHAClusterImpl extends MiniOzoneClusterImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneHAClusterImpl.class);

  private Map<String, OzoneManager> ozoneManagerMap;
  private List<OzoneManager> ozoneManagers;

  private static final Random RANDOM = new Random();
  private static final int RATIS_LEADER_ELECTION_TIMEOUT = 1000; // 1 seconds

  public static final int NODE_FAILURE_TIMEOUT = 2000; // 2 seconds

  /**
   * Creates a new MiniOzoneCluster with OM HA.
   *
   * @throws IOException if there is an I/O error
   */

  private MiniOzoneHAClusterImpl(
      OzoneConfiguration conf,
      Map<String, OzoneManager> omMap,
      StorageContainerManager scm,
      List<HddsDatanodeService> hddsDatanodes) {
    super(conf, scm, hddsDatanodes);
    this.ozoneManagerMap = omMap;
    this.ozoneManagers = new ArrayList<>(omMap.values());
  }

  /**
   * Returns the first OzoneManager from the list.
   * @return
   */
  @Override
  public OzoneManager getOzoneManager() {
    return this.ozoneManagers.get(0);
  }

  public OzoneManager getOzoneManager(int index) {
    return this.ozoneManagers.get(index);
  }

  public OzoneManager getOzoneManager(String omNodeId) {
    return this.ozoneManagerMap.get(omNodeId);
  }

  @Override
  public void restartOzoneManager() throws IOException {
    for (OzoneManager ozoneManager : ozoneManagers) {
      ozoneManager.stop();
      ozoneManager.restart();
    }
  }

  @Override
  public void stop() {
    for (OzoneManager ozoneManager : ozoneManagers) {
      if (ozoneManager != null) {
        LOG.info("Stopping the OzoneManager " + ozoneManager.getOMNodId());
        ozoneManager.stop();
        ozoneManager.join();
      }
    }
    super.stop();
  }

  public void stopOzoneManager(int index) {
    ozoneManagers.get(index).stop();
  }

  public void stopOzoneManager(String omNodeId) {
    ozoneManagerMap.get(omNodeId).stop();
  }

  /**
   * Builder for configuring the MiniOzoneCluster to run.
   */
  public static class Builder extends MiniOzoneClusterImpl.Builder {

    private final String nodeIdBaseStr = "omNode-";

    /**
     * Creates a new Builder.
     *
     * @param conf configuration
     */
    public Builder(OzoneConfiguration conf) {
      super(conf);
    }

    @Override
    public MiniOzoneCluster build() throws IOException {
      DefaultMetricsSystem.setMiniClusterMode(true);
      initializeConfiguration();
      StorageContainerManager scm;
      Map<String, OzoneManager> omMap;
      try {
        scm = createSCM();
        scm.start();
        omMap = createOMService();
      } catch (AuthenticationException ex) {
        throw new IOException("Unable to build MiniOzoneCluster. ", ex);
      }

      final List<HddsDatanodeService> hddsDatanodes = createHddsDatanodes(scm);
      MiniOzoneHAClusterImpl cluster = new MiniOzoneHAClusterImpl(conf, omMap,
          scm, hddsDatanodes);
      if (startDataNodes) {
        cluster.startHddsDatanodes();
      }
      return cluster;
    }

    /**
     * Initialize OM configurations.
     * @throws IOException
     */
    @Override
    void initializeConfiguration() throws IOException {
      super.initializeConfiguration();
      conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
      conf.setInt(OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY, numOfOmHandlers);
      conf.setTimeDuration(
          OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
          RATIS_LEADER_ELECTION_TIMEOUT, TimeUnit.MILLISECONDS);
      conf.setTimeDuration(
          OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_KEY,
          NODE_FAILURE_TIMEOUT, TimeUnit.MILLISECONDS);
      conf.setInt(OMConfigKeys.OZONE_OM_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY,
          10);
    }

    /**
     * Start OM service with multiple OMs.
     * @return list of OzoneManagers
     * @throws IOException
     * @throws AuthenticationException
     */
    private Map<String, OzoneManager> createOMService() throws IOException,
        AuthenticationException {

      Map<String, OzoneManager> omMap = new HashMap<>();

      int retryCount = 0;
      int basePort = 10000;

      while (true) {
        try {
          basePort = 10000 + RANDOM.nextInt(1000) * 4;
          initHAConfig(basePort);

          for (int i = 1; i<= numOfOMs; i++) {
            // Set nodeId
            String nodeId = nodeIdBaseStr + i;
            conf.set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, nodeId);

            // Set metadata/DB dir base path
            String metaDirPath = path + "/" + nodeId;
            conf.set(OZONE_METADATA_DIRS, metaDirPath);
            OMStorage omStore = new OMStorage(conf);
            initializeOmStorage(omStore);

            // Set HTTP address to the rpc port + 2
            int httpPort = basePort + (6*i) - 4;
            conf.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY,
                "127.0.0.1:" + httpPort);

            OzoneManager om = OzoneManager.createOm(null, conf);
            om.setCertClient(certClient);
            omMap.put(nodeId, om);

            om.start();
            LOG.info("Started OzoneManager RPC server at " +
                om.getOmRpcServerAddr());
          }

          // Set default OM address to point to the first OM. Clients would
          // try connecting to this address by default
          conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY,
              NetUtils.getHostPortString(omMap.get(nodeIdBaseStr + 1)
                  .getOmRpcServerAddr()));

          break;
        } catch (BindException e) {
          for (OzoneManager om : omMap.values()) {
            om.stop();
            om.join();
            LOG.info("Stopping OzoneManager server at " +
                om.getOmRpcServerAddr());
          }
          omMap.clear();
          ++retryCount;
          LOG.info("MiniOzoneHACluster port conflicts, retried " +
              retryCount + " times");
        }
      }
      return omMap;
    }

    /**
     * Initialize HA related configurations.
     */
    private void initHAConfig(int basePort) throws IOException {
      // Set configurations required for starting OM HA service
      conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, omServiceId);
      String omNodesKey = OmUtils.addKeySuffixes(
          OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
      StringBuilder omNodesKeyValue = new StringBuilder();

      int port = basePort;

      for (int i = 1; i <= numOfOMs; i++, port+=6) {
        String omNodeId = nodeIdBaseStr + i;
        omNodesKeyValue.append(",").append(omNodeId);
        String omAddrKey = OmUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodeId);
        String omRatisPortKey = OmUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_RATIS_PORT_KEY, omServiceId, omNodeId);

        conf.set(omAddrKey, "127.0.0.1:" + port);
        // Reserve port+2 for OMs HTTP server
        conf.setInt(omRatisPortKey, port + 4);
      }

      conf.set(omNodesKey, omNodesKeyValue.substring(1));
    }
  }
}
