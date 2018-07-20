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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.hdds.scm.server.SCMStorage;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState
    .HEALTHY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .DFS_CONTAINER_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .DFS_CONTAINER_IPC_RANDOM_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .DFS_CONTAINER_RATIS_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .DFS_CONTAINER_RATIS_IPC_RANDOM_PORT;

/**
 * MiniOzoneCluster creates a complete in-process Ozone cluster suitable for
 * running tests.  The cluster consists of a OzoneManager,
 * StorageContainerManager and multiple DataNodes.
 */
@InterfaceAudience.Private
public final class MiniOzoneClusterImpl implements MiniOzoneCluster {

  private static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneClusterImpl.class);

  private final OzoneConfiguration conf;
  private final StorageContainerManager scm;
  private final OzoneManager ozoneManager;
  private final List<HddsDatanodeService> hddsDatanodes;

  /**
   * Creates a new MiniOzoneCluster.
   *
   * @throws IOException if there is an I/O error
   */
  private MiniOzoneClusterImpl(OzoneConfiguration conf,
                               OzoneManager ozoneManager,
                               StorageContainerManager scm,
                               List<HddsDatanodeService> hddsDatanodes) {
    this.conf = conf;
    this.ozoneManager = ozoneManager;
    this.scm = scm;
    this.hddsDatanodes = hddsDatanodes;
  }

  public OzoneConfiguration getConf() {
    return conf;
  }

  /**
   * Waits for the Ozone cluster to be ready for processing requests.
   */
  @Override
  public void waitForClusterToBeReady()
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      final int healthy = scm.getNodeCount(HEALTHY);
      final boolean isReady = healthy == hddsDatanodes.size();
      LOG.info("{}. Got {} of {} DN Heartbeats.",
          isReady? "Cluster is ready" : "Waiting for cluster to be ready",
          healthy, hddsDatanodes.size());
      return isReady;
    }, 1000, 60 * 1000); //wait for 1 min.
  }

  /**
   * Waits for SCM to be out of Chill Mode. Many tests can be run iff we are out
   * of Chill mode.
   *
   * @throws TimeoutException
   * @throws InterruptedException
   */
  @Override
  public void waitTobeOutOfChillMode()
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      if (scm.getScmNodeManager().isOutOfChillMode()) {
        return true;
      }
      LOG.info("Waiting for cluster to be ready. No datanodes found");
      return false;
    }, 100, 45000);
  }

  @Override
  public StorageContainerManager getStorageContainerManager() {
    return this.scm;
  }

  @Override
  public OzoneManager getOzoneManager() {
    return this.ozoneManager;
  }

  @Override
  public List<HddsDatanodeService> getHddsDatanodes() {
    return hddsDatanodes;
  }

  @Override
  public OzoneClient getClient() throws IOException {
    return OzoneClientFactory.getClient(conf);
  }

  @Override
  public OzoneClient getRpcClient() throws IOException {
    return OzoneClientFactory.getRpcClient(conf);
  }

  /**
   * Creates an {@link OzoneClient} connected to this cluster's REST
   * service. Callers take ownership of the client and must close it when done.
   *
   * @return OzoneRestClient connected to this cluster's REST service
   * @throws OzoneException if Ozone encounters an error creating the client
   */
  @Override
  public OzoneClient getRestClient() throws IOException {
    return OzoneClientFactory.getRestClient(conf);
  }

  /**
   * Returns an RPC proxy connected to this cluster's StorageContainerManager
   * for accessing container location information.  Callers take ownership of
   * the proxy and must close it when done.
   *
   * @return RPC proxy for accessing container location information
   * @throws IOException if there is an I/O error
   */
  @Override
  public StorageContainerLocationProtocolClientSideTranslatorPB
      getStorageContainerLocationClient() throws IOException {
    long version = RPC.getProtocolVersion(
        StorageContainerLocationProtocolPB.class);
    InetSocketAddress address = scm.getClientRpcAddress();
    LOG.info(
        "Creating StorageContainerLocationProtocol RPC client with address {}",
        address);
    return new StorageContainerLocationProtocolClientSideTranslatorPB(
        RPC.getProxy(StorageContainerLocationProtocolPB.class, version,
            address, UserGroupInformation.getCurrentUser(), conf,
            NetUtils.getDefaultSocketFactory(conf),
            Client.getRpcTimeout(conf)));
  }

  @Override
  public void restartStorageContainerManager() throws IOException {
    scm.stop();
    scm.start();
  }

  @Override
  public void restartOzoneManager() throws IOException {
    ozoneManager.stop();
    ozoneManager.start();
  }

  @Override
  public void restartHddsDatanode(int i) {
    HddsDatanodeService datanodeService = hddsDatanodes.get(i);
    datanodeService.stop();
    datanodeService.join();
    // ensure same ports are used across restarts.
    Configuration conf = datanodeService.getConf();
    int currentPort = datanodeService.getDatanodeDetails()
        .getPort(DatanodeDetails.Port.Name.STANDALONE).getValue();
    conf.setInt(DFS_CONTAINER_IPC_PORT, currentPort);
    conf.setBoolean(DFS_CONTAINER_IPC_RANDOM_PORT, false);
    int ratisPort = datanodeService.getDatanodeDetails()
        .getPort(DatanodeDetails.Port.Name.RATIS).getValue();
    conf.setInt(DFS_CONTAINER_RATIS_IPC_PORT, ratisPort);
    conf.setBoolean(DFS_CONTAINER_RATIS_IPC_RANDOM_PORT, false);
    datanodeService.start(null);
  }

  @Override
  public void shutdownHddsDatanode(int i) {
    hddsDatanodes.get(i).stop();
  }

  @Override
  public void shutdown() {
    try {
      LOG.info("Shutting down the Mini Ozone Cluster");

      File baseDir = new File(GenericTestUtils.getTempPath(
          MiniOzoneClusterImpl.class.getSimpleName() + "-" +
              scm.getClientProtocolServer().getScmInfo().getClusterId()));
      FileUtils.deleteDirectory(baseDir);

      if (ozoneManager != null) {
        LOG.info("Shutting down the OzoneManager");
        ozoneManager.stop();
        ozoneManager.join();
      }

      if (scm != null) {
        LOG.info("Shutting down the StorageContainerManager");
        scm.stop();
        scm.join();
      }

      if (!hddsDatanodes.isEmpty()) {
        LOG.info("Shutting down the HddsDatanodes");
        for (HddsDatanodeService hddsDatanode : hddsDatanodes) {
          hddsDatanode.stop();
          hddsDatanode.join();
        }
      }
    } catch (IOException e) {
      LOG.error("Exception while shutting down the cluster.", e);
    }
  }

  /**
   * Builder for configuring the MiniOzoneCluster to run.
   */
  public static class Builder extends MiniOzoneCluster.Builder {

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
      StorageContainerManager scm = createSCM();
      scm.start();
      OzoneManager om = createOM();
      om.start();
      List<HddsDatanodeService> hddsDatanodes = createHddsDatanodes(scm);
      hddsDatanodes.forEach((datanode) -> datanode.start(null));
      return new MiniOzoneClusterImpl(conf, om, scm, hddsDatanodes);
    }

    /**
     * Initializes the configureation required for starting MiniOzoneCluster.
     *
     * @throws IOException
     */
    private void initializeConfiguration() throws IOException {
      conf.setBoolean(OzoneConfigKeys.OZONE_ENABLED, ozoneEnabled);
      Path metaDir = Paths.get(path, "ozone-meta");
      Files.createDirectories(metaDir);
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, metaDir.toString());
      configureTrace();
    }

    /**
     * Creates a new StorageContainerManager instance.
     *
     * @return {@link StorageContainerManager}
     *
     * @throws IOException
     */
    private StorageContainerManager createSCM() throws IOException {
      configureSCM();
      SCMStorage scmStore = new SCMStorage(conf);
      scmStore.setClusterId(clusterId);
      if (!scmId.isPresent()) {
        scmId = Optional.of(UUID.randomUUID().toString());
      }
      scmStore.setScmId(scmId.get());
      scmStore.initialize();
      return StorageContainerManager.createSCM(null, conf);
    }

    /**
     * Creates a new OzoneManager instance.
     *
     * @return {@link OzoneManager}
     *
     * @throws IOException
     */
    private OzoneManager createOM() throws IOException {
      configureOM();
      OMStorage omStore = new OMStorage(conf);
      omStore.setClusterId(clusterId);
      omStore.setScmId(scmId.get());
      omStore.setOmId(omId.orElse(UUID.randomUUID().toString()));
      omStore.initialize();
      return OzoneManager.createOm(null, conf);
    }

    /**
     * Creates HddsDatanodeService(s) instance.
     *
     * @return List of HddsDatanodeService
     *
     * @throws IOException
     */
    private List<HddsDatanodeService> createHddsDatanodes(
        StorageContainerManager scm) throws IOException {
      configureHddsDatanodes();
      String scmAddress =  scm.getDatanodeRpcAddress().getHostString() +
          ":" + scm.getDatanodeRpcAddress().getPort();
      conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, scmAddress);
      List<HddsDatanodeService> hddsDatanodes = new ArrayList<>();
      for (int i = 0; i < numOfDatanodes; i++) {
        Configuration dnConf = new OzoneConfiguration(conf);
        String datanodeBaseDir = path + "/datanode-" + Integer.toString(i);
        Path metaDir = Paths.get(datanodeBaseDir, "meta");
        Path dataDir = Paths.get(datanodeBaseDir, "data", "containers");
        Path ratisDir = Paths.get(datanodeBaseDir, "data", "ratis");
        Files.createDirectories(metaDir);
        Files.createDirectories(dataDir);
        Files.createDirectories(ratisDir);
        dnConf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, metaDir.toString());
        dnConf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDir.toString());
        dnConf.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
            ratisDir.toString());

        hddsDatanodes.add(
            HddsDatanodeService.createHddsDatanodeService(dnConf));
      }
      return hddsDatanodes;
    }

    private void configureSCM() {
      conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "127.0.0.1:0");
      conf.setInt(ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY, numOfScmHandlers);
      configureSCMheartbeat();
    }

    private void configureSCMheartbeat() {
      if (hbInterval.isPresent()) {
        conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL,
            hbInterval.get(), TimeUnit.MILLISECONDS);

      } else {
        conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL,
            DEFAULT_HB_INTERVAL_MS,
            TimeUnit.MILLISECONDS);
      }

      if (hbProcessorInterval.isPresent()) {
        conf.setTimeDuration(
            ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
            hbProcessorInterval.get(),
            TimeUnit.MILLISECONDS);
      } else {
        conf.setTimeDuration(
            ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
            DEFAULT_HB_PROCESSOR_INTERVAL_MS,
            TimeUnit.MILLISECONDS);
      }
    }


    private void configureOM() {
      conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, "127.0.0.1:0");
      conf.setInt(OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY, numOfOmHandlers);
    }

    private void configureHddsDatanodes() {
      conf.set(ScmConfigKeys.HDDS_REST_HTTP_ADDRESS_KEY, "0.0.0.0:0");
      conf.set(HDDS_DATANODE_PLUGINS_KEY,
          "org.apache.hadoop.ozone.web.OzoneHddsDatanodeService");
      conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT,
          randomContainerPort);
      conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT,
          randomContainerPort);
    }

    private void configureTrace() {
      if (enableTrace.isPresent()) {
        conf.setBoolean(OzoneConfigKeys.OZONE_TRACE_ENABLED_KEY,
            enableTrace.get());
        GenericTestUtils.setRootLogLevel(Level.TRACE);
      }
      GenericTestUtils.setRootLogLevel(Level.INFO);
    }
  }
}
