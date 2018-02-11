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
import java.util.Optional;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.container.common
    .statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.ksm.KSMConfigKeys;
import org.apache.hadoop.ozone.ksm.KeySpaceManager;
import org.apache.hadoop.ozone.scm.SCMStorage;
import org.apache.hadoop.ozone.ksm.KSMStorage;
import org.apache.hadoop.ozone.web.client.OzoneRestClient;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.ozone.scm.StorageContainerManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.OzoneConfigKeys
    .DFS_CONTAINER_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .DFS_CONTAINER_IPC_RANDOM_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .DFS_CONTAINER_RATIS_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .DFS_CONTAINER_RATIS_IPC_RANDOM_PORT;

import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos.NodeState
    .HEALTHY;
import static org.junit.Assert.assertFalse;

/**
 * MiniOzoneCluster creates a complete in-process Ozone cluster suitable for
 * running tests.  The cluster consists of a StorageContainerManager, Namenode
 * and multiple DataNodes.  This class subclasses {@link MiniDFSCluster} for
 * convenient reuse of logic for starting DataNodes.
 */
@InterfaceAudience.Private
public final class MiniOzoneClassicCluster extends MiniDFSCluster
    implements MiniOzoneCluster {
  private static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneClassicCluster.class);
  private static final String USER_AUTH = "hdfs";

  private final OzoneConfiguration conf;
  private final StorageContainerManager scm;
  private KeySpaceManager ksm;
  private final Path tempPath;

  /**
   * Creates a new MiniOzoneCluster.
   *
   * @param builder cluster builder
   * @param scm     StorageContainerManager, already running
   * @throws IOException if there is an I/O error
   */
  private MiniOzoneClassicCluster(Builder builder, StorageContainerManager scm,
                           KeySpaceManager ksm)
      throws IOException {
    super(builder);
    this.conf = builder.conf;
    this.scm = scm;
    this.ksm = ksm;
    tempPath = Paths.get(builder.getPath(), builder.getRunID());
  }


  @Override
  protected void setupDatanodeAddress(
      int i, Configuration dnConf, boolean setupHostsFile,
      boolean checkDnAddrConf) throws IOException {
    super.setupDatanodeAddress(i, dnConf, setupHostsFile, checkDnAddrConf);
    setConf(i, dnConf, OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
        getInstanceStorageDir(i, -1).getCanonicalPath());
    String containerMetaDirs = dnConf.get(
        OzoneConfigKeys.OZONE_METADATA_DIRS) + "-dn-" + i;
    Path containerMetaDirPath = Paths.get(containerMetaDirs);
    setConf(i, dnConf, OzoneConfigKeys.OZONE_METADATA_DIRS,
        containerMetaDirs);
    Path containerRootPath =
        containerMetaDirPath.resolve(OzoneConsts.CONTAINER_ROOT_PREFIX);
    Files.createDirectories(containerRootPath);
  }

  static void setConf(int i, Configuration conf, String key, String value) {
    conf.set(key, value);
    LOG.info("dn{}: set {} = {}", i, key, value);
  }

  @Override
  public void close() {
    shutdown();
    try {
      FileUtils.deleteDirectory(tempPath.toFile());
    } catch (IOException e) {
      String errorMessage = "Cleaning up metadata directories failed." + e;
      assertFalse(errorMessage, true);
    }

    try {
      final String localStorage =
          conf.getTrimmed(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
              OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);
      FileUtils.deleteDirectory(new File(localStorage));
    } catch (IOException e) {
      LOG.error("Cleaning up local storage failed", e);
    }
  }

  @Override
  public boolean restartDataNode(int i) throws IOException {
    return restartDataNode(i, true);
  }
  /*
   * Restart a particular datanode, wait for it to become active
   */
  @Override
  public boolean restartDataNode(int i, boolean keepPort) throws IOException {
    LOG.info("restarting datanode:{} keepPort:{}", i, keepPort);
    if (keepPort) {
      DataNodeProperties dnProp = dataNodes.get(i);
      OzoneContainer container =
          dnProp.getDatanode().getOzoneContainerManager();
      Configuration config = dnProp.getConf();
      int currentPort = container.getContainerServerPort();
      config.setInt(DFS_CONTAINER_IPC_PORT, currentPort);
      config.setBoolean(DFS_CONTAINER_IPC_RANDOM_PORT, false);
      int ratisPort = container.getRatisContainerServerPort();
      config.setInt(DFS_CONTAINER_RATIS_IPC_PORT, ratisPort);
      config.setBoolean(DFS_CONTAINER_RATIS_IPC_RANDOM_PORT, false);
    }
    boolean status =  super.restartDataNode(i, keepPort);

    try {
      this.waitActive();
      waitDatanodeOzoneReady(i);
    } catch (TimeoutException | InterruptedException e) {
      Thread.interrupted();
    }
    return status;
  }

  @Override
  public void shutdown() {
    super.shutdown();
    LOG.info("Shutting down the Mini Ozone Cluster");

    if (ksm != null) {
      LOG.info("Shutting down the keySpaceManager");
      ksm.stop();
      ksm.join();
    }

    if (scm != null) {
      LOG.info("Shutting down the StorageContainerManager");
      scm.stop();
      scm.join();
    }
  }

  @Override
  public StorageContainerManager getStorageContainerManager() {
    return this.scm;
  }

  public OzoneConfiguration getConf() {
    return conf;
  }

  @Override
  public KeySpaceManager getKeySpaceManager() {
    return this.ksm;
  }

  /**
   * Creates an {@link OzoneRestClient} connected to this cluster's REST
   * service. Callers take ownership of the client and must close it when done.
   *
   * @return OzoneRestClient connected to this cluster's REST service
   * @throws OzoneException if Ozone encounters an error creating the client
   */
  @Override
  public OzoneRestClient createOzoneRestClient() throws OzoneException {
    Preconditions.checkState(!getDataNodes().isEmpty(),
        "Cannot create OzoneRestClient if the cluster has no DataNodes.");
    // An Ozone request may originate at any DataNode, so pick one at random.
    int dnIndex = new Random().nextInt(getDataNodes().size());
    String uri = String.format("http://127.0.0.1:%d",
        getDataNodes().get(dnIndex).getInfoPort());
    LOG.info("Creating Ozone client to DataNode {} with URI {} and user {}",
        dnIndex, uri, USER_AUTH);
    try {
      return new OzoneRestClient(uri, USER_AUTH);
    } catch (URISyntaxException e) {
      // We control the REST service URI, so it should never be invalid.
      throw new IllegalStateException("Unexpected URISyntaxException", e);
    }
  }

  /**
   * Creates an RPC proxy connected to this cluster's StorageContainerManager
   * for accessing container location information.  Callers take ownership of
   * the proxy and must close it when done.
   *
   * @return RPC proxy for accessing container location information
   * @throws IOException if there is an I/O error
   */
  @Override
  public StorageContainerLocationProtocolClientSideTranslatorPB
      createStorageContainerLocationClient() throws IOException {
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

  /**
   * Waits for the Ozone cluster to be ready for processing requests.
   */
  @Override
  public void waitOzoneReady() throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      final int healthy = scm.getNodeCount(HEALTHY);
      final boolean isReady = healthy >= numDataNodes;
      LOG.info("{}. Got {} of {} DN Heartbeats.",
            isReady? "Cluster is ready" : "Waiting for cluster to be ready",
            healthy, numDataNodes);
      return isReady;
    }, 1000, 60 * 1000); //wait for 1 min.
  }

  /**
   * Waits for a particular Datanode to be ready for processing ozone requests.
   */
  @Override
  public void waitDatanodeOzoneReady(int dnIndex)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      DatanodeStateMachine.DatanodeStates state =
          dataNodes.get(dnIndex).getDatanode().getOzoneStateMachineState();
      final boolean rebootComplete =
          (state == DatanodeStateMachine.DatanodeStates.RUNNING);
      LOG.info("{} Current state:{}", rebootComplete, state);
      return rebootComplete;
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
  public void waitTobeOutOfChillMode() throws TimeoutException,
      InterruptedException {
    GenericTestUtils.waitFor(() -> {
      if (scm.getScmNodeManager().isOutOfChillMode()) {
        return true;
      }
      LOG.info("Waiting for cluster to be ready. No datanodes found");
      return false;
    }, 100, 45000);
  }

  @Override
  public void waitForHeartbeatProcessed() throws TimeoutException,
      InterruptedException {
    GenericTestUtils.waitFor(() ->
            scm.getScmNodeManager().waitForHeartbeatProcessed(), 100,
        4 * 1000);
    GenericTestUtils.waitFor(() ->
            scm.getScmNodeManager().getStats().getCapacity().get() > 0, 100,
        4 * 1000);
  }

  /**
   * Builder for configuring the MiniOzoneCluster to run.
   */
  public static class Builder
      extends MiniDFSCluster.Builder {

    private final OzoneConfiguration conf;
    private static final int DEFAULT_HB_SECONDS = 1;
    private static final int DEFAULT_PROCESSOR_MS = 100;
    private final String path;
    private final UUID runID;
    private Optional<String> ozoneHandlerType = java.util.Optional.empty();
    private Optional<Boolean> enableTrace = Optional.of(false);
    private Optional<Integer> hbSeconds = Optional.empty();
    private Optional<Integer> hbProcessorInterval = Optional.empty();
    private Optional<String> scmMetadataDir = Optional.empty();
    private Optional<String> clusterId = Optional.empty();
    private Optional<String> scmId = Optional.empty();
    private Optional<String> ksmId = Optional.empty();
    private Boolean ozoneEnabled = true;
    private Boolean waitForChillModeFinish = true;
    private Boolean randomContainerPort = true;
    // Use relative smaller number of handlers for testing
    private int numOfKsmHandlers = 20;
    private int numOfScmHandlers = 20;

    /**
     * Creates a new Builder.
     *
     * @param conf configuration
     */
    public Builder(OzoneConfiguration conf) {
      super(conf);
      // Mini Ozone cluster will not come up if the port is not true, since
      // Ratis will exit if the server port cannot be bound. We can remove this
      // hard coding once we fix the Ratis default behaviour.
      conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT,
          true);
      this.conf = conf;
      path = GenericTestUtils.getTempPath(
          MiniOzoneClassicCluster.class.getSimpleName() +
          UUID.randomUUID().toString());
      runID = UUID.randomUUID();
    }

    public Builder setRandomContainerPort(boolean randomPort) {
      this.randomContainerPort = randomPort;
      return this;
    }

    @Override
    public Builder numDataNodes(int val) {
      super.numDataNodes(val);
      return this;
    }

    @Override
    public Builder storageCapacities(long[] capacities) {
      super.storageCapacities(capacities);
      return this;
    }

    public Builder setHandlerType(String handler) {
      ozoneHandlerType = Optional.of(handler);
      return this;
    }

    public Builder setTrace(Boolean trace) {
      enableTrace = Optional.of(trace);
      return this;
    }

    public Builder setSCMHBInterval(int seconds) {
      hbSeconds = Optional.of(seconds);
      return this;
    }

    public Builder setSCMHeartbeatProcessingInterval(int milliseconds) {
      hbProcessorInterval = Optional.of(milliseconds);
      return this;
    }

    public Builder setSCMMetadataDir(String scmMetadataDirPath) {
      scmMetadataDir = Optional.of(scmMetadataDirPath);
      return this;
    }

    public Builder disableOzone() {
      ozoneEnabled = false;
      return this;
    }

    public Builder doNotwaitTobeOutofChillMode() {
      waitForChillModeFinish = false;
      return this;
    }

    public Builder setNumOfKSMHandlers(int numOfHandlers) {
      numOfKsmHandlers = numOfHandlers;
      return this;
    }

    public Builder setNumOfSCMHandlers(int numOfHandlers) {
      numOfScmHandlers = numOfHandlers;
      return this;
    }

    public Builder setClusterId(String cId) {
      clusterId = Optional.of(cId);
      return this;
    }

    public Builder setScmId(String sId) {
      scmId = Optional.of(sId);
      return this;
    }

    public Builder setKsmId(String kId) {
      ksmId = Optional.of(kId);
      return this;
    }

    public String getPath() {
      return path;
    }

    public String getRunID() {
      return runID.toString();
    }

    @Override
    public MiniOzoneClassicCluster build() throws IOException {


      configureHandler();
      configureTrace();
      configureSCMheartbeat();
      configScmMetadata();
      initializeScm();
      initializeKSM();

      conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(KSMConfigKeys.OZONE_KSM_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(KSMConfigKeys.OZONE_KSM_HTTP_ADDRESS_KEY, "127.0.0.1:0");

      // Configure KSM and SCM handlers
      conf.setInt(ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY, numOfScmHandlers);
      conf.setInt(KSMConfigKeys.OZONE_KSM_HANDLER_COUNT_KEY, numOfKsmHandlers);

      // Use random ports for ozone containers in mini cluster,
      // in order to launch multiple container servers per node.
      conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT,
          randomContainerPort);

      StorageContainerManager scm = StorageContainerManager.createSCM(
          null, conf);
      scm.start();

      KeySpaceManager ksm = KeySpaceManager.createKSM(null, conf);
      ksm.start();

      String addressString =  scm.getDatanodeRpcAddress().getHostString() +
          ":" + scm.getDatanodeRpcAddress().getPort();
      conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, addressString);

      MiniOzoneClassicCluster cluster =
          new MiniOzoneClassicCluster(this, scm, ksm);
      try {
        cluster.waitOzoneReady();
        if (waitForChillModeFinish) {
          cluster.waitTobeOutOfChillMode();
        }
        cluster.waitForHeartbeatProcessed();
      } catch (Exception e) {
        // A workaround to propagate MiniOzoneCluster failures without
        // changing the method signature (which would require cascading
        // changes to hundreds of unrelated HDFS tests).
        throw new IOException("Failed to start MiniOzoneCluster", e);
      }
      return cluster;
    }

    private void configScmMetadata() throws IOException {


      if (scmMetadataDir.isPresent()) {
        // if user specifies a path in the test, it is assumed that user takes
        // care of creating and cleaning up that directory after the tests.
        conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
            scmMetadataDir.get());
        return;
      }

      // If user has not specified a path, create a UUID for this miniCluster
      // and create SCM under that directory.
      Path scmPath = Paths.get(path, runID.toString(), "cont-meta");
      Files.createDirectories(scmPath);
      Path containerPath = scmPath.resolve(OzoneConsts.CONTAINER_ROOT_PREFIX);
      Files.createDirectories(containerPath);
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, scmPath
          .toString());

      // TODO : Fix this, we need a more generic mechanism to map
      // different datanode ID for different datanodes when we have lots of
      // datanodes in the cluster.
      conf.setStrings(ScmConfigKeys.OZONE_SCM_DATANODE_ID,
          scmPath.toString() + "/datanode.id");
    }

    private void initializeScm() throws IOException {
      SCMStorage scmStore = new SCMStorage(conf);
      if (!clusterId.isPresent()) {
        clusterId = Optional.of(runID.toString());
      }
      scmStore.setClusterId(clusterId.get());
      if (!scmId.isPresent()) {
        scmId = Optional.of(UUID.randomUUID().toString());
      }
      scmStore.setScmId(scmId.get());
      scmStore.initialize();
    }

    private void initializeKSM() throws IOException {
      KSMStorage ksmStore = new KSMStorage(conf);
      ksmStore.setClusterId(clusterId.get());
      ksmStore.setScmId(scmId.get());
      ksmStore.setKsmId(ksmId.orElse(UUID.randomUUID().toString()));
      ksmStore.initialize();
    }

    private void configureHandler() {
      conf.setBoolean(OzoneConfigKeys.OZONE_ENABLED, this.ozoneEnabled);
      if (!ozoneHandlerType.isPresent()) {
        throw new IllegalArgumentException(
            "The Ozone handler type must be specified.");
      } else {
        conf.set(OzoneConfigKeys.OZONE_HANDLER_TYPE_KEY,
            ozoneHandlerType.get());
      }
    }

    private void configureTrace() {
      if (enableTrace.isPresent()) {
        conf.setBoolean(OzoneConfigKeys.OZONE_TRACE_ENABLED_KEY,
            enableTrace.get());
        GenericTestUtils.setRootLogLevel(Level.TRACE);
      }
      GenericTestUtils.setRootLogLevel(Level.INFO);
    }

    private void configureSCMheartbeat() {
      if (hbSeconds.isPresent()) {
        conf.getTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_INTERVAL,
            hbSeconds.get(), TimeUnit.SECONDS);

      } else {
        conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_INTERVAL,
            DEFAULT_HB_SECONDS,
            TimeUnit.SECONDS);
      }

      if (hbProcessorInterval.isPresent()) {
        conf.setTimeDuration(
            ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
            hbProcessorInterval.get(),
            TimeUnit.MILLISECONDS);
      } else {
        conf.setTimeDuration(
            ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
            DEFAULT_PROCESSOR_MS,
            TimeUnit.MILLISECONDS);
      }

    }
  }
}
