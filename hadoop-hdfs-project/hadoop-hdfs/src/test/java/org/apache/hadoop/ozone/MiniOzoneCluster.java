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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.ozone.scm.StorageContainerManager;
import org.apache.hadoop.ozone.scm.node.SCMNodeManager;
import org.apache.hadoop.ozone.web.client.OzoneClient;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertFalse;

/**
 * MiniOzoneCluster creates a complete in-process Ozone cluster suitable for
 * running tests.  The cluster consists of a StorageContainerManager, Namenode
 * and multiple DataNodes.  This class subclasses {@link MiniDFSCluster} for
 * convenient reuse of logic for starting DataNodes.
 */
@InterfaceAudience.Private
public class MiniOzoneCluster extends MiniDFSCluster implements Closeable {
  private static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneCluster.class);
  private static final String USER_AUTH = "hdfs";

  private final OzoneConfiguration conf;
  private final StorageContainerManager scm;
  private final Path tempPath;

  /**
   * Creates a new MiniOzoneCluster.
   *
   * @param builder cluster builder
   * @param scm     StorageContainerManager, already running
   * @throws IOException if there is an I/O error
   */
  private MiniOzoneCluster(Builder builder, StorageContainerManager scm)
      throws IOException {
    super(builder);
    this.conf = builder.conf;
    this.scm = scm;
    tempPath = Paths.get(builder.getPath(), builder.getRunID());
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
  }

  @Override
  public void shutdown() {
    super.shutdown();
    LOG.info("Shutting down the Mini Ozone Cluster");
    if (scm == null) {
      return;
    }
    LOG.info("Shutting down the StorageContainerManager");
    scm.stop();
    scm.join();
  }

  /**
   * Creates an {@link OzoneClient} connected to this cluster's REST service.
   * Callers take ownership of the client and must close it when done.
   *
   * @return OzoneClient connected to this cluster's REST service
   * @throws OzoneException if Ozone encounters an error creating the client
   */
  public OzoneClient createOzoneClient() throws OzoneException {
    Preconditions.checkState(!getDataNodes().isEmpty(),
        "Cannot create OzoneClient if the cluster has no DataNodes.");
    // An Ozone request may originate at any DataNode, so pick one at random.
    int dnIndex = new Random().nextInt(getDataNodes().size());
    String uri = String.format("http://127.0.0.1:%d",
        getDataNodes().get(dnIndex).getInfoPort());
    LOG.info("Creating Ozone client to DataNode {} with URI {} and user {}",
        dnIndex, uri, USER_AUTH);
    try {
      return new OzoneClient(uri, USER_AUTH);
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
  public void waitOzoneReady() throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        if (scm.getNodeCount(SCMNodeManager.NODESTATE.HEALTHY)
            >= numDataNodes) {
          return true;
        }
        LOG.info("Waiting for cluster to be ready. Got {} of {} DN Heartbeats.",
            scm.getNodeCount(SCMNodeManager.NODESTATE.HEALTHY),
            numDataNodes);
        return false;
      }
    }, 1000, 5 * 60 * 1000); //wait for 5 mins.
  }

  /**
   * Waits for SCM to be out of Chill Mode. Many tests can be run iff we are out
   * of Chill mode.
   *
   * @throws TimeoutException
   * @throws InterruptedException
   */
  public void waitTobeOutOfChillMode() throws TimeoutException,
      InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        if (scm.getScmNodeManager().isOutOfNodeChillMode()) {
          return true;
        }
        LOG.info("Waiting for cluster to be ready. No datanodes found");
        return false;
      }
    }, 100, 45000);
  }

  /**
   * Builder for configuring the MiniOzoneCluster to run.
   */
  public static class Builder
      extends org.apache.hadoop.hdfs.MiniDFSCluster.Builder {

    private final OzoneConfiguration conf;
    private final int defaultHBSeconds = 1;
    private final int defaultProcessorMs = 100;
    private final String path;
    private final UUID runID;
    private Optional<String> ozoneHandlerType = Optional.absent();
    private Optional<Boolean> enableTrace = Optional.of(true);
    private Optional<Integer> hbSeconds = Optional.absent();
    private Optional<Integer> hbProcessorInterval = Optional.absent();
    private Optional<String> scmMetadataDir = Optional.absent();
    private Boolean ozoneEnabled = true;
    private Boolean waitForChillModeFinish = true;
    private int containerWorkerThreadInterval = 1;

    /**
     * Creates a new Builder.
     *
     * @param conf configuration
     */
    public Builder(OzoneConfiguration conf) {
      super(conf);
      this.conf = conf;

      // TODO : Remove this later, with SCM, NN and SCM can run together.
      //this.nnTopology(new MiniDFSNNTopology()); // No NameNode required

      URL p = conf.getClass().getResource("");
      path = p.getPath().concat(MiniOzoneCluster.class.getSimpleName() + UUID
          .randomUUID().toString());
      runID = UUID.randomUUID();
    }

    @Override
    public Builder numDataNodes(int val) {
      super.numDataNodes(val);
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

    public Builder setSCMContainerWorkerThreadInterval(int intervalInSeconds) {
      containerWorkerThreadInterval = intervalInSeconds;
      return this;
    }

    public String getPath() {
      return path;
    }

    public String getRunID() {
      return runID.toString();
    }

    @Override
    public MiniOzoneCluster build() throws IOException {


      configureHandler();
      configureTrace();
      configureSCMheartbeat();
      configScmMetadata();

      conf.set(OzoneConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(OzoneConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "127.0.0.1:0");


      StorageContainerManager scm = new StorageContainerManager(conf);
      scm.start();
      String addressString =  scm.getDatanodeRpcAddress().getHostString() +
          ":" + scm.getDatanodeRpcAddress().getPort();
      conf.setStrings(OzoneConfigKeys.OZONE_SCM_NAMES, addressString);

      MiniOzoneCluster cluster = new MiniOzoneCluster(this, scm);
      try {
        cluster.waitOzoneReady();
        if (waitForChillModeFinish) {
          cluster.waitTobeOutOfChillMode();
        }
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
        conf.set(OzoneConfigKeys.OZONE_CONTAINER_METADATA_DIRS,
            scmMetadataDir.get());
        return;
      }

      // If user has not specified a path, create a UUID for this miniCluser
      // and create SCM under that directory.
      Path scmPath = Paths.get(path, runID.toString(), "scm");
      Files.createDirectories(scmPath);
      conf.set(OzoneConfigKeys.OZONE_CONTAINER_METADATA_DIRS, scmPath
          .toString());

      // TODO : Fix this, we need a more generic mechanism to map
      // different datanode ID for different datanodes when we have lots of
      // datanodes in the cluster.
      conf.setStrings(OzoneConfigKeys.OZONE_SCM_DATANODE_ID,
          scmPath.toString() + "/datanode.id");

    }

    private void configureHandler() {
      conf.setBoolean(OzoneConfigKeys.OZONE_ENABLED, this.ozoneEnabled);
      if (!ozoneHandlerType.isPresent()) {
        throw new IllegalArgumentException(
            "The Ozone handler type must be specified.");
      }
    }

    private void configureTrace() {
      if (enableTrace.isPresent()) {
        conf.setBoolean(OzoneConfigKeys.OZONE_TRACE_ENABLED_KEY,
            enableTrace.get());
      }
      GenericTestUtils.setLogLevel(org.apache.log4j.Logger.getRootLogger(),
          Level.ALL);
    }

    private void configureSCMheartbeat() {
      if (hbSeconds.isPresent()) {
        conf.setInt(OzoneConfigKeys.OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS,
            hbSeconds.get());

      } else {
        conf.setInt(OzoneConfigKeys.OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS,
            defaultHBSeconds);
      }

      if (hbProcessorInterval.isPresent()) {
        conf.setInt(OzoneConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS,
            hbProcessorInterval.get());
      } else {
        conf.setInt(OzoneConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS,
            defaultProcessorMs);
      }

    }
  }
}
