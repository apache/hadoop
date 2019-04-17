/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.hdds.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.test.GenericTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Interface used for MiniOzoneClusters.
 */
public interface MiniOzoneCluster {

  /**
   * Returns the Builder to construct MiniOzoneCluster.
   *
   * @param conf OzoneConfiguration
   *
   * @return MiniOzoneCluster builder
   */
  static Builder newBuilder(OzoneConfiguration conf) {
    return new MiniOzoneClusterImpl.Builder(conf);
  }

  /**
   * Returns the Builder to construct MiniOzoneHACluster.
   *
   * @param conf OzoneConfiguration
   *
   * @return MiniOzoneCluster builder
   */
  static Builder newHABuilder(OzoneConfiguration conf) {
    return new MiniOzoneHAClusterImpl.Builder(conf);
  }

  /**
   * Returns the configuration object associated with the MiniOzoneCluster.
   *
   * @return Configuration
   */
  Configuration getConf();

  /**
   * Waits for the cluster to be ready, this call blocks till all the
   * configured {@link HddsDatanodeService} registers with
   * {@link StorageContainerManager}.
   *
   * @throws TimeoutException In case of timeout
   * @throws InterruptedException In case of interrupt while waiting
   */
  void waitForClusterToBeReady() throws TimeoutException, InterruptedException;

  /**
   * Sets the timeout value after which
   * {@link MiniOzoneCluster#waitForClusterToBeReady} times out.
   *
   * @param timeoutInMs timeout value in milliseconds
   */
  void setWaitForClusterToBeReadyTimeout(int timeoutInMs);

  /**
   * Waits/blocks till the cluster is out of safe mode.
   *
   * @throws TimeoutException TimeoutException In case of timeout
   * @throws InterruptedException In case of interrupt while waiting
   */
  void waitTobeOutOfSafeMode() throws TimeoutException, InterruptedException;

  /**
   * Returns {@link StorageContainerManager} associated with this
   * {@link MiniOzoneCluster} instance.
   *
   * @return {@link StorageContainerManager} instance
   */
  StorageContainerManager getStorageContainerManager();

  /**
   * Returns {@link OzoneManager} associated with this
   * {@link MiniOzoneCluster} instance.
   *
   * @return {@link OzoneManager} instance
   */
  OzoneManager getOzoneManager();

  /**
   * Returns the list of {@link HddsDatanodeService} which are part of this
   * {@link MiniOzoneCluster} instance.
   *
   * @return List of {@link HddsDatanodeService}
   */
  List<HddsDatanodeService> getHddsDatanodes();

  /**
   * Returns an {@link OzoneClient} to access the {@link MiniOzoneCluster}.
   *
   * @return {@link OzoneClient}
   * @throws IOException
   */
  OzoneClient getClient() throws IOException;

  /**
   * Returns an RPC based {@link OzoneClient} to access the
   * {@link MiniOzoneCluster}.
   *
   * @return {@link OzoneClient}
   * @throws IOException
   */
  OzoneClient getRpcClient() throws IOException;

  /**
   * Returns an REST based {@link OzoneClient} to access the
   * {@link MiniOzoneCluster}.
   *
   * @return {@link OzoneClient}
   * @throws IOException
   */
  OzoneClient getRestClient() throws IOException;

  /**
   * Returns StorageContainerLocationClient to communicate with
   * {@link StorageContainerManager} associated with the MiniOzoneCluster.
   *
   * @return StorageContainerLocation Client
   * @throws IOException
   */
  StorageContainerLocationProtocolClientSideTranslatorPB
      getStorageContainerLocationClient() throws IOException;

  /**
   * Restarts StorageContainerManager instance.
   *
   * @param waitForDatanode
   * @throws IOException
   * @throws TimeoutException
   * @throws InterruptedException
   */
  void restartStorageContainerManager(boolean waitForDatanode)
      throws InterruptedException, TimeoutException, IOException,
      AuthenticationException;

  /**
   * Restarts OzoneManager instance.
   *
   * @throws IOException
   */
  void restartOzoneManager() throws IOException;

  /**
   * Restart a particular HddsDatanode.
   *
   * @param i index of HddsDatanode in the MiniOzoneCluster
   */
  void restartHddsDatanode(int i, boolean waitForDatanode)
      throws InterruptedException, TimeoutException;

  int getHddsDatanodeIndex(DatanodeDetails dn) throws IOException;

  /**
   * Restart a particular HddsDatanode.
   *
   * @param dn HddsDatanode in the MiniOzoneCluster
   */
  void restartHddsDatanode(DatanodeDetails dn, boolean waitForDatanode)
      throws InterruptedException, TimeoutException, IOException;
  /**
   * Shutdown a particular HddsDatanode.
   *
   * @param i index of HddsDatanode in the MiniOzoneCluster
   */
  void shutdownHddsDatanode(int i);

  /**
   * Shutdown a particular HddsDatanode.
   *
   * @param dn HddsDatanode in the MiniOzoneCluster
   */
  void shutdownHddsDatanode(DatanodeDetails dn) throws IOException;

  /**
   * Shutdown the MiniOzoneCluster and delete the storage dirs.
   */
  void shutdown();

  /**
   * Stop the MiniOzoneCluster without any cleanup.
   */
  void stop();

  /**
   * Start Scm.
   */
  void startScm() throws IOException;

  /**
   * Start DataNodes.
   */
  void startHddsDatanodes();

  /**
   * Builder class for MiniOzoneCluster.
   */
  @SuppressWarnings("visibilitymodifier")
  abstract class Builder {

    protected static final int DEFAULT_HB_INTERVAL_MS = 1000;
    protected static final int DEFAULT_HB_PROCESSOR_INTERVAL_MS = 100;

    protected final OzoneConfiguration conf;
    protected final String path;

    protected String clusterId;
    protected String omServiceId;
    protected int numOfOMs;

    protected Optional<Boolean> enableTrace = Optional.of(false);
    protected Optional<Integer> hbInterval = Optional.empty();
    protected Optional<Integer> hbProcessorInterval = Optional.empty();
    protected Optional<String> scmId = Optional.empty();
    protected Optional<String> omId = Optional.empty();

    protected Boolean ozoneEnabled = true;
    protected Boolean randomContainerPort = true;
    protected Optional<Integer> chunkSize = Optional.empty();
    protected Optional<Long> streamBufferFlushSize = Optional.empty();
    protected Optional<Long> streamBufferMaxSize = Optional.empty();
    protected Optional<Long> blockSize = Optional.empty();
    protected Optional<StorageUnit> streamBufferSizeUnit = Optional.empty();
    // Use relative smaller number of handlers for testing
    protected int numOfOmHandlers = 20;
    protected int numOfScmHandlers = 20;
    protected int numOfDatanodes = 1;
    protected boolean  startDataNodes = true;
    protected CertificateClient certClient;

    protected Builder(OzoneConfiguration conf) {
      this.conf = conf;
      this.clusterId = UUID.randomUUID().toString();
      this.path = GenericTestUtils.getTempPath(
          MiniOzoneClusterImpl.class.getSimpleName() + "-" + clusterId);
    }

    /**
     * Sets the cluster Id.
     *
     * @param id cluster Id
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setClusterId(String id) {
      clusterId = id;
      return this;
    }

    public Builder setStartDataNodes(boolean nodes) {
      this.startDataNodes = nodes;
      return this;
    }

    /**
     * Sets the certificate client.
     *
     * @param client
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setCertificateClient(CertificateClient client) {
      this.certClient = client;
      return this;
    }

    /**
     * Sets the SCM id.
     *
     * @param id SCM Id
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setScmId(String id) {
      scmId = Optional.of(id);
      return this;
    }

    /**
     * Sets the OM id.
     *
     * @param id OM Id
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setOmId(String id) {
      omId = Optional.of(id);
      return this;
    }

    /**
     * If set to true container service will be started in a random port.
     *
     * @param randomPort enable random port
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setRandomContainerPort(boolean randomPort) {
      randomContainerPort = randomPort;
      return this;
    }

    /**
     * Sets the number of HddsDatanodes to be started as part of
     * MiniOzoneCluster.
     *
     * @param val number of datanodes
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setNumDatanodes(int val) {
      numOfDatanodes = val;
      return this;
    }

    /**
     * Sets the number of HeartBeat Interval of Datanodes, the value should be
     * in MilliSeconds.
     *
     * @param val HeartBeat interval in milliseconds
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setHbInterval(int val) {
      hbInterval = Optional.of(val);
      return this;
    }

    /**
     * Sets the number of HeartBeat Processor Interval of Datanodes,
     * the value should be in MilliSeconds.
     *
     * @param val HeartBeat Processor interval in milliseconds
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setHbProcessorInterval(int val) {
      hbProcessorInterval = Optional.of(val);
      return this;
    }

    /**
     * When set to true, enables trace level logging.
     *
     * @param trace true or false
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setTrace(Boolean trace) {
      enableTrace = Optional.of(trace);
      return this;
    }

    /**
     * Modifies the configuration such that Ozone will be disabled.
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder disableOzone() {
      ozoneEnabled = false;
      return this;
    }

    /**
     * Sets the chunk size.
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setChunkSize(int size) {
      chunkSize = Optional.of(size);
      return this;
    }

    /**
     * Sets the flush size for stream buffer.
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setStreamBufferFlushSize(long size) {
      streamBufferFlushSize = Optional.of(size);
      return this;
    }

    /**
     * Sets the max size for stream buffer.
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setStreamBufferMaxSize(long size) {
      streamBufferMaxSize = Optional.of(size);
      return this;
    }

    /**
     * Sets the block size for stream buffer.
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setBlockSize(long size) {
      blockSize = Optional.of(size);
      return this;
    }

    public Builder setNumOfOzoneManagers(int numOMs) {
      this.numOfOMs = numOMs;
      return this;
    }

    public Builder setStreamBufferSizeUnit(StorageUnit unit) {
      this.streamBufferSizeUnit = Optional.of(unit);
      return this;
    }

    public Builder setOMServiceId(String serviceId) {
      this.omServiceId = serviceId;
      return this;
    }

    /**
     * Constructs and returns MiniOzoneCluster.
     *
     * @return {@link MiniOzoneCluster}
     *
     * @throws IOException
     */
    public abstract MiniOzoneCluster build() throws IOException;
  }
}