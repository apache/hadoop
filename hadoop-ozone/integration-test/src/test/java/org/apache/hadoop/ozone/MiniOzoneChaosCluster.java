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

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Level;
import org.apache.ratis.grpc.client.GrpcClientProtocolClient;
import org.apache.ratis.util.LogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;

/**
 * This class causes random failures in the chaos cluster.
 */
public class MiniOzoneChaosCluster extends MiniOzoneClusterImpl {

  static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneChaosCluster.class);

  private final int numDatanodes;
  private final ScheduledExecutorService executorService;

  private ScheduledFuture scheduledFuture;

  private enum FailureMode {
    NODES
  }

  public MiniOzoneChaosCluster(OzoneConfiguration conf,
                               OzoneManager ozoneManager,
                       StorageContainerManager scm,
                       List<HddsDatanodeService> hddsDatanodes) {
    super(conf, ozoneManager, scm, hddsDatanodes);

    this.executorService =  Executors.newSingleThreadScheduledExecutor();
    this.numDatanodes = getHddsDatanodes().size();
    LOG.info("Starting MiniOzoneChaosCluster with:{} datanodes" + numDatanodes);
    LogUtils.setLogLevel(GrpcClientProtocolClient.LOG, Level.WARN);
  }

  // Get the number of datanodes to fail in the cluster.
  private int getNumberOfNodesToFail() {
    return RandomUtils.nextBoolean() ? 1 : 2;
  }

  // Should the failed node wait for SCM to register the even before
  // restart, i.e fast restart or not.
  private boolean isFastRestart() {
    return RandomUtils.nextBoolean();
  }

  // Get the datanode index of the datanode to fail.
  private int getNodeToFail() {
    return RandomUtils.nextInt() % numDatanodes;
  }

  private void failNodes() {
    final int numNodesToFail = getNumberOfNodesToFail();
    LOG.info("Will restart {} nodes to simulate failure", numNodesToFail);
    for (int i = 0; i < numNodesToFail; i++) {
      boolean failureMode = isFastRestart();
      int failedNodeIndex = getNodeToFail();
      try {
        LOG.info("Restarting DataNodeIndex {}", failedNodeIndex);
        restartHddsDatanode(failedNodeIndex, failureMode);
        LOG.info("Completed restarting DataNodeIndex {}", failedNodeIndex);
      } catch (Exception e) {

      }
    }
  }

  private FailureMode getFailureMode() {
    return FailureMode.
        values()[RandomUtils.nextInt() % FailureMode.values().length];
  }

  // Fail nodes randomly at configured timeout period.
  private void fail() {
    FailureMode mode = getFailureMode();
    switch (mode) {
    case NODES:
      failNodes();
      break;

    default:
      LOG.error("invalid failure mode:{}", mode);
      break;
    }
  }

  void startChaos(long initialDelay, long period, TimeUnit timeUnit) {
    LOG.info("Starting Chaos with failure period:{} unit:{} numDataNodes:{}",
        period, timeUnit, numDatanodes);
    scheduledFuture = executorService.scheduleAtFixedRate(this::fail,
        initialDelay, period, timeUnit);
  }

  void stopChaos() throws Exception {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(false);
      scheduledFuture.get();
    }
  }

  public void shutdown() {
    try {
      stopChaos();
      executorService.shutdown();
      executorService.awaitTermination(1, TimeUnit.DAYS);
      //this should be called after stopChaos to be sure that the
      //datanode collection is not modified during the shutdown
      super.shutdown();
    } catch (Exception e) {
      LOG.error("failed to shutdown MiniOzoneChaosCluster", e);
    }
  }

  /**
   * Builder for configuring the MiniOzoneChaosCluster to run.
   */
  public static class Builder extends MiniOzoneClusterImpl.Builder {

    /**
     * Creates a new Builder.
     *
     * @param conf configuration
     */
    public Builder(OzoneConfiguration conf) {
      super(conf);
    }

    /**
     * Sets the number of HddsDatanodes to be started as part of
     * MiniOzoneChaosCluster.
     *
     * @param val number of datanodes
     *
     * @return MiniOzoneChaosCluster.Builder
     */
    public Builder setNumDatanodes(int val) {
      super.setNumDatanodes(val);
      return this;
    }

    @Override
    void initializeConfiguration() throws IOException {
      super.initializeConfiguration();
      conf.setStorageSize(ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY,
          2, StorageUnit.KB);
      conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
          16, StorageUnit.KB);
      conf.setStorageSize(OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_FLUSH_SIZE,
          4, StorageUnit.KB);
      conf.setStorageSize(OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_MAX_SIZE,
          8, StorageUnit.KB);
      conf.setStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
          1, StorageUnit.MB);
      conf.setTimeDuration(ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT, 1000,
          TimeUnit.MILLISECONDS);
      conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL, 5,
          TimeUnit.SECONDS);
      conf.setTimeDuration(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, 1,
          TimeUnit.SECONDS);
      conf.setTimeDuration(HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL, 1,
          TimeUnit.SECONDS);
      conf.setTimeDuration(
          ScmConfigKeys.OZONE_SCM_CONTAINER_CREATION_LEASE_TIMEOUT, 5,
          TimeUnit.SECONDS);
      conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
          1, TimeUnit.SECONDS);
      conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, 1,
          TimeUnit.SECONDS);
      conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE, 8);
    }

    @Override
    public MiniOzoneChaosCluster build() throws IOException {
      DefaultMetricsSystem.setMiniClusterMode(true);
      initializeConfiguration();
      StorageContainerManager scm;
      OzoneManager om;
      try {
        scm = createSCM();
        scm.start();
        om = createOM();
        if(certClient != null) {
          om.setCertClient(certClient);
        }
      } catch (AuthenticationException ex) {
        throw new IOException("Unable to build MiniOzoneCluster. ", ex);
      }

      om.start();
      final List<HddsDatanodeService> hddsDatanodes = createHddsDatanodes(scm);
      MiniOzoneChaosCluster cluster =
          new MiniOzoneChaosCluster(conf, om, scm, hddsDatanodes);
      if (startDataNodes) {
        cluster.startHddsDatanodes();
      }
      return cluster;
    }
  }
}
