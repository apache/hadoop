/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BPServiceActorAttributes;
import org.apache.hadoop.util.Time;

/**
 * Datanode health checker. If dfs.datanode.health.activennconnect.timeout is configured with
 * value in milliseconds > 0, the health checker will periodically ensure that the given datanode
 * is healthy and stays connected to active namenode. If the datanode cannot stay healthy or
 * connected to the active namenode for the given time duration, the datanode health checker
 * would try to shut down the datanode.
 */
@InterfaceAudience.Private
public class DataNodeHealthChecker {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeHealthChecker.class);

  public static final String DFS_DATANODE_HEATHCHECK_RUN_INTERVAL =
      "dfs.datanode.heathcheck.run.interval";
  private static final long DFS_DATANODE_HEATHCHECK_RUN_INTERVAL_DEFAULT = 2000;

  public static final String DFS_DATANODE_HEATHCHECK_RUN_INIT_DELAY =
      "dfs.datanode.heathcheck.run.init.delay";
  private static final long DFS_DATANODE_HEATHCHECK_RUN_INIT_DELAY_DEFAULT = 5000;

  private final DataNode dataNode;
  private ScheduledExecutorService executorService;
  private final long executorServicePeriod;
  private final long executorServiceInitDelay;
  private final long activeNNConnectionTimeoutForHealthyDN;

  public DataNodeHealthChecker(DataNode dataNode) {
    this.dataNode = dataNode;
    this.executorService = null;
    this.executorServicePeriod = this.dataNode.getConf()
        .getLong(DFS_DATANODE_HEATHCHECK_RUN_INTERVAL,
            DFS_DATANODE_HEATHCHECK_RUN_INTERVAL_DEFAULT);
    this.executorServiceInitDelay = this.dataNode.getConf()
        .getLong(DFS_DATANODE_HEATHCHECK_RUN_INIT_DELAY,
            DFS_DATANODE_HEATHCHECK_RUN_INIT_DELAY_DEFAULT);
    this.activeNNConnectionTimeoutForHealthyDN = this.dataNode.getConf()
        .getLong(DFSConfigKeys.DFS_DATANODE_HEALTH_ACTIVENNCONNECT_TIMEOUT,
            DFSConfigKeys.DFS_DATANODE_HEALTH_ACTIVENNCONNECT_TIMEOUT_DEFAULT);
  }

  public void initiateHealthCheck() {
    if (this.activeNNConnectionTimeoutForHealthyDN > 0) {
      this.executorService = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("DN-HealthCheck").build());
      long initCheckTime = Time.monotonicNow();
      Runnable runnable =
          new DataNodeHeathCheckThread(this.dataNode, this.activeNNConnectionTimeoutForHealthyDN,
              initCheckTime);
      executorService.scheduleWithFixedDelay(runnable, this.executorServiceInitDelay,
          this.executorServicePeriod, TimeUnit.MILLISECONDS);
    } else {
      LOGGER.trace("To terminate the datanode if it does not stay healthy or connected to "
              + "active namenode for given time duration, provide positive time duration "
              + "value in millies for config {}", DFSConfigKeys.DFS_DATANODE_HEALTH_ACTIVENNCONNECT_TIMEOUT);
    }
  }

  public void shutdown() {
    if (executorService != null) {
      executorService.shutdown();
      try {
        executorService.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOGGER.warn("Interrupted while awaiting the datanode health check service termination",
            e);
        Thread.currentThread().interrupt();
      }
    }
  }

  private static final class DataNodeHeathCheckThread implements Runnable {

    private final DataNode dataNode;
    private final long activeNNConnectionTimeoutForHealthyDN; // value > 0
    private long initCheckTime;
    private boolean isErrorLogged = false;

    private DataNodeHeathCheckThread(DataNode dataNode,
        long activeNNConnectionTimeoutForHealthyDN, long initCheckTime) {
      this.dataNode = dataNode;
      this.activeNNConnectionTimeoutForHealthyDN = activeNNConnectionTimeoutForHealthyDN;
      this.initCheckTime = initCheckTime;
    }

    @Override
    public void run() {
      if (this.dataNode.isDatanodeFullyStarted(true)) {
        this.initCheckTime = Time.monotonicNow();
        checkForActiveNamenodeHeartbeatResponse();
      } else {
        LOGGER.trace("Datanode {} is not fully started and connected to active namenode yet.",
            this.dataNode);
        if ((Time.monotonicNow() - this.initCheckTime)
            > this.activeNNConnectionTimeoutForHealthyDN) {
          attemptToShutDownDatanode();
        }
      }
    }

    private void checkForActiveNamenodeHeartbeatResponse() {
      List<Map<String, String>> bpServiceActorInfoList = dataNode.getBPServiceActorInfoMap();
      Set<String> blockPoolsConnectedToActiveNN = new HashSet<>();
      Set<String> allBlockPools = new HashSet<>();
      for (Map<String, String> bpServiceActorInfo : bpServiceActorInfoList) {
        String blockPoolId =
            bpServiceActorInfo.get(BPServiceActorAttributes.BLOCK_POOL_ID.toString());
        if (blockPoolsConnectedToActiveNN.contains(blockPoolId)) {
          continue;
        }
        String connectedNamenodeStatus =
            bpServiceActorInfo.get(BPServiceActorAttributes.NAMENODE_HA_STATE.toString());
        if (!HAServiceProtocol.HAServiceState.ACTIVE.toString().equals(connectedNamenodeStatus)) {
          continue;
        }
        String lastHeartbeatResp = bpServiceActorInfo.get(
            BPServiceActorAttributes.LAST_HEARTBEAT_RESPONSE_TIME.toString());
        // last heartbeat response time, convert from seconds to milliseconds
        long lastHeartbeatResponseTime =
            TimeUnit.SECONDS.toMillis(Long.parseLong(lastHeartbeatResp));

        // all BPs are added here.
        allBlockPools.add(blockPoolId);
        // only if the given BP has heartbeat response from active namenode within the
        // time duration allowed, add BP in the blockPoolsConnectedToActiveNN set, else
        // consider it as candidate for datanode shutdown.
        if (lastHeartbeatResponseTime <= this.activeNNConnectionTimeoutForHealthyDN) {
          blockPoolsConnectedToActiveNN.add(blockPoolId);
        } else {
          LOGGER.info(
              "For BP id {}, last heartbeat response time: {}, "
                  + "active namenode connection timeout: {}", blockPoolId,
              lastHeartbeatResponseTime, activeNNConnectionTimeoutForHealthyDN);
        }
      }
      // if at least one BP offer service is not able to receive heartbeat response from active
      // namenode within activeNNConnectionTimeoutForHealthyDN ms, terminate the datanode.
      if (allBlockPools.size() > blockPoolsConnectedToActiveNN.size()) {
        LOGGER.debug("All BPs: {}, BPs connected to active namenode: {}", allBlockPools,
            blockPoolsConnectedToActiveNN);
        attemptToShutDownDatanode();
      }
    }

    private void attemptToShutDownDatanode() {
      if (!isErrorLogged) {
        LOGGER.error(
            "Datanode {} has not stayed healthy and connected to active namenode for {}ms. "
                + "Will attempt to stop the datanode.", this.dataNode,
            this.activeNNConnectionTimeoutForHealthyDN);
        // just in case the scheduler makes multiple attempts to shut down datanode, it will
        // not cause any issues in the datanode shut down, however we can avoid logging
        // the same error log for multiple attempts made by the multiple scheduler runs.
        isErrorLogged = true;
      }
      try {
        // this shuts down datanode asynchronously
        this.dataNode.shutdownDatanode(false);
      } catch (IOException e) {
        if (!DataNode.SHUTDOWN_ALREADY_IN_PROGRESS.equals(e.getMessage())) {
          LOGGER.error("Error while calling datanode shutdown.", e);
        }
      }
    }
  }

}
