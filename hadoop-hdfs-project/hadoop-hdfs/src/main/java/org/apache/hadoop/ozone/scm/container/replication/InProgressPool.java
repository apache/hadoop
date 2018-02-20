/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.scm.container.replication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.protocol.commands.SendContainerCommand;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.NodeState;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerInfo;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.ozone.scm.node.NodePoolManager;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.Uninterruptibles
    .sleepUninterruptibly;
import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos
    .NodeState.HEALTHY;
import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos
    .NodeState.STALE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos
    .NodeState.UNKNOWN;

/**
 * These are pools that are actively checking for replication status of the
 * containers.
 */
public final class InProgressPool {
  public static final Logger LOG =
      LoggerFactory.getLogger(InProgressPool.class);

  private final PeriodicPool pool;
  private final NodeManager nodeManager;
  private final NodePoolManager poolManager;
  private final ExecutorService executorService;
  private final Map<String, Integer> containerCountMap;
  private final Map<String, Boolean> processedNodeSet;
  private final long startTime;
  private ProgressStatus status;
  private AtomicInteger nodeCount;
  private AtomicInteger nodeProcessed;
  private AtomicInteger containerProcessedCount;
  private long maxWaitTime;
  /**
   * Constructs an pool that is being processed.
   *  @param maxWaitTime - Maximum wait time in milliseconds.
   * @param pool - Pool that we are working against
   * @param nodeManager - Nodemanager
   * @param poolManager - pool manager
   * @param executorService - Shared Executor service.
   */
  InProgressPool(long maxWaitTime, PeriodicPool pool,
      NodeManager nodeManager, NodePoolManager poolManager,
                 ExecutorService executorService) {
    Preconditions.checkNotNull(pool);
    Preconditions.checkNotNull(nodeManager);
    Preconditions.checkNotNull(poolManager);
    Preconditions.checkNotNull(executorService);
    Preconditions.checkArgument(maxWaitTime > 0);
    this.pool = pool;
    this.nodeManager = nodeManager;
    this.poolManager = poolManager;
    this.executorService = executorService;
    this.containerCountMap = new ConcurrentHashMap<>();
    this.processedNodeSet = new ConcurrentHashMap<>();
    this.maxWaitTime = maxWaitTime;
    startTime = Time.monotonicNow();
  }

  /**
   * Returns periodic pool.
   *
   * @return PeriodicPool
   */
  public PeriodicPool getPool() {
    return pool;
  }

  /**
   * We are done if we have got reports from all nodes or we have
   * done waiting for the specified time.
   *
   * @return true if we are done, false otherwise.
   */
  public boolean isDone() {
    return (nodeCount.get() == nodeProcessed.get()) ||
        (this.startTime + this.maxWaitTime) > Time.monotonicNow();
  }

  /**
   * Gets the number of containers processed.
   *
   * @return int
   */
  public int getContainerProcessedCount() {
    return containerProcessedCount.get();
  }

  /**
   * Returns the start time in milliseconds.
   *
   * @return - Start Time.
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Get the number of nodes in this pool.
   *
   * @return - node count
   */
  public int getNodeCount() {
    return nodeCount.get();
  }

  /**
   * Get the number of nodes that we have already processed container reports
   * from.
   *
   * @return - Processed count.
   */
  public int getNodeProcessed() {
    return nodeProcessed.get();
  }

  /**
   * Returns the current status.
   *
   * @return Status
   */
  public ProgressStatus getStatus() {
    return status;
  }

  /**
   * Starts the reconciliation process for all the nodes in the pool.
   */
  public void startReconciliation() {
    List<DatanodeID> datanodeIDList =
        this.poolManager.getNodes(pool.getPoolName());
    if (datanodeIDList.size() == 0) {
      LOG.error("Datanode list for {} is Empty. Pool with no nodes ? ",
          pool.getPoolName());
      this.status = ProgressStatus.Error;
      return;
    }

    nodeProcessed = new AtomicInteger(0);
    containerProcessedCount = new AtomicInteger(0);
    nodeCount = new AtomicInteger(0);
    /*
       Ask each datanode to send us commands.
     */
    SendContainerCommand cmd = SendContainerCommand.newBuilder().build();
    for (DatanodeID id : datanodeIDList) {
      NodeState currentState = getNodestate(id);
      if (currentState == HEALTHY || currentState == STALE) {
        nodeCount.incrementAndGet();
        // Queue commands to all datanodes in this pool to send us container
        // report. Since we ignore dead nodes, it is possible that we would have
        // over replicated the container if the node comes back.
        nodeManager.addDatanodeCommand(id, cmd);
      }
    }
    this.status = ProgressStatus.InProgress;
    this.getPool().setLastProcessedTime(Time.monotonicNow());
  }

  /**
   * Gets the node state.
   *
   * @param id - datanode ID.
   * @return NodeState.
   */
  private NodeState getNodestate(DatanodeID id) {
    NodeState  currentState = UNKNOWN;
    int maxTry = 100;
    // We need to loop to make sure that we will retry if we get
    // node state unknown. This can lead to infinite loop if we send
    // in unknown node ID. So max try count is used to prevent it.

    int currentTry = 0;
    while (currentState == UNKNOWN && currentTry < maxTry) {
      // Retry to make sure that we deal with the case of node state not
      // known.
      currentState = nodeManager.getNodeState(id);
      currentTry++;
      if (currentState == UNKNOWN) {
        // Sleep to make sure that this is not a tight loop.
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    }
    if (currentState == UNKNOWN) {
      LOG.error("Not able to determine the state of Node: {}, Exceeded max " +
          "try and node manager returns UNKNOWN state. This indicates we " +
          "are dealing with a node that we don't know about.", id);
    }
    return currentState;
  }

  /**
   * Queues a container Report for handling. This is done in a worker thread
   * since decoding a container report might be compute intensive . We don't
   * want to block since we have asked for bunch of container reports
   * from a set of datanodes.
   *
   * @param containerReport - ContainerReport
   */
  public void handleContainerReport(
      ContainerReportsRequestProto containerReport) {
    if (status == ProgressStatus.InProgress) {
      executorService.submit(processContainerReport(containerReport));
    } else {
      LOG.debug("Cannot handle container report when the pool is in {} status.",
          status);
    }
  }

  private Runnable processContainerReport(
      ContainerReportsRequestProto reports) {
    return () -> {
      DatanodeID datanodeID =
          DatanodeID.getFromProtoBuf(reports.getDatanodeID());
      if (processedNodeSet.computeIfAbsent(datanodeID.getDatanodeUuid(),
          (k) -> true)) {
        nodeProcessed.incrementAndGet();
        LOG.debug("Total Nodes processed : {} Node Name: {} ", nodeProcessed,
            datanodeID.getDatanodeUuid());
        for (ContainerInfo info : reports.getReportsList()) {
          containerProcessedCount.incrementAndGet();
          LOG.debug("Total Containers processed: {} Container Name: {}",
              containerProcessedCount.get(), info.getContainerName());

          // Update the container map with count + 1 if the key exists or
          // update the map with 1. Since this is a concurrentMap the
          // computation and update is atomic.
          containerCountMap.merge(info.getContainerName(), 1, Integer::sum);
        }
      }
    };
  }

  /**
   * Filter the containers based on specific rules.
   *
   * @param predicate -- Predicate to filter by
   * @return A list of map entries.
   */
  public List<Map.Entry<String, Integer>> filterContainer(
      Predicate<Map.Entry<String, Integer>> predicate) {
    return containerCountMap.entrySet().stream()
        .filter(predicate).collect(Collectors.toList());
  }

  /**
   * Used only for testing, calling this will abort container report
   * processing. This is very dangerous call and should not be made by any users
   */
  @VisibleForTesting
  public void setDoneProcessing() {
    nodeProcessed.set(nodeCount.get());
  }

  /**
   * Returns the pool name.
   *
   * @return Name of the pool.
   */
  String getPoolName() {
    return pool.getPoolName();
  }

  public void finalizeReconciliation() {
    status = ProgressStatus.Done;
    //TODO: Add finalizing logic. This is where actual reconciliation happens.
  }

  /**
   * Current status of the computing replication status.
   */
  public enum ProgressStatus {
    InProgress, Done, Error
  }
}
