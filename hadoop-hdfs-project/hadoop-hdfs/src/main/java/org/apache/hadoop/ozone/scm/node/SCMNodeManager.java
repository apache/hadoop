/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.scm.node;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.ozone.OzoneClientUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Maintains information about the Datanodes on SCM side.
 * <p/>
 * Heartbeats under SCM is very simple compared to HDFS heartbeatManager.
 * <p/>
 * Here we maintain 3 maps, and we propagate a node from healthyNodesMap to
 * staleNodesMap to deadNodesMap. This moving of a node from one map to
 * another is controlled by 4 configuration variables. These variables define
 * how many heartbeats must go missing for the node to move from one map to
 * another.
 * <p/>
 * Each heartbeat that SCMNodeManager receives is  put into heartbeatQueue. The
 * worker thread wakes up and grabs that heartbeat from the queue. The worker
 * thread will lookup the healthynodes map and update the timestamp if the entry
 * is there. if not it will look up stale and deadnodes map.
 * <p/>
 *
 * TODO: Replace with Node Registration code.
 * if the node is not found in any of these tables it is treated as new node for
 * time being and added to the healthy nodes list.
 *
 * <p/>
 *
 * The getNode(byState) functions make copy of node maps and then creates a
 * list based on that. It should be assumed that these get functions always
 * report *stale* information. For example, getting the deadNodeCount
 * followed by
 * getNodes(DEAD) could very well produce totally different count. Also
 * getNodeCount(HEALTHY) + getNodeCount(DEAD) + getNodeCode(STALE), is not
 * guaranteed to add up to the total nodes that we know off. Please treat all
 * get functions in this file as a snap-shot of information that is
 * inconsistent as soon as you read it.
 */
public class SCMNodeManager implements NodeManager {

  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(SCMNodeManager.class);
  /**
   * Key = NodeID, value = timestamp.
   */
  private final Map<String, Long> healthyNodes;
  private final Map<String, Long> staleNodes;
  private final Map<String, Long> deadNodes;
  private final Queue<DatanodeID> heartbeatQueue;
  private final Map<String, DatanodeID> nodes;
  private final AtomicInteger healthyNodeCount;
  private final AtomicInteger staleNodeCount;
  private final AtomicInteger deadNodeCount;
  private final AtomicInteger totalNodes;
  private final long staleNodeIntervalMs;
  private final long deadNodeIntervalMs;
  private final long heartbeatCheckerIntervalMs;
  private final long datanodeHBIntervalSeconds;
  private final ScheduledExecutorService executorService;
  private long lastHBcheckStart;
  private long lastHBcheckFinished = 0;
  private long lastHBProcessedCount;
  private int safeModeNodeCount;
  private final int maxHBToProcessPerLoop;

  /**
   * Constructs SCM machine Manager.
   */
  public SCMNodeManager(Configuration conf) {
    heartbeatQueue = new ConcurrentLinkedQueue<>();
    healthyNodes = new ConcurrentHashMap<>();
    deadNodes = new ConcurrentHashMap<>();
    staleNodes = new ConcurrentHashMap<>();
    nodes = new HashMap<>();

    healthyNodeCount = new AtomicInteger(0);
    staleNodeCount = new AtomicInteger(0);
    deadNodeCount = new AtomicInteger(0);
    totalNodes = new AtomicInteger(0);

    // TODO: Support this value as a Percentage of known machines.
    safeModeNodeCount = 1;

    staleNodeIntervalMs = OzoneClientUtils.getStaleNodeInterval(conf);
    deadNodeIntervalMs = OzoneClientUtils.getDeadNodeInterval(conf);
    heartbeatCheckerIntervalMs =
        OzoneClientUtils.getScmheartbeatCheckerInterval(conf);
    datanodeHBIntervalSeconds = OzoneClientUtils.getScmHeartbeatInterval(conf);
    maxHBToProcessPerLoop = OzoneClientUtils.getMaxHBToProcessPerLoop(conf);

    executorService = HadoopExecutors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("SCM Heartbeat Processing Thread - %d").build());

    Preconditions.checkState(heartbeatCheckerIntervalMs > 0);
    executorService.schedule(this, heartbeatCheckerIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Add a New Datanode to the NodeManager. This function is invoked with
   * synchronised(this) being held.
   *
   * @param nodeReg - node to register
   */
  @Override
  public void registerNode(DatanodeID nodeReg) {
    if (nodes.containsKey(nodeReg.getDatanodeUuid())) {
      LOG.error("Datanode is already registered. Datanode: {}",
          nodeReg.toString());
      return;
    }
    nodes.put(nodeReg.getDatanodeUuid(), nodeReg);
    totalNodes.incrementAndGet();
    healthyNodes.put(nodeReg.getDatanodeUuid(), monotonicNow());
    healthyNodeCount.incrementAndGet();
    LOG.info("Data node with ID: {} Registered.", nodeReg.getDatanodeUuid());
  }

  /**
   * Register the heartbeat with Machine Manager.
   *
   * This requires no synchronization since the heartbeat queue is
   * ConcurrentLinkedQueue. Hence we don't protect it specifically via a lock.
   *
   * @param datanodeID - Name of the datanode that send us heartbeat.
   */
  @Override
  public void updateHeartbeat(DatanodeID datanodeID) {
    // Checking for NULL to make sure that we don't get
    // an exception from ConcurrentList.
    // This could be a problem in tests, if this function is invoked via
    // protobuf, transport layer will guarantee that this is not null.
    if (datanodeID != null) {
      heartbeatQueue.add(datanodeID);
      return;
    }
    LOG.error("Datanode ID in heartbeat is null");
  }

  /**
   * Removes a data node from the management of this Node Manager.
   *
   * @param node - DataNode.
   * @throws UnregisteredNodeException
   */
  @Override
  public void removeNode(DatanodeID node) throws UnregisteredNodeException {
    // TODO : Fix me.

  }

  /**
   * Gets all datanodes that are in a certain state. This function works by
   * taking a snapshot of the current collection and then returning the list
   * from that collection. This means that real map might have changed by the
   * time we return this list.
   *
   * @return List of Datanodes that are known to SCM in the requested state.
   */
  @Override
  public List<DatanodeID> getNodes(NODESTATE nodestate)
      throws IllegalArgumentException{
    Map<String, Long> set;
    switch (nodestate) {
    case HEALTHY:
      synchronized (this) {
        set = Collections.unmodifiableMap(new HashMap<>(healthyNodes));
      }
      break;
    case STALE:
      synchronized (this) {
        set = Collections.unmodifiableMap(new HashMap<>(staleNodes));
      }
      break;
    case DEAD:
      synchronized (this) {
        set = Collections.unmodifiableMap(new HashMap<>(deadNodes));
      }
      break;
    default:
      throw new IllegalArgumentException("Unknown node state requested.");
    }

    return set.entrySet().stream().map(entry -> nodes.get(entry.getKey()))
            .collect(Collectors.toList());
  }

  /**
   * Returns all datanodes that are known to SCM.
   *
   * @return List of DatanodeIDs
   */
  @Override
  public List<DatanodeID> getAllNodes() {
    Map<String, DatanodeID> set;
    synchronized (this) {
      set = Collections.unmodifiableMap(new HashMap<>(nodes));
    }
    return set.entrySet().stream().map(entry -> nodes.get(entry.getKey()))
            .collect(Collectors.toList());
  }

  /**
   * Get the minimum number of nodes to get out of safe mode.
   *
   * @return int
   */
  @Override
  public int getMinimumSafeModeNodes() {
    return safeModeNodeCount;
  }

  /**
   * Sets the Minimum SafeModeNode count, used only in testing.
   *
   * @param count  - Number of nodes.
   */
  @VisibleForTesting
  public void setMinimumSafeModeNodes(int count) {
    safeModeNodeCount = count;
  }

  /**
   * Reports if we have exited out of safe mode.
   *
   * @return true if we are out of safe mode.
   */
  @Override
  public boolean isOutOfNodeSafeMode() {
    LOG.trace("Node count : {}", totalNodes.get());

    //TODO : Support a boolean to force getting out of Safe mode.
    return (totalNodes.get() >= getMinimumSafeModeNodes());
  }

  /**
   * Returns the Number of Datanodes by State they are in.
   *
   * @return int -- count
   */
  @Override
  public int getNodeCount(NODESTATE nodestate) {
    switch (nodestate) {
    case HEALTHY:
      return healthyNodeCount.get();
    case STALE:
      return staleNodeCount.get();
    case DEAD:
      return deadNodeCount.get();
    default:
      throw new IllegalArgumentException("Unknown node state requested.");
    }
  }

  /**
   * Used for testing.
   * @return true if the HB check is done.
   */
  @VisibleForTesting
  public boolean waitForHeartbeatThead() {
    return lastHBcheckFinished != 0;
  }

  /**
   * This is the real worker thread that processes the HB queue. We do the
   * following things in this thread.
   *
   * Process the Heartbeats that are in the HB Queue.
   * Move Stale or Dead node to healthy if we got a heartbeat from them.
   * Move Stales Node to dead node table if it is needed.
   * Move healthy nodes to stale nodes if it is needed.
   *
   * if it is a new node, we call register node and add it to the list of nodes.
   * This will be replaced when we support registration of a node in SCM.
   * @see Thread#run()
   */
  @Override
  public void run() {
    lastHBcheckStart = monotonicNow();
    lastHBProcessedCount = 0;

    // Process the whole queue.
    while (!heartbeatQueue.isEmpty() &&
        (lastHBProcessedCount < maxHBToProcessPerLoop)) {
      DatanodeID datanodeID = heartbeatQueue.poll();
      synchronized (this) {
        handleHeartbeat(datanodeID);
      }
      // we are shutting down or something give up processing the rest of
      // HBs. This will terminate the HB processing thread.
      if (Thread.currentThread().isInterrupted()) {
        LOG.info("Current Thread is isInterrupted, shutting down HB " +
            "processing thread for Node Manager.");
        return;
      }
    }

    if (lastHBProcessedCount >= maxHBToProcessPerLoop) {
      LOG.error("SCM is being flooded by heartbeats. Not able to keep up with" +
          " the heartbeat counts. Processed {} heartbeats. Breaking out of" +
          " loop. Leaving rest to be processed later. ", lastHBProcessedCount);
    }

    // Iterate over the Stale nodes and decide if we need to move any node to
    // dead State.
    long currentTime = monotonicNow();
    for (Map.Entry<String, Long> entry : staleNodes.entrySet()) {
      if (currentTime - entry.getValue() > deadNodeIntervalMs) {
        synchronized (this) {
          moveStaleNodeToDead(entry);
        }
      }
    }

    // Iterate over the healthy nodes and decide if we need to move any node to
    // Stale State.
    currentTime = monotonicNow();
    for (Map.Entry<String, Long> entry : healthyNodes.entrySet()) {
      if (currentTime - entry.getValue() > staleNodeIntervalMs) {
        synchronized (this) {
          moveHealthyNodeToStale(entry);
        }
      }
    }
    lastHBcheckFinished = monotonicNow();

    monitorHBProcessingTime();

    // we purposefully make this non-deterministic. Instead of using a
    // scheduleAtFixedFrequency  we will just go to sleep
    // and wake up at the next rendezvous point, which is currentTime +
    // heartbeatCheckerIntervalMs. This leads to the issue that we are now
    // heart beating not at a fixed cadence, but clock tick + time taken to
    // work.
    //
    // This time taken to work can skew the heartbeat processor thread.
    // The reason why we don't care is because of the following reasons.
    //
    // 1. checkerInterval is general many magnitudes faster than datanode HB
    // frequency.
    //
    // 2. if we have too much nodes, the SCM would be doing only HB
    // processing, this could lead to SCM's CPU starvation. With this
    // approach we always guarantee that  HB thread sleeps for a little while.
    //
    // 3. It is possible that we will never finish processing the HB's in the
    // thread. But that means we have a mis-configured system. We will warn
    // the users by logging that information.
    //
    // 4. And the most important reason, heartbeats are not blocked even if
    // this thread does not run, they will go into the processing queue.

    if (!Thread.currentThread().isInterrupted()) {
      executorService.schedule(this, heartbeatCheckerIntervalMs, TimeUnit
          .MILLISECONDS);
    } else {
      LOG.info("Current Thread is interrupted, shutting down HB processing " +
          "thread for Node Manager.");
    }
  }

  /**
   * If we have taken too much time for HB processing, log that information.
   */
  private void monitorHBProcessingTime() {
    if (TimeUnit.MILLISECONDS.toSeconds(lastHBcheckFinished -
        lastHBcheckStart) > datanodeHBIntervalSeconds) {
      LOG.error("Total time spend processing datanode HB's is greater than " +
              "configured values for datanode heartbeats. Please adjust the" +
              " heartbeat configs. Time Spend on HB processing: {} seconds " +
              "Datanode heartbeat Interval: {} seconds , heartbeats " +
              "processed: {}",
          TimeUnit.MILLISECONDS
              .toSeconds(lastHBcheckFinished - lastHBcheckStart),
          datanodeHBIntervalSeconds, lastHBProcessedCount);
    }
  }

  /**
   * Moves a Healthy node to a Stale node state.
   *
   * @param entry - Map Entry
   */
  private void moveHealthyNodeToStale(Map.Entry<String, Long> entry) {
    LOG.trace("Moving healthy node to stale: {}", entry.getKey());
    healthyNodes.remove(entry.getKey());
    healthyNodeCount.decrementAndGet();
    staleNodes.put(entry.getKey(), entry.getValue());
    staleNodeCount.incrementAndGet();
  }

  /**
   * Moves a Stale node to a dead node state.
   *
   * @param entry - Map Entry
   */
  private void moveStaleNodeToDead(Map.Entry<String, Long> entry) {
    LOG.trace("Moving stale node to dead: {}", entry.getKey());
    staleNodes.remove(entry.getKey());
    staleNodeCount.decrementAndGet();
    deadNodes.put(entry.getKey(), entry.getValue());
    deadNodeCount.incrementAndGet();
  }

  /**
   * Handles a single heartbeat from a datanode.
   *
   * @param datanodeID - datanode ID.
   */
  private void handleHeartbeat(DatanodeID datanodeID) {
    lastHBProcessedCount++;

    // If this node is already in the list of known and healthy nodes
    // just update the last timestamp and return.
    if (healthyNodes.containsKey(datanodeID.getDatanodeUuid())) {
      healthyNodes.put(datanodeID.getDatanodeUuid(), monotonicNow());
      return;
    }

    // A stale node has heartbeat us we need to remove the node from stale
    // list and move to healthy list.
    if (staleNodes.containsKey(datanodeID.getDatanodeUuid())) {
      staleNodes.remove(datanodeID.getDatanodeUuid());
      healthyNodes.put(datanodeID.getDatanodeUuid(), monotonicNow());
      healthyNodeCount.incrementAndGet();
      staleNodeCount.decrementAndGet();
      return;
    }

    // A dead node has heartbeat us, we need to remove that node from dead
    // node list and move it to the healthy list.
    if (deadNodes.containsKey(datanodeID.getDatanodeUuid())) {
      deadNodes.remove(datanodeID.getDatanodeUuid());
      healthyNodes.put(datanodeID.getDatanodeUuid(), monotonicNow());
      deadNodeCount.decrementAndGet();
      healthyNodeCount.incrementAndGet();
      return;
    }

    registerNode(datanodeID);
  }

  /**
   * Closes this stream and releases any system resources associated with it. If
   * the stream is already closed then invoking this method has no effect.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }

      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.error("Unable to shutdown NodeManager properly.");
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  long getLastHBProcessedCount() {
    return lastHBProcessedCount;
  }

}
