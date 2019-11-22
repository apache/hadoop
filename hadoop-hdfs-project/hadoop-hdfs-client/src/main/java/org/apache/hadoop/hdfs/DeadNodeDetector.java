/**
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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_DEAD_NODE_QUEUE_MAX_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_DEAD_NODE_QUEUE_MAX_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_CONNECTION_TIMEOUT_MS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_CONNECTION_TIMEOUT_MS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_INTERVAL_MS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_THREADS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_THREADS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_RPC_THREADS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_RPC_THREADS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;

/**
 * Detect the dead nodes in advance, and share this information among all the
 * DFSInputStreams in the same client.
 */
public class DeadNodeDetector implements Runnable {
  public static final Logger LOG =
      LoggerFactory.getLogger(DeadNodeDetector.class);

  /**
   * Waiting time when DeadNodeDetector happens error.
   */
  private static final long ERROR_SLEEP_MS = 5000;

  /**
   * Waiting time when DeadNodeDetector's state is idle.
   */
  private static final long IDLE_SLEEP_MS = 10000;

  /**
   * Client context name.
   */
  private String name;

  private Configuration conf;

  /**
   * Dead nodes shared by all the DFSInputStreams of the client.
   */
  private final Map<String, DatanodeInfo> deadNodes;

  /**
   * Record dead nodes by one DFSInputStream. When dead node is not used by one
   * DFSInputStream, remove it from dfsInputStreamNodes#DFSInputStream. If
   * DFSInputStream does not include any dead node, remove DFSInputStream from
   * dfsInputStreamNodes.
   */
  private final Map<DFSInputStream, HashSet<DatanodeInfo>>
          dfsInputStreamNodes;

  /**
   * Datanodes that is being probed.
   */
  private Map<String, DatanodeInfo> probeInProg =
      new ConcurrentHashMap<String, DatanodeInfo>();

  /**
   * The last time when detect dead node.
   */
  private long lastDetectDeadTS = 0;

  /**
   * Interval time in milliseconds for probing dead node behavior.
   */
  private long deadNodeDetectInterval = 0;

  /**
   * The max queue size of probing dead node.
   */
  private int maxDeadNodesProbeQueueLen = 0;

  /**
   * Connection timeout for probing dead node in milliseconds.
   */
  private long probeConnectionTimeoutMs;

  /**
   * The dead node probe queue.
   */
  private Queue<DatanodeInfo> deadNodesProbeQueue;

  /**
   * The thread pool of probing dead node.
   */
  private ExecutorService probeDeadNodesThreadPool;

  /**
   * The scheduler thread of probing dead node.
   */
  private Thread probeDeadNodesSchedulerThr;

  /**
   * The thread pool of probing datanodes' rpc request. Sometimes the data node
   * can hang and not respond to the client in a short time. And these node will
   * filled with probe thread pool and block other normal node probing.
   */
  private ExecutorService rpcThreadPool;

  private int socketTimeout;

  /**
   * The type of probe.
   */
  private enum ProbeType {
    CHECK_DEAD
  }

  /**
   * The state of DeadNodeDetector.
   */
  private enum State {
    INIT, CHECK_DEAD, IDLE, ERROR
  }

  private State state;

  public DeadNodeDetector(String name, Configuration conf) {
    this.conf = new Configuration(conf);
    this.deadNodes = new ConcurrentHashMap<String, DatanodeInfo>();
    this.dfsInputStreamNodes =
        new ConcurrentHashMap<DFSInputStream, HashSet<DatanodeInfo>>();
    this.name = name;

    deadNodeDetectInterval = conf.getLong(
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_INTERVAL_MS_KEY,
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_INTERVAL_MS_DEFAULT);
    socketTimeout =
        conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, HdfsConstants.READ_TIMEOUT);
    maxDeadNodesProbeQueueLen =
        conf.getInt(DFS_CLIENT_DEAD_NODE_DETECTION_DEAD_NODE_QUEUE_MAX_KEY,
            DFS_CLIENT_DEAD_NODE_DETECTION_DEAD_NODE_QUEUE_MAX_DEFAULT);
    probeConnectionTimeoutMs = conf.getLong(
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_CONNECTION_TIMEOUT_MS_KEY,
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_CONNECTION_TIMEOUT_MS_DEFAULT);

    this.deadNodesProbeQueue =
        new ArrayBlockingQueue<DatanodeInfo>(maxDeadNodesProbeQueueLen);

    int deadNodeDetectDeadThreads =
        conf.getInt(DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_THREADS_KEY,
            DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_THREADS_DEFAULT);
    int rpcThreads = conf.getInt(DFS_CLIENT_DEAD_NODE_DETECTION_RPC_THREADS_KEY,
        DFS_CLIENT_DEAD_NODE_DETECTION_RPC_THREADS_DEFAULT);
    probeDeadNodesThreadPool = Executors.newFixedThreadPool(
        deadNodeDetectDeadThreads, new Daemon.DaemonFactory());
    rpcThreadPool =
        Executors.newFixedThreadPool(rpcThreads, new Daemon.DaemonFactory());

    startProbeScheduler();

    LOG.info("Start dead node detector for DFSClient {}.", this.name);
    state = State.INIT;
  }

  @Override
  public void run() {
    while (true) {
      clearAndGetDetectedDeadNodes();
      LOG.debug("Current detector state {}, the detected nodes: {}.", state,
          deadNodes.values());
      switch (state) {
      case INIT:
        init();
        break;
      case CHECK_DEAD:
        checkDeadNodes();
        break;
      case IDLE:
        idle();
        break;
      case ERROR:
        try {
          Thread.sleep(ERROR_SLEEP_MS);
        } catch (InterruptedException e) {
        }
        return;
      default:
        break;
      }
    }
  }

  /**
   * Start probe dead node thread.
   */
  private void startProbeScheduler() {
    probeDeadNodesSchedulerThr =
            new Thread(new ProbeScheduler(this, ProbeType.CHECK_DEAD));
    probeDeadNodesSchedulerThr.setDaemon(true);
    probeDeadNodesSchedulerThr.start();
  }

  /**
   * Prode datanode by probe byte.
   */
  private void scheduleProbe(ProbeType type) {
    LOG.debug("Schedule probe datanode for probe type: {}.", type);
    DatanodeInfo datanodeInfo = null;
    if (type == ProbeType.CHECK_DEAD) {
      while ((datanodeInfo = deadNodesProbeQueue.poll()) != null) {
        if (probeInProg.containsKey(datanodeInfo.getDatanodeUuid())) {
          LOG.debug("The datanode {} is already contained in probe queue, " +
              "skip to add it.", datanodeInfo);
          continue;
        }
        probeInProg.put(datanodeInfo.getDatanodeUuid(), datanodeInfo);
        Probe probe = new Probe(this, datanodeInfo, ProbeType.CHECK_DEAD);
        probeDeadNodesThreadPool.execute(probe);
      }
    }
  }

  /**
   * Request the data node through rpc, and determine the data node status based
   * on the returned result.
   */
  class Probe implements Runnable {
    private DeadNodeDetector deadNodeDetector;
    private DatanodeInfo datanodeInfo;
    private ProbeType type;

    Probe(DeadNodeDetector deadNodeDetector, DatanodeInfo datanodeInfo,
        ProbeType type) {
      this.deadNodeDetector = deadNodeDetector;
      this.datanodeInfo = datanodeInfo;
      this.type = type;
    }

    public DatanodeInfo getDatanodeInfo() {
      return datanodeInfo;
    }

    public ProbeType getType() {
      return type;
    }

    @Override
    public void run() {
      LOG.debug("Check node: {}, type: {}.", datanodeInfo, type);
      try {
        final ClientDatanodeProtocol proxy =
            DFSUtilClient.createClientDatanodeProtocolProxy(datanodeInfo,
                deadNodeDetector.conf, socketTimeout, true);

        Future<DatanodeLocalInfo> future = rpcThreadPool.submit(new Callable() {
          @Override
          public DatanodeLocalInfo call() throws Exception {
            return proxy.getDatanodeInfo();
          }
        });

        try {
          future.get(probeConnectionTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          LOG.error("Probe failed, datanode: {}, type: {}.", datanodeInfo, type,
              e);
          deadNodeDetector.probeCallBack(this, false);
          return;
        } finally {
          future.cancel(true);
        }
        deadNodeDetector.probeCallBack(this, true);
        return;
      } catch (Exception e) {
        LOG.error("Probe failed, datanode: {}, type: {}.", datanodeInfo, type,
            e);
      }

      deadNodeDetector.probeCallBack(this, false);
    }
  }

  /**
   * Handle data node, according to probe result. When ProbeType is CHECK_DEAD,
   * remove the datanode from DeadNodeDetector#deadNodes if probe success.
   */
  private void probeCallBack(Probe probe, boolean success) {
    LOG.debug("Probe datanode: {} result: {}, type: {}",
        probe.getDatanodeInfo(), success, probe.getType());
    probeInProg.remove(probe.getDatanodeInfo().getDatanodeUuid());
    if (success) {
      if (probe.getType() == ProbeType.CHECK_DEAD) {
        LOG.info("Remove the node out from dead node list: {}. ",
            probe.getDatanodeInfo());
        removeNodeFromDeadNode(probe.getDatanodeInfo());
      }
    }
  }

  /**
   * Check dead node periodically.
   */
  private void checkDeadNodes() {
    long ts = Time.monotonicNow();
    if (ts - lastDetectDeadTS > deadNodeDetectInterval) {
      Set<DatanodeInfo> datanodeInfos = clearAndGetDetectedDeadNodes();
      for (DatanodeInfo datanodeInfo : datanodeInfos) {
        LOG.debug("Add dead node to check: {}.", datanodeInfo);
        if (!deadNodesProbeQueue.offer(datanodeInfo)) {
          LOG.debug("Skip to add dead node {} to check " +
              "since the probe queue is full.", datanodeInfo);
          break;
        }
      }
      lastDetectDeadTS = ts;
    }

    state = State.IDLE;
  }

  private void idle() {
    try {
      Thread.sleep(IDLE_SLEEP_MS);
    } catch (InterruptedException e) {

    }

    state = State.CHECK_DEAD;
  }

  private void init() {
    state = State.CHECK_DEAD;
  }

  private void addToDead(DatanodeInfo datanodeInfo) {
    deadNodes.put(datanodeInfo.getDatanodeUuid(), datanodeInfo);
  }

  public boolean isDeadNode(DatanodeInfo datanodeInfo) {
    return deadNodes.containsKey(datanodeInfo.getDatanodeUuid());
  }

  private void removeFromDead(DatanodeInfo datanodeInfo) {
    deadNodes.remove(datanodeInfo.getDatanodeUuid());
  }

  public Queue<DatanodeInfo> getDeadNodesProbeQueue() {
    return deadNodesProbeQueue;
  }

  /**
   * Add datanode in deadNodes and dfsInputStreamNodes. The node is considered
   * to dead node. The dead node is shared by all the DFSInputStreams in the
   * same client.
   */
  public synchronized void addNodeToDetect(DFSInputStream dfsInputStream,
      DatanodeInfo datanodeInfo) {
    HashSet<DatanodeInfo> datanodeInfos =
        dfsInputStreamNodes.get(dfsInputStream);
    if (datanodeInfos == null) {
      datanodeInfos = new HashSet<DatanodeInfo>();
      datanodeInfos.add(datanodeInfo);
      dfsInputStreamNodes.putIfAbsent(dfsInputStream, datanodeInfos);
    } else {
      datanodeInfos.add(datanodeInfo);
    }

    addToDead(datanodeInfo);
  }

  /**
   * Remove dead node which is not used by any DFSInputStream from deadNodes.
   * @return new dead node shared by all DFSInputStreams.
   */
  public synchronized Set<DatanodeInfo> clearAndGetDetectedDeadNodes() {
    // remove the dead nodes who doesn't have any inputstream first
    Set<DatanodeInfo> newDeadNodes = new HashSet<DatanodeInfo>();
    for (HashSet<DatanodeInfo> datanodeInfos : dfsInputStreamNodes.values()) {
      newDeadNodes.addAll(datanodeInfos);
    }

    for (DatanodeInfo datanodeInfo : deadNodes.values()) {
      if (!newDeadNodes.contains(datanodeInfo)) {
        deadNodes.remove(datanodeInfo.getDatanodeUuid());
      }
    }
    return new HashSet<>(deadNodes.values());
  }

  /**
   * Remove dead node from dfsInputStreamNodes#dfsInputStream. If
   * dfsInputStreamNodes#dfsInputStream does not contain any dead node, remove
   * it from dfsInputStreamNodes.
   */
  public synchronized void removeNodeFromDeadNodeDetector(
      DFSInputStream dfsInputStream, DatanodeInfo datanodeInfo) {
    Set<DatanodeInfo> datanodeInfos = dfsInputStreamNodes.get(dfsInputStream);
    if (datanodeInfos != null) {
      datanodeInfos.remove(datanodeInfo);
      if (datanodeInfos.isEmpty()) {
        dfsInputStreamNodes.remove(dfsInputStream);
      }
    }
  }

  /**
   * Remove dead node from dfsInputStreamNodes#dfsInputStream and deadNodes.
   */
  public synchronized void removeNodeFromDeadNode(DatanodeInfo datanodeInfo) {
    for (Map.Entry<DFSInputStream, HashSet<DatanodeInfo>> entry :
            dfsInputStreamNodes.entrySet()) {
      Set<DatanodeInfo> datanodeInfos = entry.getValue();
      if (datanodeInfos.remove(datanodeInfo)) {
        DFSInputStream dfsInputStream = entry.getKey();
        dfsInputStream.removeFromLocalDeadNodes(datanodeInfo);
      }
    }

    removeFromDead(datanodeInfo);
  }

  private static void probeSleep(long time) {
    try {
      Thread.sleep(time);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Schedule probe data node.
   */
  static class ProbeScheduler implements Runnable {
    private DeadNodeDetector deadNodeDetector;
    private ProbeType type;

    ProbeScheduler(DeadNodeDetector deadNodeDetector, ProbeType type) {
      this.deadNodeDetector = deadNodeDetector;
      this.type = type;
    }

    @Override
    public void run() {
      while (true) {
        deadNodeDetector.scheduleProbe(type);
        probeSleep(deadNodeDetector.deadNodeDetectInterval);
      }
    }
  }
}
