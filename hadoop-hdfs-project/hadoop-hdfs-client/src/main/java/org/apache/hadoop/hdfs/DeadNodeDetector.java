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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_CONNECTION_TIMEOUT_MS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_CONNECTION_TIMEOUT_MS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_INTERVAL_MS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_THREADS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_THREADS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_SUSPECT_NODE_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_SUSPECT_NODE_INTERVAL_MS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_SUSPECT_NODE_THREADS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_SUSPECT_NODE_THREADS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_RPC_THREADS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_RPC_THREADS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_IDLE_SLEEP_MS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_IDLE_SLEEP_MS_DEFAULT;

/**
 * Detect the dead nodes in advance, and share this information among all the
 * DFSInputStreams in the same client.
 */
public class DeadNodeDetector extends Daemon {
  public static final Logger LOG =
      LoggerFactory.getLogger(DeadNodeDetector.class);

  /**
   * Waiting time when DeadNodeDetector happens error.
   */
  private static final long ERROR_SLEEP_MS = 5000;

  /**
   * Waiting time when DeadNodeDetector's state is idle.
   */
  private final long idleSleepMs;

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
   * Record suspect and dead nodes by one DFSInputStream. When node is not used
   * by one DFSInputStream, remove it from suspectAndDeadNodes#DFSInputStream.
   * If DFSInputStream does not include any node, remove DFSInputStream from
   * suspectAndDeadNodes.
   */
  private final Map<DFSInputStream, HashSet<DatanodeInfo>>
          suspectAndDeadNodes;

  /**
   * Datanodes that is being probed.
   */
  private Map<String, DatanodeInfo> probeInProg =
      new ConcurrentHashMap<String, DatanodeInfo>();

  /**
   * Interval time in milliseconds for probing dead node behavior.
   */
  private long deadNodeDetectInterval = 0;

  /**
   * Interval time in milliseconds for probing suspect node behavior.
   */
  private long suspectNodeDetectInterval = 0;

  /**
   * Connection timeout for probing dead node in milliseconds.
   */
  private long probeConnectionTimeoutMs;

  /**
   * The dead node probe queue.
   */
  private UniqueQueue<DatanodeInfo> deadNodesProbeQueue;

  /**
   * The suspect node probe queue.
   */
  private UniqueQueue<DatanodeInfo> suspectNodesProbeQueue;

  /**
   * The thread pool of probing dead node.
   */
  private ExecutorService probeDeadNodesThreadPool;

  /**
   * The thread pool of probing suspect node.
   */
  private ExecutorService probeSuspectNodesThreadPool;

  /**
   * The scheduler thread of probing dead node.
   */
  private Thread probeDeadNodesSchedulerThr;

  /**
   * The scheduler thread of probing suspect node.
   */
  private Thread probeSuspectNodesSchedulerThr;

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
    CHECK_DEAD, CHECK_SUSPECT
  }

  /**
   * The state of DeadNodeDetector.
   */
  private enum State {
    INIT, CHECK_DEAD, IDLE, ERROR
  }

  /**
   * The thread safe unique queue.
   */
  static class UniqueQueue<T> {
    private Deque<T> queue = new LinkedList<>();
    private Set<T> set = new HashSet<>();

    synchronized boolean offer(T dn) {
      if (set.add(dn)) {
        queue.addLast(dn);
        return true;
      }
      return false;
    }

    synchronized T poll() {
      T dn = queue.pollFirst();
      set.remove(dn);
      return dn;
    }

    synchronized int size() {
      return set.size();
    }
  }

  /**
   * Disabled start probe suspect/dead thread for the testing.
   */
  private static volatile boolean disabledProbeThreadForTest = false;

  private State state;

  public DeadNodeDetector(String name, Configuration conf) {
    this.conf = new Configuration(conf);
    this.deadNodes = new ConcurrentHashMap<String, DatanodeInfo>();
    this.suspectAndDeadNodes =
        new ConcurrentHashMap<DFSInputStream, HashSet<DatanodeInfo>>();
    this.name = name;

    deadNodeDetectInterval = conf.getLong(
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_INTERVAL_MS_KEY,
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_INTERVAL_MS_DEFAULT);
    suspectNodeDetectInterval = conf.getLong(
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_SUSPECT_NODE_INTERVAL_MS_KEY,
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_SUSPECT_NODE_INTERVAL_MS_DEFAULT);
    socketTimeout =
        conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, HdfsConstants.READ_TIMEOUT);
    probeConnectionTimeoutMs = conf.getLong(
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_CONNECTION_TIMEOUT_MS_KEY,
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_CONNECTION_TIMEOUT_MS_DEFAULT);
    this.deadNodesProbeQueue = new UniqueQueue<>();
    this.suspectNodesProbeQueue = new UniqueQueue<>();

    idleSleepMs = conf.getLong(DFS_CLIENT_DEAD_NODE_DETECTION_IDLE_SLEEP_MS_KEY,
        DFS_CLIENT_DEAD_NODE_DETECTION_IDLE_SLEEP_MS_DEFAULT);

    int deadNodeDetectDeadThreads =
        conf.getInt(DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_THREADS_KEY,
            DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_THREADS_DEFAULT);
    int suspectNodeDetectDeadThreads = conf.getInt(
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_SUSPECT_NODE_THREADS_KEY,
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_SUSPECT_NODE_THREADS_DEFAULT);
    int rpcThreads = conf.getInt(DFS_CLIENT_DEAD_NODE_DETECTION_RPC_THREADS_KEY,
        DFS_CLIENT_DEAD_NODE_DETECTION_RPC_THREADS_DEFAULT);
    probeDeadNodesThreadPool = Executors.newFixedThreadPool(
        deadNodeDetectDeadThreads, new Daemon.DaemonFactory());
    probeSuspectNodesThreadPool = Executors.newFixedThreadPool(
        suspectNodeDetectDeadThreads, new Daemon.DaemonFactory());
    rpcThreadPool =
        Executors.newFixedThreadPool(rpcThreads, new Daemon.DaemonFactory());

    if (!disabledProbeThreadForTest) {
      startProbeScheduler();
    }

    LOG.info("Start dead node detector for DFSClient {}.", this.name);
    state = State.INIT;
  }

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
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
          LOG.debug("Got interrupted while DeadNodeDetector is error.", e);
          Thread.currentThread().interrupt();
        }
        return;
      default:
        break;
      }
    }
  }

  /**
   * Shutdown all the threads.
   */
  public void shutdown() {
    threadShutDown(this);
    threadShutDown(probeDeadNodesSchedulerThr);
    threadShutDown(probeSuspectNodesSchedulerThr);
    probeDeadNodesThreadPool.shutdown();
    probeSuspectNodesThreadPool.shutdown();
    rpcThreadPool.shutdown();
  }

  private static void threadShutDown(Thread thread) {
    if (thread != null && thread.isAlive()) {
      thread.interrupt();
      try {
        thread.join();
      } catch (InterruptedException e) {
      }
    }
  }

  @VisibleForTesting
  boolean isThreadsShutdown() {
    return !this.isAlive() && !probeDeadNodesSchedulerThr.isAlive()
        && !probeSuspectNodesSchedulerThr.isAlive()
        && probeDeadNodesThreadPool.isShutdown()
        && probeSuspectNodesThreadPool.isShutdown()
        && rpcThreadPool.isShutdown();
  }

  @VisibleForTesting
  static void setDisabledProbeThreadForTest(
      boolean disabledProbeThreadForTest) {
    DeadNodeDetector.disabledProbeThreadForTest = disabledProbeThreadForTest;
  }

  /**
   * Start probe dead node and suspect node thread.
   */
  @VisibleForTesting
  void startProbeScheduler() {
    probeDeadNodesSchedulerThr =
            new Thread(new ProbeScheduler(this, ProbeType.CHECK_DEAD));
    probeDeadNodesSchedulerThr.setDaemon(true);
    probeDeadNodesSchedulerThr.start();

    probeSuspectNodesSchedulerThr =
            new Thread(new ProbeScheduler(this, ProbeType.CHECK_SUSPECT));
    probeSuspectNodesSchedulerThr.setDaemon(true);
    probeSuspectNodesSchedulerThr.start();
  }

  /**
   * Prode datanode by probe type.
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
    } else if (type == ProbeType.CHECK_SUSPECT) {
      while ((datanodeInfo = suspectNodesProbeQueue.poll()) != null) {
        if (probeInProg.containsKey(datanodeInfo.getDatanodeUuid())) {
          continue;
        }
        probeInProg.put(datanodeInfo.getDatanodeUuid(), datanodeInfo);
        Probe probe = new Probe(this, datanodeInfo, ProbeType.CHECK_SUSPECT);
        probeSuspectNodesThreadPool.execute(probe);
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
        deadNodeDetector.probeCallBack(this, false);
      }
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
        LOG.info("Remove the node out from dead node list: {}.",
            probe.getDatanodeInfo());
        removeDeadNode(probe.getDatanodeInfo());
      } else if (probe.getType() == ProbeType.CHECK_SUSPECT) {
        LOG.debug("Remove the node out from suspect node list: {}.",
            probe.getDatanodeInfo());
        removeNodeFromDeadNodeDetector(probe.getDatanodeInfo());
      }
    } else {
      if (probe.getType() == ProbeType.CHECK_SUSPECT) {
        LOG.warn("Probe failed, add suspect node to dead node list: {}.",
            probe.getDatanodeInfo());
        addToDead(probe.getDatanodeInfo());
      }
    }
  }

  /**
   * Check dead node periodically.
   */
  private void checkDeadNodes() {
    Set<DatanodeInfo> datanodeInfos = clearAndGetDetectedDeadNodes();
    for (DatanodeInfo datanodeInfo : datanodeInfos) {
      if (!deadNodesProbeQueue.offer(datanodeInfo)) {
        LOG.debug("Skip to add dead node {} to check " +
                "since the node is already in the probe queue.", datanodeInfo);
      } else {
        LOG.debug("Add dead node to check: {}.", datanodeInfo);
      }
    }
    state = State.IDLE;
  }

  private void idle() {
    try {
      Thread.sleep(idleSleepMs);
    } catch (InterruptedException e) {
      LOG.debug("Got interrupted while DeadNodeDetector is idle.", e);
      Thread.currentThread().interrupt();
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

  public UniqueQueue<DatanodeInfo> getDeadNodesProbeQueue() {
    return deadNodesProbeQueue;
  }

  public UniqueQueue<DatanodeInfo> getSuspectNodesProbeQueue() {
    return suspectNodesProbeQueue;
  }

  @VisibleForTesting
  void setSuspectQueue(UniqueQueue<DatanodeInfo> queue) {
    this.suspectNodesProbeQueue = queue;
  }

  @VisibleForTesting
  void setDeadQueue(UniqueQueue<DatanodeInfo> queue) {
    this.deadNodesProbeQueue = queue;
  }

  /**
   * Add datanode to suspectNodes and suspectAndDeadNodes.
   */
  public synchronized void addNodeToDetect(DFSInputStream dfsInputStream,
      DatanodeInfo datanodeInfo) {
    HashSet<DatanodeInfo> datanodeInfos =
        suspectAndDeadNodes.get(dfsInputStream);
    if (datanodeInfos == null) {
      datanodeInfos = new HashSet<DatanodeInfo>();
      datanodeInfos.add(datanodeInfo);
      suspectAndDeadNodes.putIfAbsent(dfsInputStream, datanodeInfos);
    } else {
      datanodeInfos.add(datanodeInfo);
    }

    LOG.debug("Add datanode {} to suspectAndDeadNodes.", datanodeInfo);
    addSuspectNodeToDetect(datanodeInfo);
  }

  /**
   * Add datanode to suspectNodes.
   */
  private boolean addSuspectNodeToDetect(DatanodeInfo datanodeInfo) {
    return suspectNodesProbeQueue.offer(datanodeInfo);
  }

    /**
     * Remove dead node which is not used by any DFSInputStream from deadNodes.
     * @return new dead node shared by all DFSInputStreams.
     */
  public synchronized Set<DatanodeInfo> clearAndGetDetectedDeadNodes() {
    // remove the dead nodes who doesn't have any inputstream first
    Set<DatanodeInfo> newDeadNodes = new HashSet<DatanodeInfo>();
    for (HashSet<DatanodeInfo> datanodeInfos : suspectAndDeadNodes.values()) {
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
   * Remove suspect and dead node from suspectAndDeadNodes#dfsInputStream and
   *  local deadNodes.
   */
  public synchronized void removeNodeFromDeadNodeDetector(
      DFSInputStream dfsInputStream, DatanodeInfo datanodeInfo) {
    Set<DatanodeInfo> datanodeInfos = suspectAndDeadNodes.get(dfsInputStream);
    if (datanodeInfos != null) {
      datanodeInfos.remove(datanodeInfo);
      dfsInputStream.removeFromLocalDeadNodes(datanodeInfo);
      if (datanodeInfos.isEmpty()) {
        suspectAndDeadNodes.remove(dfsInputStream);
      }
    }
  }

  /**
   * Remove suspect and dead node from suspectAndDeadNodes#dfsInputStream and
   *  local deadNodes.
   */
  private synchronized void removeNodeFromDeadNodeDetector(
      DatanodeInfo datanodeInfo) {
    for (Map.Entry<DFSInputStream, HashSet<DatanodeInfo>> entry :
            suspectAndDeadNodes.entrySet()) {
      Set<DatanodeInfo> datanodeInfos = entry.getValue();
      if (datanodeInfos.remove(datanodeInfo)) {
        DFSInputStream dfsInputStream = entry.getKey();
        dfsInputStream.removeFromLocalDeadNodes(datanodeInfo);
        if (datanodeInfos.isEmpty()) {
          suspectAndDeadNodes.remove(dfsInputStream);
        }
      }
    }
  }

  /**
   * Remove suspect and dead node from suspectAndDeadNodes#dfsInputStream and
   * deadNodes.
   */
  private void removeDeadNode(DatanodeInfo datanodeInfo) {
    removeNodeFromDeadNodeDetector(datanodeInfo);
    removeFromDead(datanodeInfo);
  }

  private static void probeSleep(long time) {
    try {
      Thread.sleep(time);
    } catch (InterruptedException e) {
      LOG.debug("Got interrupted while probe is scheduling.", e);
      Thread.currentThread().interrupt();
      return;
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
      while (!Thread.currentThread().isInterrupted()) {
        deadNodeDetector.scheduleProbe(type);
        if (type == ProbeType.CHECK_SUSPECT) {
          probeSleep(deadNodeDetector.suspectNodeDetectInterval);
        } else {
          probeSleep(deadNodeDetector.deadNodeDetectInterval);
        }
      }
    }
  }
}
