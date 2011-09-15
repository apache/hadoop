/*
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * This class is responsible to manage all the replication
 * sources. There are two classes of sources:
 * <li> Normal sources are persistent and one per peer cluster</li>
 * <li> Old sources are recovered from a failed region server and our
 * only goal is to finish replicating the HLog queue it had up in ZK</li>
 *
 * When a region server dies, this class uses a watcher to get notified and it
 * tries to grab a lock in order to transfer all the queues in a local
 * old source.
 */
public class ReplicationSourceManager {
  private static final Log LOG =
      LogFactory.getLog(ReplicationSourceManager.class);
  // List of all the sources that read this RS's logs
  private final List<ReplicationSourceInterface> sources;
  // List of all the sources we got from died RSs
  private final List<ReplicationSourceInterface> oldsources;
  // Indicates if we are currently replicating
  private final AtomicBoolean replicating;
  // Helper for zookeeper
  private final ReplicationZookeeper zkHelper;
  // All about stopping
  private final Stoppable stopper;
  // All logs we are currently trackign
  private final Map<String, SortedSet<String>> hlogsById;
  private final Configuration conf;
  private final FileSystem fs;
  // The path to the latest log we saw, for new coming sources
  private Path latestPath;
  // List of all the other region servers in this cluster
  private final List<String> otherRegionServers;
  // Path to the hlogs directories
  private final Path logDir;
  // Path to the hlog archive
  private final Path oldLogDir;
  // The number of ms that we wait before moving znodes, HBASE-3596
  private final long sleepBeforeFailover;
  // Homemade executer service for replication
  private final ThreadPoolExecutor executor;

  /**
   * Creates a replication manager and sets the watch on all the other
   * registered region servers
   * @param zkHelper the zk helper for replication
   * @param conf the configuration to use
   * @param stopper the stopper object for this region server
   * @param fs the file system to use
   * @param replicating the status of the replication on this cluster
   * @param logDir the directory that contains all hlog directories of live RSs
   * @param oldLogDir the directory where old logs are archived
   */
  public ReplicationSourceManager(final ReplicationZookeeper zkHelper,
                                  final Configuration conf,
                                  final Stoppable stopper,
                                  final FileSystem fs,
                                  final AtomicBoolean replicating,
                                  final Path logDir,
                                  final Path oldLogDir) {
    this.sources = new ArrayList<ReplicationSourceInterface>();
    this.replicating = replicating;
    this.zkHelper = zkHelper;
    this.stopper = stopper;
    this.hlogsById = new HashMap<String, SortedSet<String>>();
    this.oldsources = new ArrayList<ReplicationSourceInterface>();
    this.conf = conf;
    this.fs = fs;
    this.logDir = logDir;
    this.oldLogDir = oldLogDir;
    this.sleepBeforeFailover = conf.getLong("replication.sleep.before.failover", 2000);
    this.zkHelper.registerRegionServerListener(
        new OtherRegionServerWatcher(this.zkHelper.getZookeeperWatcher()));
    List<String> otherRSs =
        this.zkHelper.getRegisteredRegionServers();
    this.zkHelper.registerRegionServerListener(
        new PeersWatcher(this.zkHelper.getZookeeperWatcher()));
    this.zkHelper.listPeersIdsAndWatch();
    this.otherRegionServers = otherRSs == null ? new ArrayList<String>() : otherRSs;
    // It's preferable to failover 1 RS at a time, but with good zk servers
    // more could be processed at the same time.
    int nbWorkers = conf.getInt("replication.executor.workers", 1);
    // use a short 100ms sleep since this could be done inline with a RS startup
    // even if we fail, other region servers can take care of it
    this.executor = new ThreadPoolExecutor(nbWorkers, nbWorkers,
        100, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>());
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setNameFormat("ReplicationExecutor-%d");
    this.executor.setThreadFactory(tfb.build());
  }

  /**
   * Provide the id of the peer and a log key and this method will figure which
   * hlog it belongs to and will log, for this region server, the current
   * position. It will also clean old logs from the queue.
   * @param log Path to the log currently being replicated from
   * replication status in zookeeper. It will also delete older entries.
   * @param id id of the peer cluster
   * @param position current location in the log
   * @param queueRecovered indicates if this queue comes from another region server
   */
  public void logPositionAndCleanOldLogs(Path log, String id, long position, boolean queueRecovered) {
    String key = log.getName();
    LOG.info("Going to report log #" + key + " for position " + position + " in " + log);
    this.zkHelper.writeReplicationStatus(key, id, position);
    synchronized (this.hlogsById) {
      SortedSet<String> hlogs = this.hlogsById.get(id);
      if (!queueRecovered && hlogs.first() != key) {
        SortedSet<String> hlogSet = hlogs.headSet(key);
        LOG.info("Removing " + hlogSet.size() +
            " logs in the list: " + hlogSet);
        for (String hlog : hlogSet) {
          this.zkHelper.removeLogFromList(hlog, id);
        }
        hlogSet.clear();
      }
    }
  }

  /**
   * Adds a normal source per registered peer cluster and tries to process all
   * old region server hlog queues
   */
  public void init() throws IOException {
    for (String id : this.zkHelper.getPeerClusters().keySet()) {
      addSource(id);
    }
    List<String> currentReplicators = this.zkHelper.getListOfReplicators();
    if (currentReplicators == null || currentReplicators.size() == 0) {
      return;
    }
    synchronized (otherRegionServers) {
      LOG.info("Current list of replicators: " + currentReplicators
          + " other RSs: " + otherRegionServers);
    }
    // Look if there's anything to process after a restart
    for (String rs : currentReplicators) {
      synchronized (otherRegionServers) {
        if (!this.otherRegionServers.contains(rs)) {
          transferQueues(rs);
        }
      }
    }
  }

  /**
   * Add a new normal source to this region server
   * @param id the id of the peer cluster
   * @return the source that was created
   * @throws IOException
   */
  public ReplicationSourceInterface addSource(String id) throws IOException {
    ReplicationSourceInterface src =
        getReplicationSource(this.conf, this.fs, this, stopper, replicating, id);
    // TODO set it to what's in ZK
    src.setSourceEnabled(true);
    synchronized (this.hlogsById) {
      this.sources.add(src);
      this.hlogsById.put(id, new TreeSet<String>());
      // Add the latest hlog to that source's queue
      if (this.latestPath != null) {
        String name = this.latestPath.getName();
        this.hlogsById.get(id).add(name);
        this.zkHelper.addLogToList(name, src.getPeerClusterZnode());
        src.enqueueLog(this.latestPath);
      }
    }
    src.startup();
    return src;
  }

  /**
   * Terminate the replication on this region server
   */
  public void join() {
    this.executor.shutdown();
    if (this.sources.size() == 0) {
      this.zkHelper.deleteOwnRSZNode();
    }
    for (ReplicationSourceInterface source : this.sources) {
      source.terminate("Region server is closing");
    }
  }

  /**
   * Get a copy of the hlogs of the first source on this rs
   * @return a sorted set of hlog names
   */
  protected Map<String, SortedSet<String>> getHLogs() {
    return Collections.unmodifiableMap(hlogsById);
  }

  /**
   * Get a list of all the normal sources of this rs
   * @return lis of all sources
   */
  public List<ReplicationSourceInterface> getSources() {
    return this.sources;
  }

  void logRolled(Path newLog) {
    if (!this.replicating.get()) {
      LOG.warn("Replication stopped, won't add new log");
      return;
    }

    synchronized (this.hlogsById) {
      String name = newLog.getName();
      for (ReplicationSourceInterface source : this.sources) {
        this.zkHelper.addLogToList(name, source.getPeerClusterZnode());
      }
      for (SortedSet<String> hlogs : this.hlogsById.values()) {
        if (this.sources.isEmpty()) {
          // If there's no slaves, don't need to keep the old hlogs since
          // we only consider the last one when a new slave comes in
          hlogs.clear();
        }
        hlogs.add(name);
      }
    }

    this.latestPath = newLog;
    // This only updates the sources we own, not the recovered ones
    for (ReplicationSourceInterface source : this.sources) {
      source.enqueueLog(newLog);    
    }
  }

  /**
   * Get the ZK help of this manager
   * @return the helper
   */
  public ReplicationZookeeper getRepZkWrapper() {
    return zkHelper;
  }

  /**
   * Factory method to create a replication source
   * @param conf the configuration to use
   * @param fs the file system to use
   * @param manager the manager to use
   * @param stopper the stopper object for this region server
   * @param replicating the status of the replication on this cluster
   * @param peerId the id of the peer cluster
   * @return the created source
   * @throws IOException
   */
  public ReplicationSourceInterface getReplicationSource(
      final Configuration conf,
      final FileSystem fs,
      final ReplicationSourceManager manager,
      final Stoppable stopper,
      final AtomicBoolean replicating,
      final String peerId) throws IOException {
    ReplicationSourceInterface src;
    try {
      @SuppressWarnings("rawtypes")
      Class c = Class.forName(conf.get("replication.replicationsource.implementation",
          ReplicationSource.class.getCanonicalName()));
      src = (ReplicationSourceInterface) c.newInstance();
    } catch (Exception e) {
      LOG.warn("Passed replication source implementation throws errors, " +
          "defaulting to ReplicationSource", e);
      src = new ReplicationSource();

    }
    src.init(conf, fs, manager, stopper, replicating, peerId);
    return src;
  }

  /**
   * Transfer all the queues of the specified to this region server.
   * First it tries to grab a lock and if it works it will move the
   * znodes and finally will delete the old znodes.
   *
   * It creates one old source for any type of source of the old rs.
   * @param rsZnode
   */
  public void transferQueues(String rsZnode) {
    NodeFailoverWorker transfer = new NodeFailoverWorker(rsZnode);
    try {
      this.executor.execute(transfer);
    } catch (RejectedExecutionException ex) {
      LOG.info("Cancelling the transfer of " + rsZnode +
          " because of " + ex.getMessage());
    }
  }

  /**
   * Clear the references to the specified old source
   * @param src source to clear
   */
  public void closeRecoveredQueue(ReplicationSourceInterface src) {
    LOG.info("Done with the recovered queue " + src.getPeerClusterZnode());
    this.oldsources.remove(src);
    this.zkHelper.deleteSource(src.getPeerClusterZnode(), false);
  }

  /**
   * Thie method first deletes all the recovered sources for the specified
   * id, then deletes the normal source (deleting all related data in ZK).
   * @param id The id of the peer cluster
   */
  public void removePeer(String id) {
    LOG.info("Closing the following queue " + id + ", currently have "
        + sources.size() + " and another "
        + oldsources.size() + " that were recovered");
    ReplicationSourceInterface srcToRemove = null;
    List<ReplicationSourceInterface> oldSourcesToDelete =
        new ArrayList<ReplicationSourceInterface>();
    // First close all the recovered sources for this peer
    for (ReplicationSourceInterface src : oldsources) {
      if (id.equals(src.getPeerClusterId())) {
        oldSourcesToDelete.add(src);
      }
    }
    for (ReplicationSourceInterface src : oldSourcesToDelete) {
      closeRecoveredQueue((src));
    }
    LOG.info("Number of deleted recovered sources for " + id + ": "
        + oldSourcesToDelete.size());
    // Now look for the one on this cluster
    for (ReplicationSourceInterface src : this.sources) {
      if (id.equals(src.getPeerClusterId())) {
        srcToRemove = src;
        break;
      }
    }
    if (srcToRemove == null) {
      LOG.error("The queue we wanted to close is missing " + id);
      return;
    }
    srcToRemove.terminate("Replication stream was removed by a user");
    this.sources.remove(srcToRemove);
    this.zkHelper.deleteSource(id, true);
  }

  /**
   * Watcher used to be notified of the other region server's death
   * in the local cluster. It initiates the process to transfer the queues
   * if it is able to grab the lock.
   */
  public class OtherRegionServerWatcher extends ZooKeeperListener {

    /**
     * Construct a ZooKeeper event listener.
     */
    public OtherRegionServerWatcher(ZooKeeperWatcher watcher) {
      super(watcher);
    }

    /**
     * Called when a new node has been created.
     * @param path full path of the new node
     */
    public void nodeCreated(String path) {
      refreshRegionServersList(path);
    }

    /**
     * Called when a node has been deleted
     * @param path full path of the deleted node
     */
    public void nodeDeleted(String path) {
      if (stopper.isStopped()) {
        return;
      }
      boolean cont = refreshRegionServersList(path);
      if (!cont) {
        return;
      }
      LOG.info(path + " znode expired, trying to lock it");
      transferQueues(ReplicationZookeeper.getZNodeName(path));
    }

    /**
     * Called when an existing node has a child node added or removed.
     * @param path full path of the node whose children have changed
     */
    public void nodeChildrenChanged(String path) {
      if (stopper == null || stopper.isStopped()) {
        return;
      }
      refreshRegionServersList(path);
    }

    private boolean refreshRegionServersList(String path) {
      if (!path.startsWith(zkHelper.getZookeeperWatcher().rsZNode)) {
        return false;
      }
      List<String> newRsList = (zkHelper.getRegisteredRegionServers());
      if (newRsList == null) {
        return false;
      } else {
        synchronized (otherRegionServers) {
          otherRegionServers.clear();
          otherRegionServers.addAll(newRsList);
        }
      }
      return true;
    }
  }

  /**
   * Watcher used to follow the creation and deletion of peer clusters.
   */
  public class PeersWatcher extends ZooKeeperListener {

    /**
     * Construct a ZooKeeper event listener.
     */
    public PeersWatcher(ZooKeeperWatcher watcher) {
      super(watcher);
    }

    /**
     * Called when a node has been deleted
     * @param path full path of the deleted node
     */
    public void nodeDeleted(String path) {
      List<String> peers = refreshPeersList(path);
      if (peers == null) {
        return;
      }
      String id = ReplicationZookeeper.getZNodeName(path);
      removePeer(id);
    }

    /**
     * Called when an existing node has a child node added or removed.
     * @param path full path of the node whose children have changed
     */
    public void nodeChildrenChanged(String path) {
      List<String> peers = refreshPeersList(path);
      if (peers == null) {
        return;
      }
      for (String id : peers) {
        try {
          boolean added = zkHelper.connectToPeer(id);
          if (added) {
            addSource(id);
          }
        } catch (IOException e) {
          // TODO manage better than that ?
          LOG.error("Error while adding a new peer", e);
        } catch (KeeperException e) {
          LOG.error("Error while adding a new peer", e);
        }
      }
    }

    /**
     * Verify if this event is meant for us, and if so then get the latest
     * peers' list from ZK. Also reset the watches.
     * @param path path to check against
     * @return A list of peers' identifiers if the event concerns this watcher,
     * else null.
     */
    private List<String> refreshPeersList(String path) {
      if (!path.startsWith(zkHelper.getPeersZNode())) {
        return null;
      }
      return zkHelper.listPeersIdsAndWatch();
    }
  }

  /**
   * Class responsible to setup new ReplicationSources to take care of the
   * queues from dead region servers.
   */
  class NodeFailoverWorker extends Thread {

    private String rsZnode;

    /**
     *
     * @param rsZnode
     */
    public NodeFailoverWorker(String rsZnode) {
      super("Failover-for-"+rsZnode);
      this.rsZnode = rsZnode;
    }

    @Override
    public void run() {
      // Wait a bit before transferring the queues, we may be shutting down.
      // This sleep may not be enough in some cases.
      try {
        Thread.sleep(sleepBeforeFailover);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting before transferring a queue.");
        Thread.currentThread().interrupt();
      }
      // We try to lock that rs' queue directory
      if (stopper.isStopped()) {
        LOG.info("Not transferring queue since we are shutting down");
        return;
      }
      if (!zkHelper.lockOtherRS(rsZnode)) {
        return;
      }
      LOG.info("Moving " + rsZnode + "'s hlogs to my queue");
      SortedMap<String, SortedSet<String>> newQueues =
          zkHelper.copyQueuesFromRS(rsZnode);
      zkHelper.deleteRsQueues(rsZnode);
      if (newQueues == null || newQueues.size() == 0) {
        return;
      }

      for (Map.Entry<String, SortedSet<String>> entry : newQueues.entrySet()) {
        String peerId = entry.getKey();
        try {
          ReplicationSourceInterface src = getReplicationSource(conf,
              fs, ReplicationSourceManager.this, stopper, replicating, peerId);
          if (!zkHelper.getPeerClusters().containsKey(src.getPeerClusterId())) {
            src.terminate("Recovered queue doesn't belong to any current peer");
            break;
          }
          oldsources.add(src);
          for (String hlog : entry.getValue()) {
            src.enqueueLog(new Path(oldLogDir, hlog));
          }
          // TODO set it to what's in ZK
          src.setSourceEnabled(true);
          src.startup();
        } catch (IOException e) {
          // TODO manage it
          LOG.error("Failed creating a source", e);
        }
      }
    }
  }

  /**
   * Get the directory where hlogs are archived
   * @return the directory where hlogs are archived
   */
  public Path getOldLogDir() {
    return this.oldLogDir;
  }

  /**
   * Get the directory where hlogs are stored by their RSs
   * @return the directory where hlogs are stored by their RSs
   */
  public Path getLogDir() {
    return this.logDir;
  }

  /**
   * Get the handle on the local file system
   * @return Handle on the local file system
   */
  public FileSystem getFs() {
    return this.fs;
  }
}
