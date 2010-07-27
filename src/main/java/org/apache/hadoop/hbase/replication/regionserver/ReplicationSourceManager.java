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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.LogActionsListener;
import org.apache.hadoop.hbase.replication.ReplicationZookeeperWrapper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

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
public class ReplicationSourceManager implements LogActionsListener {

  private static final Log LOG =
      LogFactory.getLog(ReplicationSourceManager.class);
  // List of all the sources that read this RS's logs
  private final List<ReplicationSourceInterface> sources;
  // List of all the sources we got from died RSs
  private final List<ReplicationSourceInterface> oldsources;
  // Indicates if we are currently replicating
  private final AtomicBoolean replicating;
  // Helper for zookeeper
  private final ReplicationZookeeperWrapper zkHelper;
  // Indicates if the region server is closing
  private final AtomicBoolean stopper;
  // All logs we are currently trackign
  private final SortedSet<String> hlogs;
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
  public ReplicationSourceManager(final ReplicationZookeeperWrapper zkHelper,
                                  final Configuration conf,
                                  final AtomicBoolean stopper,
                                  final FileSystem fs,
                                  final AtomicBoolean replicating,
                                  final Path logDir,
                                  final Path oldLogDir) {
    this.sources = new ArrayList<ReplicationSourceInterface>();
    this.replicating = replicating;
    this.zkHelper = zkHelper;
    this.stopper = stopper;
    this.hlogs = new TreeSet<String>();
    this.oldsources = new ArrayList<ReplicationSourceInterface>();
    this.conf = conf;
    this.fs = fs;
    this.logDir = logDir;
    this.oldLogDir = oldLogDir;
    List<String> otherRSs =
        this.zkHelper.getRegisteredRegionServers(new OtherRegionServerWatcher());
    this.otherRegionServers = otherRSs == null ? new ArrayList<String>() : otherRSs;
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
    this.zkHelper.writeReplicationStatus(key.toString(), id, position);
    synchronized (this.hlogs) {
      if (!queueRecovered && this.hlogs.first() != key) {
        SortedSet<String> hlogSet = this.hlogs.headSet(key);
        LOG.info("Removing " + hlogSet.size() +
            " logs in the list: " + hlogSet);
        for (String hlog : hlogSet) {
          this.zkHelper.removeLogFromList(hlog.toString(), id);
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
      ReplicationSourceInterface src = addSource(id);
      src.startup();
    }
    List<String> currentReplicators = this.zkHelper.getListOfReplicators(null);
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
   * @return the created source
   * @throws IOException
   */
  public ReplicationSourceInterface addSource(String id) throws IOException {
    ReplicationSourceInterface src =
        getReplicationSource(this.conf, this.fs, this, stopper, replicating, id);
    this.sources.add(src);
    synchronized (this.hlogs) {
      if (this.hlogs.size() > 0) {
        this.zkHelper.addLogToList(this.hlogs.first(),
            this.sources.get(0).getPeerClusterZnode());
        src.enqueueLog(this.latestPath);
      }
    }
    return src;
  }

  /**
   * Terminate the replication on this region server
   */
  public void join() {
    if (this.sources.size() == 0) {
      this.zkHelper.deleteOwnRSZNode();
    }
    for (ReplicationSourceInterface source : this.sources) {
      source.terminate();
    }
  }

  /**
   * Get a copy of the hlogs of the first source on this rs
   * @return a sorted set of hlog names
   */
  protected SortedSet<String> getHLogs() {
    return new TreeSet(this.hlogs);
  }

  /**
   * Get a list of all the normal sources of this rs
   * @return lis of all sources
   */
  public List<ReplicationSourceInterface> getSources() {
    return this.sources;
  }

  @Override
  public void logRolled(Path newLog) {
    if (this.sources.size() > 0) {
      this.zkHelper.addLogToList(newLog.getName(),
          this.sources.get(0).getPeerClusterZnode());
    }
    synchronized (this.hlogs) {
      this.hlogs.add(newLog.getName());
    }
    this.latestPath = newLog;
    // This only update the sources we own, not the recovered ones
    for (ReplicationSourceInterface source : this.sources) {
      source.enqueueLog(newLog);
    }
  }

  /**
   * Get the ZK help of this manager
   * @return the helper
   */
  public ReplicationZookeeperWrapper getRepZkWrapper() {
    return zkHelper;
  }

  /**
   * Factory method to create a replication source
   * @param conf the configuration to use
   * @param fs the file system to use
   * @param manager the manager to use
   * @param stopper the stopper object for this region server
   * @param replicating the status of the replication on this cluster
   * @param peerClusterId the id of the peer cluster
   * @return the created source
   * @throws IOException
   */
  public ReplicationSourceInterface getReplicationSource(
      final Configuration conf,
      final FileSystem fs,
      final ReplicationSourceManager manager,
      final AtomicBoolean stopper,
      final AtomicBoolean replicating,
      final String peerClusterId) throws IOException {
    ReplicationSourceInterface src;
    try {
      Class c = Class.forName(conf.get("replication.replicationsource.implementation",
          ReplicationSource.class.getCanonicalName()));
      src = (ReplicationSourceInterface) c.newInstance();
    } catch (Exception e) {
      LOG.warn("Passed replication source implemention throws errors, " +
          "defaulting to ReplicationSource", e);
      src = new ReplicationSource();

    }
    src.init(conf, fs, manager, stopper, replicating, peerClusterId);
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
    // We try to lock that rs' queue directory
    if (this.stopper.get()) {
      LOG.info("Not transferring queue since we are shutting down");
      return;
    }
    if (!this.zkHelper.lockOtherRS(rsZnode)) {
      return;
    }
    LOG.info("Moving " + rsZnode + "'s hlogs to my queue");
    SortedMap<String, SortedSet<String>> newQueues =
        this.zkHelper.copyQueuesFromRS(rsZnode);
    if (newQueues == null || newQueues.size() == 0) {
      return;
    }
    this.zkHelper.deleteRsQueues(rsZnode);

    for (Map.Entry<String, SortedSet<String>> entry : newQueues.entrySet()) {
      String peerId = entry.getKey();
      try {
        ReplicationSourceInterface src = getReplicationSource(this.conf,
            this.fs, this, this.stopper, this.replicating, peerId);
        this.oldsources.add(src);
        for (String hlog : entry.getValue()) {
          src.enqueueLog(new Path(this.oldLogDir, hlog));
        }
        src.startup();
      } catch (IOException e) {
        // TODO manage it
        LOG.error("Failed creating a source", e);
      }
    }
  }

  /**
   * Clear the references to the specified old source
   * @param src source to clear
   */
  public void closeRecoveredQueue(ReplicationSourceInterface src) {
    LOG.info("Done with the recovered queue " + src.getPeerClusterZnode());
    this.oldsources.remove(src);
    this.zkHelper.deleteSource(src.getPeerClusterZnode());
  }

  /**
   * Watcher used to be notified of the other region server's death
   * in the local cluster. It initiates the process to transfer the queues
   * if it is able to grab the lock.
   */
  public class OtherRegionServerWatcher implements Watcher {
    @Override
    public void process(WatchedEvent watchedEvent) {
      LOG.info(" event " + watchedEvent);
      if (watchedEvent.getType().equals(Event.KeeperState.Expired) ||
          watchedEvent.getType().equals(Event.KeeperState.Disconnected)) {
        return;
      }

      List<String> newRsList = (zkHelper.getRegisteredRegionServers(this));
      if (newRsList == null) {
        return;
      } else {
        synchronized (otherRegionServers) {
          otherRegionServers.clear();
          otherRegionServers.addAll(newRsList);
        }
      }
      if (watchedEvent.getType().equals(Event.EventType.NodeDeleted)) {
        LOG.info(watchedEvent.getPath() + " znode expired, trying to lock it");
        String[] rsZnodeParts = watchedEvent.getPath().split("/");
        transferQueues(rsZnodeParts[rsZnodeParts.length-1]);
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
   * @returnthe handle on the local file system
   */
  public FileSystem getFs() {
    return this.fs;
  }

}
