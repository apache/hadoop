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
package org.apache.hadoop.hbase.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class servers as a helper for all things related to zookeeper
 * in the replication contrib.
 */
public class ReplicationZookeeperHelper implements HConstants, Watcher {

  static final Log LOG = LogFactory.getLog(ReplicationZookeeperHelper.class);

  private final ZooKeeperWrapper zookeeperWrapper;

  private final List<ZooKeeperWrapper> peerClusters;

  private final String replicationZNode;
  private final String peersZNode;

  private final String replicationStateNodeName;

  private final boolean isMaster;

  private final Configuration conf;

  private final AtomicBoolean isReplicating;

  /**
   * Constructor used by region servers
   * @param zookeeperWrapper zkw to wrap
   * @param conf conf to use
   * @param isReplicating atomic boolean to start/stop replication
   * @throws IOException
   */
  public ReplicationZookeeperHelper(
      ZooKeeperWrapper zookeeperWrapper, Configuration conf,
      final AtomicBoolean isReplicating) throws IOException{
    this.zookeeperWrapper = zookeeperWrapper;
    this.conf = conf;
    String replicationZNodeName =
        conf.get("zookeeper.znode.replication", "replication");
    String peersZNodeName =
        conf.get("zookeeper.znode.peers", "peers");
    String repMasterZNodeName =
        conf.get("zookeeper.znode.master", "master");
    this.replicationStateNodeName =
        conf.get("zookeeper.znode.state", "state");


    this.peerClusters = new ArrayList<ZooKeeperWrapper>();
    this.replicationZNode = zookeeperWrapper.getZNode(
        zookeeperWrapper.getParentZNode(),replicationZNodeName);
    this.peersZNode =
        zookeeperWrapper.getZNode(replicationZNode,peersZNodeName);
    
    List<String> znodes =
        this.zookeeperWrapper.listZnodes(this.peersZNode, this);
    if(znodes != null) {
      for(String znode : znodes) {
        connectToPeer(znode);
      }
    }
    String address = this.zookeeperWrapper.getData(this.replicationZNode,
        repMasterZNodeName);

    String thisCluster = this.conf.get(ZOOKEEPER_QUORUM)+":"+
        this.conf.get("hbase.zookeeper.property.clientPort") +":" +
        this.conf.get(ZOOKEEPER_ZNODE_PARENT);

    this.isMaster = thisCluster.equals(address);
    
    LOG.info("This cluster (" + thisCluster + ") is a "
        + (this.isMaster ? "master" : "slave") + " for replication" +
        ", compared with (" + address + ")");

    this.isReplicating = isReplicating;

    setIsReplicating();
  }

  /**
   * Returns all region servers from given peer
   * @param clusterIndex the cluster to interrogate
   * @return addresses of all region servers
   */
  public List<HServerAddress> getPeersAddresses(int clusterIndex) {
    return this.peerClusters.size() == 0 ?
        null : this.peerClusters.get(clusterIndex).scanRSDirectory();
  }

  // This method connects this cluster to another one and registers it
  private void connectToPeer(String znode) throws IOException {
    String[] quorum =
        this.zookeeperWrapper.getData(this.peersZNode, znode).split(":");
    if(quorum.length == 3) {
      Configuration otherConf = new Configuration(this.conf);
      otherConf.set(ZOOKEEPER_QUORUM, quorum[0]);
      otherConf.set("hbase.zookeeper.property.clientPort", quorum[1]);
      otherConf.set(ZOOKEEPER_ZNODE_PARENT, quorum[2]);
      this.peerClusters.add(new ZooKeeperWrapper(otherConf, this));
      LOG.info("Added new peer cluster " + StringUtils.arrayToString(quorum));
    } else {
      LOG.error("Wrong format of cluster address: " +
          this.zookeeperWrapper.getData(this.peersZNode, znode));
    }
  }

  /**
   * Tells if this cluster replicates or not
   * @return
   */
  public boolean isMaster() {
    return isMaster;
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    Event.EventType type = watchedEvent.getType();
    LOG.info(("Got event " + type + " with path " + watchedEvent.getPath()));
    if (type.equals(Event.EventType.NodeDataChanged)) {
      setIsReplicating();
    }
  }

  /**
   * This reads the state znode for replication and sets the atomic boolean
   */
  private void setIsReplicating() {
    String value = this.zookeeperWrapper.getDataAndWatch(
        this.replicationZNode, this.replicationStateNodeName, this);
    if(value != null) {
      isReplicating.set(value.equals("true"));
      LOG.info("Replication is now " + (isReplicating.get() ?
          "started" : "stopped"));
    }
  }
}
