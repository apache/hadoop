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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * This class acts as a wrapper for all the objects used to identify and
 * communicate with remote peers. Everything needs to be created for objects
 * of this class as it doesn't encapsulate any specific functionality e.g.
 * it's a container class.
 */
public class ReplicationPeer {

  private final String clusterKey;
  private final String id;
  private List<ServerName> regionServers = new ArrayList<ServerName>(0);
  private final AtomicBoolean peerEnabled = new AtomicBoolean();
  // Cannot be final since a new object needs to be recreated when session fails
  private ZooKeeperWatcher zkw;
  private final Configuration conf;

  /**
   * Constructor that takes all the objects required to communicate with the
   * specified peer, except for the region server addresses.
   * @param conf configuration object to this peer
   * @param key cluster key used to locate the peer
   * @param id string representation of this peer's identifier
   * @param zkw zookeeper connection to the peer
   */
  public ReplicationPeer(Configuration conf, String key,
      String id, ZooKeeperWatcher zkw) {
    this.conf = conf;
    this.clusterKey = key;
    this.id = id;
    this.zkw = zkw;
  }

  /**
   * Get the cluster key of that peer
   * @return string consisting of zk ensemble addresses, client port
   * and root znode
   */
  public String getClusterKey() {
    return clusterKey;
  }

  /**
   * Get the state of this peer
   * @return atomic boolean that holds the status
   */
  public AtomicBoolean getPeerEnabled() {
    return peerEnabled;
  }

  /**
   * Get a list of all the addresses of all the region servers
   * for this peer cluster
   * @return list of addresses
   */
  public List<ServerName> getRegionServers() {
    return regionServers;
  }

  /**
   * Set the list of region servers for that peer
   * @param regionServers list of addresses for the region servers
   */
  public void setRegionServers(List<ServerName> regionServers) {
    this.regionServers = regionServers;
  }

  /**
   * Get the ZK connection to this peer
   * @return zk connection
   */
  public ZooKeeperWatcher getZkw() {
    return zkw;
  }

  /**
   * Get the identifier of this peer
   * @return string representation of the id (short)
   */
  public String getId() {
    return id;
  }

  /**
   * Get the configuration object required to communicate with this peer
   * @return configuration object
   */
  public Configuration getConfiguration() {
    return conf;
  }
}
