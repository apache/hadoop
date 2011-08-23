/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import java.util.List;
import java.util.Map;

/**
 * Makes decisions about the placement and movement of Regions across
 * RegionServers.
 *
 * <p>Cluster-wide load balancing will occur only when there are no regions in
 * transition and according to a fixed period of a time using {@link #balanceCluster(Map)}.
 *
 * <p>Inline region placement with {@link #immediateAssignment} can be used when
 * the Master needs to handle closed regions that it currently does not have
 * a destination set for.  This can happen during master failover.
 *
 * <p>On cluster startup, bulk assignment can be used to determine
 * locations for all Regions in a cluster.
 *
 * <p>This classes produces plans for the {@link AssignmentManager} to execute.
 */
public interface LoadBalancer extends Configurable {

  /**
   * Set the current cluster status.  This allows a LoadBalancer to map host name to a server
   * @param st
   */
  public void setClusterStatus(ClusterStatus st);


  /**
   * Set the master service.
   * @param masterServices
   */
  public void setMasterServices(MasterServices masterServices);

  /**
   * Perform the major balance operation
   * @param clusterState
   * @return
   */
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState);

  /**
   * Perform a Round Robin assignment of regions.
   * @param regions
   * @param servers
   * @return
   */
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions, List<ServerName> servers);

  /**
   * Assign regions to the previously hosting region server
   * @param regions
   * @param servers
   * @return
   */
  public Map<ServerName, List<HRegionInfo>> retainAssignment(Map<HRegionInfo, ServerName> regions, List<ServerName> servers);

  /**
   * Sync assign a region
   * @param regions
   * @param servers
   * @return
   */
  public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regions, List<ServerName> servers);

  /**
   * Get a random region server from the list
   * @param servers
   * @return
   */
  public ServerName randomAssignment(List<ServerName> servers);
}
