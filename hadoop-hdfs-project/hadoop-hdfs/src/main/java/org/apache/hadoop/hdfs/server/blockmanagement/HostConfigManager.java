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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hdfs.protocol.DatanodeID;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * This interface abstracts how datanode configuration is managed.
 *
 * Each implementation defines its own way to persist the configuration.
 * For example, it can use one JSON file to store the configs for all
 * datanodes; or it can use one file to store in-service datanodes and another
 * file to store decommission-requested datanodes.
 *
 * These files control which DataNodes the NameNode expects to see in the
 * cluster.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class HostConfigManager implements Configurable {

  /**
   * Return all the datanodes that are allowed to connect to the namenode.
   * @return Iterable of all datanodes
   */
  public abstract Iterable<InetSocketAddress> getIncludes();

  /**
   * Return all datanodes that should be in decommissioned state.
   * @return Iterable of those datanodes
   */
  public abstract Iterable<InetSocketAddress> getExcludes();

  /**
   * Check if a datanode is allowed to connect the namenode.
   * @param dn the DatanodeID of the datanode
   * @return boolean if dn is allowed to connect the namenode.
   */
  public abstract boolean isIncluded(DatanodeID dn);

  /**
   * Check if a datanode needs to be decommissioned.
   * @param dn the DatanodeID of the datanode
   * @return boolean if dn needs to be decommissioned.
   */
  public abstract boolean isExcluded(DatanodeID dn);

  /**
   * Reload the configuration.
   */
  public abstract void refresh() throws IOException;

  /**
   * Get the upgrade domain of a datanode.
   * @param dn the DatanodeID of the datanode
   * @return the upgrade domain of dn.
   */
  public abstract String getUpgradeDomain(DatanodeID dn);
}
