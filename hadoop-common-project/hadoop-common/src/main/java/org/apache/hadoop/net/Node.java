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
package org.apache.hadoop.net;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** The interface defines a node in a network topology.
 * A node may be a leave representing a data node or an inner
 * node representing a datacenter or rack.
 * Each data has a name and its location in the network is
 * decided by a string with syntax similar to a file name. 
 * For example, a data node's name is hostname:port# and if it's located at
 * rack "orange" in datacenter "dog", the string representation of its
 * network location is /dog/orange
 */

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public interface Node {
  /** @return the string representation of this node's network location */
  public String getNetworkLocation();

  /** Set this node's network location
   * @param location the location
   */
  public void setNetworkLocation(String location);

  /** @return this node's name */
  public String getName();

  /** @return this node's parent */
  public Node getParent();

  /** Set this node's parent
   * @param parent the parent
   */
  public void setParent(Node parent);

  /** @return this node's level in the tree.
   * E.g. the root of a tree returns 0 and its children return 1
   */
  public int getLevel();

  /** Set this node's level in the tree
   * @param i the level
   */
  public void setLevel(int i);
}
