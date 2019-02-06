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

package org.apache.hadoop.hdds.scm.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.util.Set;

/**
 * Holds the nodes that currently host the container for an object key hash.
 */
@InterfaceAudience.Private
public final class LocatedContainer {
  private final String key;
  private final String matchedKeyPrefix;
  private final String containerName;
  private final Set<DatanodeInfo> locations;
  private final DatanodeInfo leader;

  /**
   * Creates a LocatedContainer.
   *
   * @param key object key
   * @param matchedKeyPrefix prefix of key that was used to find the location
   * @param containerName container name
   * @param locations nodes that currently host the container
   * @param leader node that currently acts as pipeline leader
   */
  public LocatedContainer(String key, String matchedKeyPrefix,
      String containerName, Set<DatanodeInfo> locations, DatanodeInfo leader) {
    this.key = key;
    this.matchedKeyPrefix = matchedKeyPrefix;
    this.containerName = containerName;
    this.locations = locations;
    this.leader = leader;
  }

  /**
   * Returns the container name.
   *
   * @return container name
   */
  public String getContainerName() {
    return this.containerName;
  }

  /**
   * Returns the object key.
   *
   * @return object key
   */
  public String getKey() {
    return this.key;
  }

  /**
   * Returns the node that currently acts as pipeline leader.
   *
   * @return node that currently acts as pipeline leader
   */
  public DatanodeInfo getLeader() {
    return this.leader;
  }

  /**
   * Returns the nodes that currently host the container.
   *
   * @return {@code Set<DatanodeInfo>} nodes that currently host the container
   */
  public Set<DatanodeInfo> getLocations() {
    return this.locations;
  }

  /**
   * Returns the prefix of the key that was used to find the location.
   *
   * @return prefix of the key that was used to find the location
   */
  public String getMatchedKeyPrefix() {
    return this.matchedKeyPrefix;
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == null) {
      return false;
    }
    if (!(otherObj instanceof LocatedContainer)) {
      return false;
    }
    LocatedContainer other = (LocatedContainer)otherObj;
    return this.key == null ? other.key == null : this.key.equals(other.key);
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "{key=" + key
        + "; matchedKeyPrefix=" + matchedKeyPrefix
        + "; containerName=" + containerName
        + "; locations=" + locations
        + "; leader=" + leader
        + "}";
  }
}
