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

import java.util.List;
import java.util.stream.Collectors;

/**
 * Holds the nodes that currently host the block for a block key.
 */
@InterfaceAudience.Private
public final class ScmLocatedBlock {
  private final String key;
  private final List<DatanodeInfo> locations;
  private final DatanodeInfo leader;

  /**
   * Creates a ScmLocatedBlock.
   *
   * @param key object key
   * @param locations nodes that currently host the block
   * @param leader node that currently acts as pipeline leader
   */
  public ScmLocatedBlock(final String key, final List<DatanodeInfo> locations,
      final DatanodeInfo leader) {
    this.key = key;
    this.locations = locations;
    this.leader = leader;
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
   * Returns the nodes that currently host the block.
   *
   * @return {@literal List<DatanodeInfo>} nodes that currently host the block
   */
  public List<DatanodeInfo> getLocations() {
    return this.locations;
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == null) {
      return false;
    }
    if (!(otherObj instanceof ScmLocatedBlock)) {
      return false;
    }
    ScmLocatedBlock other = (ScmLocatedBlock)otherObj;
    return this.key == null ? other.key == null : this.key.equals(other.key);
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{key=" + key + "; locations="
        + locations.stream().map(loc -> loc.toString()).collect(Collectors
            .joining(",")) + "; leader=" + leader + "}";
  }
}
