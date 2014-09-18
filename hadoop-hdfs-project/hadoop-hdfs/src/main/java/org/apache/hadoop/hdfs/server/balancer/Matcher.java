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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

/** A matcher interface for matching nodes. */
public interface Matcher {
  /** Given the cluster topology, does the left node match the right node? */
  public boolean match(NetworkTopology cluster, Node left,  Node right);

  /** Match datanodes in the same node group. */
  public static final Matcher SAME_NODE_GROUP = new Matcher() {
    @Override
    public boolean match(NetworkTopology cluster, Node left, Node right) {
      return cluster.isOnSameNodeGroup(left, right);
    }

    @Override
    public String toString() {
      return "SAME_NODE_GROUP";
    }
  };

  /** Match datanodes in the same rack. */
  public static final Matcher SAME_RACK = new Matcher() {
    @Override
    public boolean match(NetworkTopology cluster, Node left, Node right) {
      return cluster.isOnSameRack(left, right);
    }

    @Override
    public String toString() {
      return "SAME_RACK";
    }
  };

  /** Match any datanode with any other datanode. */
  public static final Matcher ANY_OTHER = new Matcher() {
    @Override
    public boolean match(NetworkTopology cluster, Node left, Node right) {
      return left != right;
    }

    @Override
    public String toString() {
      return "ANY_OTHER";
    }
  };
}