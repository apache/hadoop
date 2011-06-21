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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

/**
 * This is a utility class to expose NameNode functionality for unit tests.
 */
public class NameNodeAdapter {
  /**
   * Get the namesystem from the namenode
   */
  public static FSNamesystem getNamesystem(NameNode namenode) {
    return namenode.getNamesystem();
  }

  /**
   * Get block locations within the specified range.
   */
  public static LocatedBlocks getBlockLocations(NameNode namenode,
      String src, long offset, long length) throws IOException {
    return namenode.getNamesystem().getBlockLocations(
        src, offset, length, false, true);
  }

  /**
   * Refresh block queue counts on the name-node.
   * @param namenode to proxy the invocation to
   */
  public static void refreshBlockCounts(NameNode namenode) {
    namenode.getNamesystem().blockManager.updateState();
  }

  /**
   * Get the internal RPC server instance.
   * @return rpc server
   */
  public static Server getRpcServer(NameNode namenode) {
    return namenode.server;
  }

  /**
   * Return a tuple of the replica state (number racks, number live
   * replicas, and number needed replicas) for the given block.
   * @param namenode to proxy the invocation to.
   */
  public static int[] getReplicaInfo(NameNode namenode, Block b) {
    FSNamesystem ns = namenode.getNamesystem();
    ns.readLock();
    int[] r = {ns.blockManager.getNumberOfRacks(b),
               ns.blockManager.countNodes(b).liveReplicas(),
               ns.blockManager.neededReplications.contains(b) ? 1 : 0};
    ns.readUnlock();
    return r;
  }
  
  public static String getLeaseHolderForPath(NameNode namenode, String path) {
    return namenode.getNamesystem().leaseManager.getLeaseByPath(path).getHolder();
  }

  /**
   * Return the datanode descriptor for the given datanode.
   */
  public static DatanodeDescriptor getDatanode(NameNode namenode,
      DatanodeID id) throws IOException {
    FSNamesystem ns = namenode.getNamesystem();
    ns.readLock();
    try {
      return ns.getDatanode(id);
    } finally {
      ns.readUnlock();
    }
  }
}
