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

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.ipc.Server;

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
   * Get the internal RPC server instance.
   * @return rpc server
   */
  public static Server getRpcServer(NameNode namenode) {
    return ((NameNodeRpcServer)namenode.getRpcServer()).clientRpcServer;
  }

  public static DelegationTokenSecretManager getDtSecretManager(
      final FSNamesystem ns) {
    return ns.getDelegationTokenSecretManager();
  }

  public static DatanodeCommand[] sendHeartBeat(DatanodeRegistration nodeReg,
      DatanodeDescriptor dd, FSNamesystem namesystem) throws IOException {
    return namesystem.handleHeartbeat(nodeReg, dd.getCapacity(), 
        dd.getDfsUsed(), dd.getRemaining(), dd.getBlockPoolUsed(), 0, 0, 0);
  }

  public static boolean setReplication(final FSNamesystem ns,
      final String src, final short replication) throws IOException {
    return ns.setReplication(src, replication);
  }
  
  public static LeaseManager getLeaseManager(final FSNamesystem ns) {
    return ns.leaseManager;
  }

  /** Set the softLimit and hardLimit of client lease periods. */
  public static void setLeasePeriod(final FSNamesystem namesystem, long soft, long hard) {
    getLeaseManager(namesystem).setLeasePeriod(soft, hard);
    namesystem.leaseManager.triggerMonitorCheckNow();
  }

  public static String getLeaseHolderForPath(NameNode namenode, String path) {
    return namenode.getNamesystem().leaseManager.getLeaseByPath(path).getHolder();
  }

  /**
   * Return the datanode descriptor for the given datanode.
   */
  public static DatanodeDescriptor getDatanode(final FSNamesystem ns,
      DatanodeID id) throws IOException {
    ns.readLock();
    try {
      return ns.getBlockManager().getDatanodeManager().getDatanode(id);
    } finally {
      ns.readUnlock();
    }
  }
  
  /**
   * Return the FSNamesystem stats
   */
  public static long[] getStats(final FSNamesystem fsn) {
    return fsn.getStats();
  }
}
