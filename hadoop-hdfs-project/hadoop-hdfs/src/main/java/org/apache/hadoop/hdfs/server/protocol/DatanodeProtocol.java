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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.*;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;

import javax.annotation.Nonnull;

/**********************************************************************
 * Protocol that a DFS datanode uses to communicate with the NameNode.
 * It's used to upload current load information and block reports.
 *
 * The only way a NameNode can communicate with a DataNode is by
 * returning values from these functions.
 *
 **********************************************************************/
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, 
    clientPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface DatanodeProtocol {
  /**
   * This class is used by both the Namenode (client) and BackupNode (server) 
   * to insulate from the protocol serialization.
   * 
   * If you are adding/changing DN's interface then you need to 
   * change both this class and ALSO related protocol buffer
   * wire protocol definition in DatanodeProtocol.proto.
   * 
   * For more details on protocol buffer wire protocol, please see 
   * .../org/apache/hadoop/hdfs/protocolPB/overview.html
   */
  public static final long versionID = 28L;
  
  // error code
  final static int NOTIFY = 0;
  final static int DISK_ERROR = 1; // there are still valid volumes on DN
  final static int INVALID_BLOCK = 2;
  final static int FATAL_DISK_ERROR = 3; // no valid volumes left on DN

  /**
   * Determines actions that data node should perform 
   * when receiving a datanode command. 
   */
  final static int DNA_UNKNOWN = 0;    // unknown action   
  final static int DNA_TRANSFER = 1;   // transfer blocks to another datanode
  final static int DNA_INVALIDATE = 2; // invalidate blocks
  final static int DNA_SHUTDOWN = 3;   // shutdown node
  final static int DNA_REGISTER = 4;   // re-register
  final static int DNA_FINALIZE = 5;   // finalize previous upgrade
  final static int DNA_RECOVERBLOCK = 6;  // request a block recovery
  final static int DNA_ACCESSKEYUPDATE = 7;  // update access key
  final static int DNA_BALANCERBANDWIDTHUPDATE = 8; // update balancer bandwidth
  final static int DNA_CACHE = 9;      // cache blocks
  final static int DNA_UNCACHE = 10;   // uncache blocks
  final static int DNA_ERASURE_CODING_RECONSTRUCTION = 11; // erasure coding reconstruction command
  int DNA_BLOCK_STORAGE_MOVEMENT = 12; // block storage movement command
  int DNA_DROP_SPS_WORK_COMMAND = 13; // drop sps work command

  /** 
   * Register Datanode.
   *
   * @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem#registerDatanode(DatanodeRegistration)
   * @param registration datanode registration information
   * @return the given {@link org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration} with
   *  updated registration information
   */
  @Idempotent
  public DatanodeRegistration registerDatanode(DatanodeRegistration registration
      ) throws IOException;
  
  /**
   * sendHeartbeat() tells the NameNode that the DataNode is still
   * alive and well.  Includes some status info, too. 
   * It also gives the NameNode a chance to return 
   * an array of "DatanodeCommand" objects in HeartbeatResponse.
   * A DatanodeCommand tells the DataNode to invalidate local block(s), 
   * or to copy them to other DataNodes, etc.
   * @param registration datanode registration information
   * @param reports utilization report per storage
   * @param xmitsInProgress number of transfers from this datanode to others
   * @param xceiverCount number of active transceiver threads
   * @param failedVolumes number of failed volumes
   * @param volumeFailureSummary info about volume failures
   * @param requestFullBlockReportLease whether to request a full block
   *                                    report lease.
   * @param slowPeers Details of peer DataNodes that were detected as being
   *                  slow to respond to packet writes. Empty report if no
   *                  slow peers were detected by the DataNode.
   * @throws IOException on error
   */
  @Idempotent
  public HeartbeatResponse sendHeartbeat(DatanodeRegistration registration,
                                       StorageReport[] reports,
                                       long dnCacheCapacity,
                                       long dnCacheUsed,
                                       int xmitsInProgress,
                                       int xceiverCount,
                                       int failedVolumes,
                                       VolumeFailureSummary volumeFailureSummary,
                                       boolean requestFullBlockReportLease,
                                       @Nonnull SlowPeerReports slowPeers,
                                       @Nonnull SlowDiskReports slowDisks)
      throws IOException;

  /**
   * blockReport() tells the NameNode about all the locally-stored blocks.
   * The NameNode returns an array of Blocks that have become obsolete
   * and should be deleted.  This function is meant to upload *all*
   * the locally-stored blocks.  It's invoked upon startup and then
   * infrequently afterwards.
   * @param registration datanode registration
   * @param poolId the block pool ID for the blocks
   * @param reports report of blocks per storage
   *     Each finalized block is represented as 3 longs. Each under-
   *     construction replica is represented as 4 longs.
   *     This is done instead of Block[] to reduce memory used by block reports.
   * @param context Context information for this block report.
   *
   * @return - the next command for DN to process.
   * @throws IOException
   */
  @Idempotent
  public DatanodeCommand blockReport(DatanodeRegistration registration,
            String poolId, StorageBlockReport[] reports,
            BlockReportContext context) throws IOException;
    

  /**
   * Communicates the complete list of locally cached blocks to the NameNode.
   * 
   * This method is similar to
   * {@link #blockReport(DatanodeRegistration, String, StorageBlockReport[], BlockReportContext)},
   * which is used to communicated blocks stored on disk.
   *
   * @param            The datanode registration.
   * @param poolId     The block pool ID for the blocks.
   * @param blockIds   A list of block IDs.
   * @return           The DatanodeCommand.
   * @throws IOException
   */
  @Idempotent
  public DatanodeCommand cacheReport(DatanodeRegistration registration,
      String poolId, List<Long> blockIds) throws IOException;

  /**
   * blockReceivedAndDeleted() allows the DataNode to tell the NameNode about
   * recently-received and -deleted block data. 
   * 
   * For the case of received blocks, a hint for preferred replica to be 
   * deleted when there is any excessive blocks is provided.
   * For example, whenever client code
   * writes a new Block here, or another DataNode copies a Block to
   * this DataNode, it will call blockReceived().
   */
  @Idempotent
  public void blockReceivedAndDeleted(DatanodeRegistration registration,
                            String poolId,
                            StorageReceivedDeletedBlocks[] rcvdAndDeletedBlocks)
                            throws IOException;

  /**
   * errorReport() tells the NameNode about something that has gone
   * awry.  Useful for debugging.
   */
  @Idempotent
  public void errorReport(DatanodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException;
    
  @Idempotent
  public NamespaceInfo versionRequest() throws IOException;

  /**
   * same as {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#reportBadBlocks(LocatedBlock[])}
   * }
   */
  @Idempotent
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException;
  
  /**
   * Commit block synchronization in lease recovery
   */
  @Idempotent
  public void commitBlockSynchronization(ExtendedBlock block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets,
      String[] newtargetstorages) throws IOException;
}
