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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;

/*****************************************************************************
 * Protocol that a secondary NameNode uses to communicate with the NameNode.
 * It's used to get part of the name node state
 *****************************************************************************/
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface NamenodeProtocol {
  /**
   * Until version 6L, this class served as both
   * the client interface to the NN AND the RPC protocol used to 
   * communicate with the NN.
   * 
   * This class is used by both the DFSClient and the 
   * NN server side to insulate from the protocol serialization.
   * 
   * If you are adding/changing NN's interface then you need to 
   * change both this class and ALSO related protocol buffer
   * wire protocol definition in NamenodeProtocol.proto.
   * 
   * For more details on protocol buffer wire protocol, please see 
   * .../org/apache/hadoop/hdfs/protocolPB/overview.html
   * 
   * 6: Switch to txid-based file naming for image and edits
   */
  public static final long versionID = 6L;

  // Error codes passed by errorReport().
  final static int NOTIFY = 0;
  final static int FATAL = 1;

  public final static int ACT_UNKNOWN = 0;    // unknown action   
  public final static int ACT_SHUTDOWN = 50;   // shutdown node
  public final static int ACT_CHECKPOINT = 51;   // do checkpoint

  /**
   * Get a list of blocks belonging to <code>datanode</code>
   * whose total size equals <code>size</code>.
   *
   * @see org.apache.hadoop.hdfs.server.balancer.Balancer
   * @param datanode  a data node
   * @param size      requested size
   * @param minBlockSize each block should be of this minimum Block Size
   * @return          a list of blocks & their locations
   * @throws IOException if size is less than or equal to 0 or
  datanode does not exist
   */
  @Idempotent
  BlocksWithLocations getBlocks(DatanodeInfo datanode, long size, long
      minBlockSize) throws IOException;

  /**
   * Get the current block keys
   * 
   * @return ExportedBlockKeys containing current block keys
   * @throws IOException 
   */
  @Idempotent
  public ExportedBlockKeys getBlockKeys() throws IOException;

  /**
   * @return The most recent transaction ID that has been synced to
   * persistent storage, or applied from persistent storage in the
   * case of a non-active node.
   * @throws IOException
   */
  @Idempotent
  public long getTransactionID() throws IOException;

  /**
   * Get the transaction ID of the most recent checkpoint.
   */
  @Idempotent
  public long getMostRecentCheckpointTxId() throws IOException;

  /**
   * Closes the current edit log and opens a new one. The 
   * call fails if the file system is in SafeMode.
   * @throws IOException
   * @return a unique token to identify this transaction.
   */
  @Idempotent
  public CheckpointSignature rollEditLog() throws IOException;

  /**
   * Request name-node version and storage information.
   * 
   * @return {@link NamespaceInfo} identifying versions and storage information 
   *          of the name-node
   * @throws IOException
   */
  @Idempotent
  public NamespaceInfo versionRequest() throws IOException;

  /**
   * Report to the active name-node an error occurred on a subordinate node.
   * Depending on the error code the active node may decide to unregister the
   * reporting node.
   * 
   * @param registration requesting node.
   * @param errorCode indicates the error
   * @param msg free text description of the error
   * @throws IOException
   */
  @Idempotent
  public void errorReport(NamenodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException;

  /** 
   * Register a subordinate name-node like backup node.
   *
   * @return  {@link NamenodeRegistration} of the node,
   *          which this node has just registered with.
   */
  @Idempotent
  public NamenodeRegistration registerSubordinateNamenode(
      NamenodeRegistration registration) throws IOException;

  /**
   * A request to the active name-node to start a checkpoint.
   * The name-node should decide whether to admit it or reject.
   * The name-node also decides what should be done with the backup node
   * image before and after the checkpoint.
   * 
   * @see CheckpointCommand
   * @see NamenodeCommand
   * @see #ACT_SHUTDOWN
   * 
   * @param registration the requesting node
   * @return {@link CheckpointCommand} if checkpoint is allowed.
   * @throws IOException
   */
  @AtMostOnce
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
  throws IOException;

  /**
   * A request to the active name-node to finalize
   * previously started checkpoint.
   * 
   * @param registration the requesting node
   * @param sig {@code CheckpointSignature} which identifies the checkpoint.
   * @throws IOException
   */
  @AtMostOnce
  public void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException;
  
  
  /**
   * Return a structure containing details about all edit logs
   * available to be fetched from the NameNode.
   * @param sinceTxId return only logs that contain transactions >= sinceTxId
   */
  @Idempotent
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
    throws IOException;

  /**
   * @return Whether the NameNode is in upgrade state (false) or not (true)
   */
  @Idempotent
  public boolean isUpgradeFinalized() throws IOException;

  /**
   * return whether the Namenode is rolling upgrade in progress (true) or
   * not (false).
   * @return
   * @throws IOException
   */
  @Idempotent
  boolean isRollingUpgrade() throws IOException;
}

