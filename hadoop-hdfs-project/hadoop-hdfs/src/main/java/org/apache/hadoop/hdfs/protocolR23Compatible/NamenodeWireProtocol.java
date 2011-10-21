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

package org.apache.hadoop.hdfs.protocolR23Compatible;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;

/*****************************************************************************
 * Protocol that a secondary NameNode uses to communicate with the NameNode.
 * It's used to get part of the name node state
 *****************************************************************************/
/** 
 * This class defines the actual protocol used to communicate between namenodes.
 * The parameters in the methods which are specified in the
 * package are separate from those used internally in the DN and DFSClient
 * and hence need to be converted using {@link NamenodeProtocolTranslatorR23}
 * and {@link NamenodeProtocolServerSideTranslatorR23}.
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
    clientPrincipal = DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY)
@InterfaceAudience.Private
public interface NamenodeWireProtocol extends VersionedProtocol {
  /**
   * The  rules for changing this protocol are the same as that for
   * {@link ClientNamenodeWireProtocol} - see that java file for details.
   */
  public static final long versionID = 6L;

  /**
   * Get a list of blocks belonging to <code>datanode</code>
   * whose total size equals <code>size</code>.
   * 
   * @see org.apache.hadoop.hdfs.server.balancer.Balancer
   * @param datanode  a data node
   * @param size      requested size
   * @return          a list of blocks & their locations
   * @throws RemoteException if size is less than or equal to 0 or
   *                               datanode does not exist
   */
  public BlocksWithLocationsWritable getBlocks(DatanodeInfoWritable datanode,
      long size) throws IOException;

  /**
   * Get the current block keys
   * 
   * @return ExportedBlockKeys containing current block keys
   * @throws IOException 
   */
  public ExportedBlockKeysWritable getBlockKeys() throws IOException;

  /**
   * @return The most recent transaction ID that has been synced to
   * persistent storage.
   * @throws IOException
   */
  public long getTransactionID() throws IOException;

  /**
   * Closes the current edit log and opens a new one. The 
   * call fails if the file system is in SafeMode.
   * @throws IOException
   * @return a unique token to identify this transaction.
   * @deprecated 
   *    See {@link org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode}
   */
  @Deprecated
  public CheckpointSignatureWritable rollEditLog() throws IOException;

  /**
   * Request name-node version and storage information.
   * @throws IOException
   */
  public NamespaceInfoWritable versionRequest() throws IOException;

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
  public void errorReport(NamenodeRegistrationWritable registration,
                          int errorCode, 
                          String msg) throws IOException;

  /** 
   * Register a subordinate name-node like backup node.
   *
   * @return  {@link NamenodeRegistration} of the node,
   *          which this node has just registered with.
   */
  public NamenodeRegistrationWritable register(
      NamenodeRegistrationWritable registration) throws IOException;

  /**
   * A request to the active name-node to start a checkpoint.
   * The name-node should decide whether to admit it or reject.
   * The name-node also decides what should be done with the backup node
   * image before and after the checkpoint.
   * 
   * @see CheckpointCommand
   * @see NamenodeCommandWritable
   * @see #ACT_SHUTDOWN
   * 
   * @param registration the requesting node
   * @return {@link CheckpointCommand} if checkpoint is allowed.
   * @throws IOException
   */
  public NamenodeCommandWritable startCheckpoint(
      NamenodeRegistrationWritable registration) throws IOException;

  /**
   * A request to the active name-node to finalize
   * previously started checkpoint.
   * 
   * @param registration the requesting node
   * @param sig {@code CheckpointSignature} which identifies the checkpoint.
   * @throws IOException
   */
  public void endCheckpoint(NamenodeRegistrationWritable registration,
                            CheckpointSignatureWritable sig) throws IOException;
  
  
  /**
   * Return a structure containing details about all edit logs
   * available to be fetched from the NameNode.
   * @param sinceTxId return only logs that contain transactions >= sinceTxId
   */
  public RemoteEditLogManifestWritable getEditLogManifest(long sinceTxId)
    throws IOException;
  
  /**
   * This method is defined to get the protocol signature using 
   * the R23 protocol - hence we have added the suffix of 2 the method name
   * to avoid conflict.
   */
  public org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable
           getProtocolSignature2(String protocol, 
      long clientVersion,
      int clientMethodsHash) throws IOException;
}

