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
import org.apache.hadoop.hdfs.security.ExportedAccessKeys;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;

/*****************************************************************************
 * Protocol that a secondary NameNode uses to communicate with the NameNode.
 * It's used to get part of the name node state
 *****************************************************************************/
@KerberosInfo(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY)
@InterfaceAudience.Private
public interface NamenodeProtocol extends VersionedProtocol {
  /**
   * Compared to the previous version the following changes have been introduced:
   * (Only the latest change is reflected.
   * The log of historical changes can be retrieved from the svn).
   * 
   * 4: new method added: getAccessKeys()
   *      
   */
  public static final long versionID = 4L;

  // Error codes passed by errorReport().
  final static int NOTIFY = 0;
  final static int FATAL = 1;

  // Journal action codes. See journal().
  public static byte JA_IS_ALIVE = 100; // check whether the journal is alive
  public static byte JA_JOURNAL      = 101; // just journal
  public static byte JA_JSPOOL_START = 102;  // = FSEditLog.OP_JSPOOL_START
  public static byte JA_CHECKPOINT_TIME = 103; // = FSEditLog.OP_CHECKPOINT_TIME

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
   * @return          a list of blocks & their locations
   * @throws RemoteException if size is less than or equal to 0 or
                                   datanode does not exist
   */
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
  throws IOException;

  /**
   * Get the current access keys
   * 
   * @return ExportedAccessKeys containing current access keys
   * @throws IOException 
   */
  public ExportedAccessKeys getAccessKeys() throws IOException;

  /**
   * Get the size of the current edit log (in bytes).
   * @return The number of bytes in the current edit log.
   * @throws IOException
   * @deprecated 
   *    See {@link org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode}
   */
  @Deprecated
  public long getEditLogSize() throws IOException;

  /**
   * Closes the current edit log and opens a new one. The 
   * call fails if the file system is in SafeMode.
   * @throws IOException
   * @return a unique token to identify this transaction.
   * @deprecated 
   *    See {@link org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode}
   */
  @Deprecated
  public CheckpointSignature rollEditLog() throws IOException;

  /**
   * Rolls the fsImage log. It removes the old fsImage, copies the
   * new image to fsImage, removes the old edits and renames edits.new 
   * to edits. The call fails if any of the four files are missing.
   * @throws IOException
   * @deprecated 
   *    See {@link org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode}
   */
  @Deprecated
  public void rollFsImage() throws IOException;

  /**
   * Request name-node version and storage information.
   * 
   * @return {@link NamespaceInfo} identifying versions and storage information 
   *          of the name-node
   * @throws IOException
   */
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
  public void errorReport(NamenodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException;

  /** 
   * Register a subordinate name-node like backup node.
   *
   * @return  {@link NamenodeRegistration} of the node,
   *          which this node has just registered with.
   */
  public NamenodeRegistration register(NamenodeRegistration registration)
  throws IOException;

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
  public void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException;

  /**
   * Get the size of the active name-node journal (edit log) in bytes.
   * 
   * @param registration the requesting node
   * @return The number of bytes in the journal.
   * @throws IOException
   */
  public long journalSize(NamenodeRegistration registration) throws IOException;

  /**
   * Journal edit records.
   * This message is sent by the active name-node to the backup node
   * via {@code EditLogBackupOutputStream} in order to synchronize meta-data
   * changes with the backup namespace image.
   * 
   * @param registration active node registration
   * @param jAction journal action
   * @param length length of the byte array
   * @param records byte array containing serialized journal records
   * @throws IOException
   */
  public void journal(NamenodeRegistration registration,
                      int jAction,
                      int length,
                      byte[] records) throws IOException;
}

