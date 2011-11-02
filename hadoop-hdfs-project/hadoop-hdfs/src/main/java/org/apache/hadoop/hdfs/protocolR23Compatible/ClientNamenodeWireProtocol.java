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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.avro.reflect.Nullable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.ipc.ProtocolInfo;

import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;

/**********************************************************************
 * This class defines the actual protocol used to communicate with the
 * NN via RPC using writable types.
 * The parameters in the methods which are specified in the
 * package are separate from those used internally in the NN and DFSClient
 * and hence need to be converted using {@link ClientNamenodeProtocolTranslatorR23}
 * and {@link ClientNamenodeProtocolServerSideTranslatorR23}.
 *
 **********************************************************************/
@InterfaceAudience.Private
@InterfaceStability.Stable
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY)
@TokenInfo(DelegationTokenSelector.class)
@ProtocolInfo(protocolName = HdfsConstants.CLIENT_NAMENODE_PROTOCOL_NAME)
public interface ClientNamenodeWireProtocol extends VersionedProtocol {

  /**
   * Changes to the protocol:
   * 
   * Do NOT change a method's signature (ie name, parameters, parameter types
   * or exceptions thrown). If you need to make changes then ADD new methods and
   * new data types.
   * Hence if you maintain compatibility you will NOT have to change
   * the version number below. The version number is changed ONLY
   * if you break compatibility (which is a big deal).
   * Hence the version number is really a Major Version Number.
   *
   * The log of historical changes prior to 69 can be retrieved from the svn.
   * ALL changes since version 69L are recorded.
   * Version number is changed ONLY for Incompatible changes.
   *  (note previously we used to change version number for both
   *  compatible and incompatible changes).
   * 69: Eliminate overloaded method names. (Compatible)
   * 70: Separation of Datatypes - the client namenode protocol is implemented
   *     in this class instead of in 
   *           {@link org.apache.hadoop.hdfs.protocol.ClientProtocol}
   *     as was done prior to version 70.
   */
  public static final long versionID = 70L;
  
  ///////////////////////////////////////
  // File contents
  ///////////////////////////////////////
  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#getBlockLocations}
   */
  @Nullable
  public LocatedBlocksWritable getBlockLocations(String src,
                                         long offset,
                                         long length) 
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#getServerDefaults()}
   */
  public FsServerDefaultsWritable getServerDefaults() throws IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#create(String, 
   * org.apache.hadoop.fs.permission.FsPermission, String, 
   * EnumSetWritable, boolean, short, long)}
   */
  public void create(String src, FsPermissionWritable masked, String clientName,
      EnumSetWritable<CreateFlag> flag, boolean createParent,
      short replication, long blockSize) throws AccessControlException,
      AlreadyBeingCreatedException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#append(String, String)}
   */
  public LocatedBlockWritable append(String src, String clientName)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#setReplication(String, short)}
   */
  public boolean setReplication(String src, short replication)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#setPermission(String,
   * org.apache.hadoop.fs.permission.FsPermission)}
   */
  public void setPermission(String src, FsPermissionWritable permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#setOwner(String, String, String)}
   */
  public void setOwner(String src, String username, String groupname)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#abandonBlock(
   * org.apache.hadoop.hdfs.protocol.ExtendedBlock, String, String)}
   */
  public void abandonBlock(ExtendedBlockWritable b, String src, String holder)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#addBlock(String, 
   * String, org.apache.hadoop.hdfs.protocol.ExtendedBlock, 
   * org.apache.hadoop.hdfs.protocol.DatanodeInfo[])}
   */
  public LocatedBlockWritable addBlock(String src, String clientName,
      @Nullable ExtendedBlockWritable previous, @Nullable DatanodeInfoWritable[] excludeNodes)
      throws AccessControlException, FileNotFoundException,
      NotReplicatedYetException, SafeModeException, UnresolvedLinkException,
      IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#getAdditionalDatanode}
   */
  public LocatedBlockWritable getAdditionalDatanode(
      final String src, final ExtendedBlockWritable blk,
      final DatanodeInfoWritable[] existings,
      final DatanodeInfoWritable[] excludes,
      final int numAdditionalNodes, final String clientName
      ) throws AccessControlException, FileNotFoundException,
          SafeModeException, UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#complete}
   */
  public boolean complete(
      String src, String clientName, ExtendedBlockWritable last)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#reportBadBlocks}
   */
  public void reportBadBlocks(LocatedBlockWritable[] blocks) throws IOException;

  ///////////////////////////////////////
  // Namespace management
  ///////////////////////////////////////
  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#rename(String, String)}
   */
  public boolean rename(String src, String dst) 
      throws UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#concat(String, String[])}
   */
  public void concat(String trg, String[] srcs) 
      throws IOException, UnresolvedLinkException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#rename2}
   */
  public void rename2(String src, String dst, Options.Rename... options)
      throws AccessControlException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#delete(String, boolean)}
   */
  public boolean delete(String src, boolean recursive)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException;
  
  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#mkdirs}
   */
  public boolean mkdirs(
      String src, FsPermissionWritable masked, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, NSQuotaExceededException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#getListing}
   */
  public DirectoryListingWritable getListing(String src,
                                     byte[] startAfter,
                                     boolean needLocation)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  ///////////////////////////////////////
  // System issues and management
  ///////////////////////////////////////

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#renewLease(String)}
   */
  public void renewLease(String clientName) throws AccessControlException,
      IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#recoverLease(String, String)}
   */
  public boolean recoverLease(String src, String clientName) throws IOException;

  public int GET_STATS_CAPACITY_IDX = 0;
  public int GET_STATS_USED_IDX = 1;
  public int GET_STATS_REMAINING_IDX = 2;
  public int GET_STATS_UNDER_REPLICATED_IDX = 3;
  public int GET_STATS_CORRUPT_BLOCKS_IDX = 4;
  public int GET_STATS_MISSING_BLOCKS_IDX = 5;
  
  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#getStats()}
   */
  public long[] getStats() throws IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#getDatanodeReport}
   */
  public DatanodeInfoWritable[] getDatanodeReport(
      HdfsConstants.DatanodeReportType type)
      throws IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#getPreferredBlockSize}
   */
  public long getPreferredBlockSize(String filename) 
      throws IOException, UnresolvedLinkException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction)}
   */
  public boolean setSafeMode(HdfsConstants.SafeModeAction action) 
      throws IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#saveNamespace()}
   */
  public void saveNamespace() throws AccessControlException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#restoreFailedStorage(String)}
   */
  public boolean restoreFailedStorage(String arg) throws AccessControlException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#refreshNodes()}
   */
  public void refreshNodes() throws IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#finalizeUpgrade()}
   */
  public void finalizeUpgrade() throws IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#distributedUpgradeProgress}
   */
  @Nullable
  public UpgradeStatusReportWritable distributedUpgradeProgress(
      UpgradeAction action) 
      throws IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#listCorruptFileBlocks(String, String)}
   */
  public CorruptFileBlocksWritable
    listCorruptFileBlocks(String path, String cookie)
    throws IOException;
  
  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#metaSave(String)}
   */
  public void metaSave(String filename) throws IOException;
  
  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#setBalancerBandwidth(long)}
   */
  public void setBalancerBandwidth(long bandwidth) throws IOException;
  
  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#getFileInfo(String)}
   */
  @Nullable
  public HdfsFileStatusWritable getFileInfo(String src)
      throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#getFileLinkInfo(String)}
   */
  public HdfsFileStatusWritable getFileLinkInfo(String src)
      throws AccessControlException, UnresolvedLinkException, IOException;
  
  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#getContentSummary(String)}
   */
  public ContentSummaryWritable getContentSummary(String path)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String, long, long)}
   */
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#fsync(String, String)}
   */
  public void fsync(String src, String client) 
      throws AccessControlException, FileNotFoundException, 
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#setTimes(String, long, long)}
   */
  public void setTimes(String src, long mtime, long atime)
      throws AccessControlException, FileNotFoundException, 
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#createSymlink}
   */
  public void createSymlink(
      String target, String link, FsPermissionWritable dirPerm,
      boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#getLinkTarget(String)}
   */
  public String getLinkTarget(String path) throws AccessControlException,
      FileNotFoundException, IOException; 
  
  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#updateBlockForPipeline}
   */
  public LocatedBlockWritable updateBlockForPipeline(
      ExtendedBlockWritable block, String clientName) throws IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#updatePipeline}
   */
  public void updatePipeline(String clientName, ExtendedBlockWritable oldBlock, 
      ExtendedBlockWritable newBlock, DatanodeIDWritable[] newNodes)
      throws IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#getDelegationToken(Text)}
   */
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) 
      throws IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#renewDelegationToken(Token)}
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException;
  
  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#cancelDelegationToken(Token)}
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
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
