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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.UpgradeAction;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;

/**
 * This class is used on the server side. Calls come across the wire for the
 * protocol family of Release 23 onwards. This class translates the R23 data
 * types to the native data types used inside the NN as specified in the generic
 * ClientProtocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ClientNamenodeProtocolServerSideTranslatorR23 implements
    ClientNamenodeWireProtocol {
  final private ClientProtocol server;

  /**
   * Constructor
   * 
   * @param server - the NN server
   * @throws IOException
   */
  public ClientNamenodeProtocolServerSideTranslatorR23(ClientProtocol server)
      throws IOException {
    this.server = server;
  }

  /**
   * The client side will redirect getProtocolSignature to
   * getProtocolSignature2.
   * 
   * However the RPC layer below on the Server side will call getProtocolVersion
   * and possibly in the future getProtocolSignature. Hence we still implement
   * it even though the end client's call will never reach here.
   */
  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link ClientNamenodeProtocol}
     * 
     */
    if (!protocol.equals(RPC.getProtocolName(
        ClientNamenodeWireProtocol.class))) {
      throw new IOException("Namenode Serverside implements " +
          RPC.getProtocolName(ClientNamenodeWireProtocol.class) +
          ". The following requested protocol is unknown: " + protocol);
    }

    return ProtocolSignature.getProtocolSignature(clientMethodsHash,
        ClientNamenodeWireProtocol.versionID,
        ClientNamenodeWireProtocol.class);
  }

  @Override
  public ProtocolSignatureWritable 
          getProtocolSignature2(
      String protocol, long clientVersion, int clientMethodsHash)
      throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link ClientNamenodeProtocol}
     * 
     */

    return ProtocolSignatureWritable.convert(
        this.getProtocolSignature(protocol, clientVersion, clientMethodsHash));
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(RPC.getProtocolName(
        ClientNamenodeWireProtocol.class))) {
      return ClientNamenodeWireProtocol.versionID;
    }
    throw new IOException("Namenode Serverside implements " +
        RPC.getProtocolName(ClientNamenodeWireProtocol.class) +
        ". The following requested protocol is unknown: " + protocol);
  }

  @Override
  public LocatedBlocksWritable getBlockLocations(
      String src, long offset, long length)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    return LocatedBlocksWritable.convertLocatedBlocks(
        server.getBlockLocations(src, offset, length));
  }

  @Override
  public FsServerDefaultsWritable getServerDefaults() throws IOException {
    return FsServerDefaultsWritable.convert(server.getServerDefaults());
  }

  @Override
  public void create(String src, FsPermissionWritable masked, String clientName,
      EnumSetWritable<CreateFlag> flag, boolean createParent,
      short replication, long blockSize) throws AccessControlException,
      AlreadyBeingCreatedException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    server.create(src, FsPermissionWritable.convertPermission(masked),
        clientName, flag, createParent, replication, blockSize);

  }

  @Override
  public LocatedBlockWritable append(String src, String clientName)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    return LocatedBlockWritable.convertLocatedBlock(
        server.append(src, clientName));
  }

  @Override
  public boolean setReplication(String src, short replication)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    return server.setReplication(src, replication);
  }

  @Override
  public void setPermission(String src, FsPermissionWritable permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    server.setPermission(src, 
        FsPermissionWritable.convertPermission(permission));

  }

  @Override
  public void setOwner(String src, String username, String groupname)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    server.setOwner(src, username, groupname);

  }

  @Override
  public void abandonBlock(ExtendedBlockWritable b, String src, String holder)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    server.abandonBlock(
        ExtendedBlockWritable.convertExtendedBlock(b), src, holder);

  }

  @Override
  public LocatedBlockWritable addBlock(String src, String clientName,
      ExtendedBlockWritable previous, DatanodeInfoWritable[] excludeNodes)
      throws AccessControlException, FileNotFoundException,
      NotReplicatedYetException, SafeModeException, UnresolvedLinkException,
      IOException {
    return LocatedBlockWritable.convertLocatedBlock(
        server.addBlock(src, clientName,
        ExtendedBlockWritable.convertExtendedBlock(previous),
        DatanodeInfoWritable.convertDatanodeInfo(excludeNodes)));
  }

  @Override
  public LocatedBlockWritable getAdditionalDatanode(String src, ExtendedBlockWritable blk,
      DatanodeInfoWritable[] existings, DatanodeInfoWritable[] excludes,
      int numAdditionalNodes, String clientName) throws AccessControlException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    return LocatedBlockWritable.convertLocatedBlock(
        server.getAdditionalDatanode(src,
              ExtendedBlockWritable.convertExtendedBlock(blk),
              DatanodeInfoWritable.convertDatanodeInfo(existings),
              DatanodeInfoWritable.convertDatanodeInfo(excludes),
              numAdditionalNodes, clientName));
  }

  @Override
  public boolean complete(String src, String clientName, ExtendedBlockWritable last)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    return server.complete(src, clientName,
        ExtendedBlockWritable.convertExtendedBlock(last));
  }

  @Override
  public void reportBadBlocks(LocatedBlockWritable[] blocks) throws IOException {
    server.reportBadBlocks(LocatedBlockWritable.convertLocatedBlock(blocks));

  }

  @Override
  public boolean rename(String src, String dst) throws UnresolvedLinkException,
      IOException {
    return server.rename(src, dst);
  }

  @Override
  public void concat(String trg, String[] srcs) throws IOException,
      UnresolvedLinkException {
    server.concat(trg, srcs);

  }

  @Override
  public void rename2(String src, String dst, Rename... options)
      throws AccessControlException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    server.rename2(src, dst, options);
  }

  @Override
  public boolean delete(String src, boolean recursive)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    return server.delete(src, recursive);
  }

  @Override
  public boolean mkdirs(String src, FsPermissionWritable masked, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, NSQuotaExceededException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException {

    return server.mkdirs(src, FsPermissionWritable.convertPermission(masked),
        createParent);
  }

  @Override
  public DirectoryListingWritable getListing(String src, byte[] startAfter,
      boolean needLocation) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    return DirectoryListingWritable.convertDirectoryListing(
        server.getListing(src, startAfter, needLocation));
  }

  @Override
  public void renewLease(String clientName) throws AccessControlException,
      IOException {
    server.renewLease(clientName);

  }

  @Override
  public boolean recoverLease(String src, String clientName) throws IOException {
    return server.recoverLease(src, clientName);
  }

  @Override
  public long[] getStats() throws IOException {
    return server.getStats();
  }

  @Override
  public DatanodeInfoWritable[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    return DatanodeInfoWritable
        .convertDatanodeInfo(server.getDatanodeReport(type));
  }

  @Override
  public long getPreferredBlockSize(String filename) throws IOException,
      UnresolvedLinkException {
    return server.getPreferredBlockSize(filename);
  }

  @Override
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return server.setSafeMode(action);
  }

  @Override
  public void saveNamespace() throws AccessControlException, IOException {
    server.saveNamespace();

  }

  @Override
  public boolean restoreFailedStorage(String arg) throws AccessControlException {
    return server.restoreFailedStorage(arg);
  }

  @Override
  public void refreshNodes() throws IOException {
    server.refreshNodes();

  }

  @Override
  public void finalizeUpgrade() throws IOException {
    server.finalizeUpgrade();

  }

  @Override
  public UpgradeStatusReportWritable distributedUpgradeProgress(UpgradeAction action)
      throws IOException {
    return UpgradeStatusReportWritable.convert(
        server.distributedUpgradeProgress(action));
  }

  @Override
  public CorruptFileBlocksWritable listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    return CorruptFileBlocksWritable.convertCorruptFilesBlocks(
        server.listCorruptFileBlocks(path, cookie));
  }

  @Override
  public void metaSave(String filename) throws IOException {
    server.metaSave(filename);

  }

  @Override
  public HdfsFileStatusWritable getFileInfo(String src) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    return HdfsFileStatusWritable.convertHdfsFileStatus(
        server.getFileInfo(src));
  }

  @Override
  public HdfsFileStatusWritable getFileLinkInfo(String src)
      throws AccessControlException, UnresolvedLinkException, IOException {
    return HdfsFileStatusWritable.convertHdfsFileStatus(
        server.getFileLinkInfo(src));
  }

  @Override
  public ContentSummaryWritable getContentSummary(String path)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    return ContentSummaryWritable.convert(server.getContentSummary(path));
  }

  @Override
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    server.setQuota(path, namespaceQuota, diskspaceQuota);

  }

  @Override
  public void fsync(String src, String client) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    server.fsync(src, client);

  }

  @Override
  public void setTimes(String src, long mtime, long atime)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    server.setTimes(src, mtime, atime);

  }

  @Override
  public void createSymlink(String target, String link, FsPermissionWritable dirPerm,
      boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException {
    server.createSymlink(target, link, FsPermissionWritable.convertPermission(dirPerm),
        createParent);

  }

  @Override
  public String getLinkTarget(String path) throws AccessControlException,
      FileNotFoundException, IOException {
    return server.getLinkTarget(path);
  }

  @Override
  public LocatedBlockWritable updateBlockForPipeline(ExtendedBlockWritable block,
      String clientName) throws IOException {
    return LocatedBlockWritable.convertLocatedBlock(
        server.updateBlockForPipeline(
        ExtendedBlockWritable.convertExtendedBlock(block), clientName));
  }

  @Override
  public void updatePipeline(String clientName, ExtendedBlockWritable oldBlock,
      ExtendedBlockWritable newBlock, DatanodeIDWritable[] newNodes)
    throws IOException {
    server.updatePipeline(clientName, 
              ExtendedBlockWritable.convertExtendedBlock(oldBlock), 
              ExtendedBlockWritable.convertExtendedBlock(newBlock),
              DatanodeIDWritable.convertDatanodeID(newNodes));
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return server.getDelegationToken(renewer);
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    return server.renewDelegationToken(token);
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    server.cancelDelegationToken(token);
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    server.setBalancerBandwidth(bandwidth);
  }
}
