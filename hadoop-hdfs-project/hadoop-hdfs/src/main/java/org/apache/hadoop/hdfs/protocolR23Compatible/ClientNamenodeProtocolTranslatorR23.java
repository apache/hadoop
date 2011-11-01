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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.UpgradeAction;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

/**
 * This class forwards NN's ClientProtocol calls as RPC calls to the NN server
 * while translating from the parameter types used in ClientProtocol to those
 * used in protocolR23Compatile.*.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ClientNamenodeProtocolTranslatorR23 implements
    ClientProtocol, Closeable {
  final private ClientNamenodeWireProtocol rpcProxy;

  private static ClientNamenodeWireProtocol createNamenode(
      InetSocketAddress nameNodeAddr, Configuration conf,
      UserGroupInformation ugi) throws IOException {
    return RPC.getProxy(ClientNamenodeWireProtocol.class,
        ClientNamenodeWireProtocol.versionID, nameNodeAddr, ugi, conf,
        NetUtils.getSocketFactory(conf, ClientNamenodeWireProtocol.class));
  }

  /** Create a {@link NameNode} proxy */
  static ClientNamenodeWireProtocol createNamenodeWithRetry(
      ClientNamenodeWireProtocol rpcNamenode) {
    RetryPolicy createPolicy = RetryPolicies
        .retryUpToMaximumCountWithFixedSleep(5,
            HdfsConstants.LEASE_SOFTLIMIT_PERIOD, TimeUnit.MILLISECONDS);

    Map<Class<? extends Exception>, RetryPolicy> remoteExceptionToPolicyMap = new HashMap<Class<? extends Exception>, RetryPolicy>();
    remoteExceptionToPolicyMap.put(AlreadyBeingCreatedException.class,
        createPolicy);

    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = 
        new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(RemoteException.class, RetryPolicies
        .retryByRemoteException(RetryPolicies.TRY_ONCE_THEN_FAIL,
            remoteExceptionToPolicyMap));
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<String, RetryPolicy>();

    methodNameToPolicyMap.put("create", methodPolicy);

    return (ClientNamenodeWireProtocol) RetryProxy.create(
        ClientNamenodeWireProtocol.class, rpcNamenode, methodNameToPolicyMap);
  }

  public ClientNamenodeProtocolTranslatorR23(InetSocketAddress nameNodeAddr,
      Configuration conf, UserGroupInformation ugi) throws IOException {
    rpcProxy = createNamenodeWithRetry(createNamenode(nameNodeAddr, conf, ugi));
  }

  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocolName,
      long clientVersion, int clientMethodHash)
      throws IOException {
    return ProtocolSignatureWritable.convert(rpcProxy.getProtocolSignature2(
        protocolName, clientVersion, clientMethodHash));
  }

  @Override
  public long getProtocolVersion(String protocolName, long clientVersion) throws IOException {
    return rpcProxy.getProtocolVersion(protocolName, clientVersion);
  }

  @Override
  public LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    return LocatedBlocksWritable
        .convertLocatedBlocks(rpcProxy.getBlockLocations(src, offset, length));
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return FsServerDefaultsWritable
        .convert(rpcProxy.getServerDefaults());
  }

  @Override
  public void create(String src, FsPermission masked, String clientName,
      EnumSetWritable<CreateFlag> flag, boolean createParent,
      short replication, long blockSize) throws AccessControlException,
      AlreadyBeingCreatedException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    rpcProxy.create(src, FsPermissionWritable.convertPermission(masked),
        clientName, flag, createParent, replication, blockSize);

  }

  @Override
  public LocatedBlock append(String src, String clientName)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    return LocatedBlockWritable
        .convertLocatedBlock(rpcProxy.append(src, clientName));
  }

  @Override
  public boolean setReplication(String src, short replication)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    return rpcProxy.setReplication(src, replication);
  }

  @Override
  public void setPermission(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    rpcProxy.setPermission(src,
        FsPermissionWritable.convertPermission(permission));

  }

  @Override
  public void setOwner(String src, String username, String groupname)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    rpcProxy.setOwner(src, username, groupname);

  }

  @Override
  public void abandonBlock(ExtendedBlock b, String src, String holder)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    rpcProxy.abandonBlock(
        ExtendedBlockWritable.convertExtendedBlock(b), src, holder);

  }

  @Override
  public LocatedBlock addBlock(String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludeNodes)
      throws AccessControlException, FileNotFoundException,
      NotReplicatedYetException, SafeModeException, UnresolvedLinkException,
      IOException {
    return LocatedBlockWritable
        .convertLocatedBlock(rpcProxy.addBlock(src, clientName,
            ExtendedBlockWritable.convertExtendedBlock(previous),
            DatanodeInfoWritable.convertDatanodeInfo(excludeNodes)));
  }

  @Override
  public LocatedBlock getAdditionalDatanode(String src, ExtendedBlock blk,
      DatanodeInfo[] existings, DatanodeInfo[] excludes,
      int numAdditionalNodes, String clientName) throws AccessControlException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    return LocatedBlockWritable
        .convertLocatedBlock(rpcProxy.getAdditionalDatanode(src,
            ExtendedBlockWritable.convertExtendedBlock(blk),
            DatanodeInfoWritable.convertDatanodeInfo(existings),
            DatanodeInfoWritable.convertDatanodeInfo(excludes),
            numAdditionalNodes, clientName));
  }

  @Override
  public boolean complete(String src, String clientName, ExtendedBlock last)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    return rpcProxy.complete(src, clientName,
        ExtendedBlockWritable.convertExtendedBlock(last));
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    rpcProxy.reportBadBlocks(LocatedBlockWritable.convertLocatedBlock(blocks));

  }

  @Override
  public boolean rename(String src, String dst) throws UnresolvedLinkException,
      IOException {
    return rpcProxy.rename(src, dst);
  }

  @Override
  public void concat(String trg, String[] srcs) throws IOException,
      UnresolvedLinkException {
    rpcProxy.concat(trg, srcs);

  }

  @Override
  public void rename2(String src, String dst, Rename... options)
      throws AccessControlException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    rpcProxy.rename2(src, dst, options);

  }

  @Override
  public boolean delete(String src, boolean recursive)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    return rpcProxy.delete(src, recursive);
  }

  @Override
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, NSQuotaExceededException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException {

    return rpcProxy.mkdirs(src,
        FsPermissionWritable.convertPermission(masked), createParent);
  }

  @Override
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    return DirectoryListingWritable.convertDirectoryListing(
        rpcProxy.getListing(src, startAfter, needLocation));
  }

  @Override
  public void renewLease(String clientName) throws AccessControlException,
      IOException {
    rpcProxy.renewLease(clientName);

  }

  @Override
  public boolean recoverLease(String src, String clientName) throws IOException {
    return rpcProxy.recoverLease(src, clientName);
  }

  @Override
  public long[] getStats() throws IOException {
    return rpcProxy.getStats();
  }

  @Override
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    return DatanodeInfoWritable.convertDatanodeInfo(
        rpcProxy.getDatanodeReport(type));
  }

  @Override
  public long getPreferredBlockSize(String filename) throws IOException,
      UnresolvedLinkException {
    return rpcProxy.getPreferredBlockSize(filename);
  }

  @Override
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return rpcProxy.setSafeMode(action);
  }

  @Override
  public void saveNamespace() throws AccessControlException, IOException {
    rpcProxy.saveNamespace();

  }

  @Override
  public boolean restoreFailedStorage(String arg) throws AccessControlException {
    return rpcProxy.restoreFailedStorage(arg);
  }

  @Override
  public void refreshNodes() throws IOException {
    rpcProxy.refreshNodes();

  }

  @Override
  public void finalizeUpgrade() throws IOException {
    rpcProxy.finalizeUpgrade();

  }

  @Override
  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
      throws IOException {
    return UpgradeStatusReportWritable.convert(
        rpcProxy.distributedUpgradeProgress(action));
  }

  @Override
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    return CorruptFileBlocksWritable.convertCorruptFileBlocks(
        rpcProxy.listCorruptFileBlocks(path, cookie));
  }

  @Override
  public void metaSave(String filename) throws IOException {
    rpcProxy.metaSave(filename);

  }

  @Override
  public HdfsFileStatus getFileInfo(String src) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    return HdfsFileStatusWritable.convertHdfsFileStatus(
        rpcProxy.getFileInfo(src));
  }

  @Override
  public HdfsFileStatus getFileLinkInfo(String src)
      throws AccessControlException, UnresolvedLinkException, IOException {
    return HdfsFileStatusWritable
        .convertHdfsFileStatus(rpcProxy.getFileLinkInfo(src));
  }

  @Override
  public ContentSummary getContentSummary(String path)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    return ContentSummaryWritable
        .convert(rpcProxy.getContentSummary(path));
  }

  @Override
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    rpcProxy.setQuota(path, namespaceQuota, diskspaceQuota);

  }

  @Override
  public void fsync(String src, String client) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    rpcProxy.fsync(src, client);

  }

  @Override
  public void setTimes(String src, long mtime, long atime)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    rpcProxy.setTimes(src, mtime, atime);

  }

  @Override
  public void createSymlink(String target, String link, FsPermission dirPerm,
      boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException {
    rpcProxy.createSymlink(target, link,
        FsPermissionWritable.convertPermission(dirPerm), createParent);

  }

  @Override
  public String getLinkTarget(String path) throws AccessControlException,
      FileNotFoundException, IOException {
    return rpcProxy.getLinkTarget(path);
  }

  @Override
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block,
      String clientName) throws IOException {
    return LocatedBlockWritable.convertLocatedBlock(
        rpcProxy.updateBlockForPipeline(
            ExtendedBlockWritable.convertExtendedBlock(block), clientName));
  }

  @Override
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes) throws IOException {
    rpcProxy.updatePipeline(clientName,
        ExtendedBlockWritable.convertExtendedBlock(oldBlock),
        ExtendedBlockWritable.convertExtendedBlock(newBlock),
        DatanodeIDWritable.convertDatanodeID(newNodes));

  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return rpcProxy.getDelegationToken(renewer);
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    return rpcProxy.renewDelegationToken(token);
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    rpcProxy.cancelDelegationToken(token);
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    rpcProxy.setBalancerBandwidth(bandwidth);
  }
}
