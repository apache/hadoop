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
package org.apache.hadoop.hdfs.protocolPB;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AbandonBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AppendRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AppendResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CompleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ConcatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSymlinkRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FinalizeUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FsyncRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetContentSummaryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLinkTargetRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLinkTargetResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MetaSaveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RefreshNodesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.Rename2RequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollEditsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollEditsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SaveNamespaceRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetOwnerRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetPermissionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetQuotaRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetReplicationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetSafeModeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetTimesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdatePipelineRequestProto;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

/**
 * This class forwards NN's ClientProtocol calls as RPC calls to the NN server
 * while translating from the parameter types used in ClientProtocol to the
 * new PB types.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ClientNamenodeProtocolTranslatorPB implements
    ProtocolMetaInterface, ClientProtocol, Closeable, ProtocolTranslator {
  final private ClientNamenodeProtocolPB rpcProxy;

  static final GetServerDefaultsRequestProto VOID_GET_SERVER_DEFAULT_REQUEST = 
  GetServerDefaultsRequestProto.newBuilder().build();

  private final static GetFsStatusRequestProto VOID_GET_FSSTATUS_REQUEST =
  GetFsStatusRequestProto.newBuilder().build();

  private final static SaveNamespaceRequestProto VOID_SAVE_NAMESPACE_REQUEST =
  SaveNamespaceRequestProto.newBuilder().build();

  private final static RollEditsRequestProto VOID_ROLLEDITS_REQUEST = 
  RollEditsRequestProto.getDefaultInstance();

  private final static RefreshNodesRequestProto VOID_REFRESH_NODES_REQUEST =
  RefreshNodesRequestProto.newBuilder().build();

  private final static FinalizeUpgradeRequestProto
  VOID_FINALIZE_UPGRADE_REQUEST =
      FinalizeUpgradeRequestProto.newBuilder().build();

  private final static GetDataEncryptionKeyRequestProto
  VOID_GET_DATA_ENCRYPTIONKEY_REQUEST =
      GetDataEncryptionKeyRequestProto.newBuilder().build();

  public ClientNamenodeProtocolTranslatorPB(ClientNamenodeProtocolPB proxy) {
    rpcProxy = proxy;
  }
  
  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    GetBlockLocationsRequestProto req = GetBlockLocationsRequestProto
        .newBuilder()
        .setSrc(src)
        .setOffset(offset)
        .setLength(length)
        .build();
    try {
      GetBlockLocationsResponseProto resp = rpcProxy.getBlockLocations(null,
          req);
      return resp.hasLocations() ? 
        PBHelper.convert(resp.getLocations()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    GetServerDefaultsRequestProto req = VOID_GET_SERVER_DEFAULT_REQUEST;
    try {
      return PBHelper
          .convert(rpcProxy.getServerDefaults(null, req).getServerDefaults());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication, long blockSize)
      throws AccessControlException, AlreadyBeingCreatedException,
      DSQuotaExceededException, FileAlreadyExistsException,
      FileNotFoundException, NSQuotaExceededException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException {
    CreateRequestProto req = CreateRequestProto.newBuilder()
        .setSrc(src)
        .setMasked(PBHelper.convert(masked))
        .setClientName(clientName)
        .setCreateFlag(PBHelper.convertCreateFlag(flag))
        .setCreateParent(createParent)
        .setReplication(replication)
        .setBlockSize(blockSize)
        .build();
    try {
      CreateResponseProto res = rpcProxy.create(null, req);
      return res.hasFs() ? PBHelper.convert(res.getFs()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

  }

  @Override
  public LocatedBlock append(String src, String clientName)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    AppendRequestProto req = AppendRequestProto.newBuilder()
        .setSrc(src)
        .setClientName(clientName)
        .build();
    try {
      AppendResponseProto res = rpcProxy.append(null, req);
      return res.hasBlock() ? PBHelper.convert(res.getBlock()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean setReplication(String src, short replication)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    SetReplicationRequestProto req = SetReplicationRequestProto.newBuilder()
        .setSrc(src)
        .setReplication(replication)
        .build();
    try {
      return rpcProxy.setReplication(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setPermission(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    SetPermissionRequestProto req = SetPermissionRequestProto.newBuilder()
        .setSrc(src)
        .setPermission(PBHelper.convert(permission))
        .build();
    try {
      rpcProxy.setPermission(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setOwner(String src, String username, String groupname)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    SetOwnerRequestProto.Builder req = SetOwnerRequestProto.newBuilder()
        .setSrc(src);
    if (username != null)
        req.setUsername(username);
    if (groupname != null)
        req.setGroupname(groupname);
    try {
      rpcProxy.setOwner(null, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void abandonBlock(ExtendedBlock b, String src, String holder)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    AbandonBlockRequestProto req = AbandonBlockRequestProto.newBuilder()
        .setB(PBHelper.convert(b)).setSrc(src).setHolder(holder).build();
    try {
      rpcProxy.abandonBlock(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
  
  @Override
  public LocatedBlock addBlock(String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId)
      throws AccessControlException, FileNotFoundException,
      NotReplicatedYetException, SafeModeException, UnresolvedLinkException,
      IOException {
    AddBlockRequestProto.Builder req = AddBlockRequestProto.newBuilder()
        .setSrc(src).setClientName(clientName).setFileId(fileId);
    if (previous != null) 
      req.setPrevious(PBHelper.convert(previous)); 
    if (excludeNodes != null) 
      req.addAllExcludeNodes(PBHelper.convert(excludeNodes));
    try {
      return PBHelper.convert(rpcProxy.addBlock(null, req.build()).getBlock());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public LocatedBlock getAdditionalDatanode(String src, ExtendedBlock blk,
      DatanodeInfo[] existings, DatanodeInfo[] excludes,
      int numAdditionalNodes, String clientName) throws AccessControlException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    GetAdditionalDatanodeRequestProto req = GetAdditionalDatanodeRequestProto
        .newBuilder()
        .setSrc(src)
        .setBlk(PBHelper.convert(blk))
        .addAllExistings(PBHelper.convert(existings))
        .addAllExcludes(PBHelper.convert(excludes))
        .setNumAdditionalNodes(numAdditionalNodes)
        .setClientName(clientName)
        .build();
    try {
      return PBHelper.convert(rpcProxy.getAdditionalDatanode(null, req)
          .getBlock());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean complete(String src, String clientName, ExtendedBlock last)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    CompleteRequestProto.Builder req = CompleteRequestProto.newBuilder()
        .setSrc(src)
        .setClientName(clientName);   
    if (last != null)
      req.setLast(PBHelper.convert(last));
    try {
      return rpcProxy.complete(null, req.build()).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    ReportBadBlocksRequestProto req = ReportBadBlocksRequestProto.newBuilder()
        .addAllBlocks(Arrays.asList(PBHelper.convertLocatedBlock(blocks)))
        .build();
    try {
      rpcProxy.reportBadBlocks(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean rename(String src, String dst) throws UnresolvedLinkException,
      IOException {
    RenameRequestProto req = RenameRequestProto.newBuilder()
        .setSrc(src)
        .setDst(dst).build();
    try {
      return rpcProxy.rename(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
  

  @Override
  public void rename2(String src, String dst, Rename... options)
      throws AccessControlException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    boolean overwrite = false;
    if (options != null) {
      for (Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    Rename2RequestProto req = Rename2RequestProto.newBuilder().
        setSrc(src).
        setDst(dst).setOverwriteDest(overwrite).
        build();
    try {
      rpcProxy.rename2(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

  }

  @Override
  public void concat(String trg, String[] srcs) throws IOException,
      UnresolvedLinkException {
    ConcatRequestProto req = ConcatRequestProto.newBuilder().
        setTrg(trg).
        addAllSrcs(Arrays.asList(srcs)).build();
    try {
      rpcProxy.concat(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }


  @Override
  public boolean delete(String src, boolean recursive)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    DeleteRequestProto req = DeleteRequestProto.newBuilder().setSrc(src).setRecursive(recursive).build();
    try {
      return rpcProxy.delete(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, NSQuotaExceededException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException {
    MkdirsRequestProto req = MkdirsRequestProto.newBuilder()
        .setSrc(src)
        .setMasked(PBHelper.convert(masked))
        .setCreateParent(createParent).build();

    try {
      return rpcProxy.mkdirs(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    GetListingRequestProto req = GetListingRequestProto.newBuilder()
        .setSrc(src)
        .setStartAfter(ByteString.copyFrom(startAfter))
        .setNeedLocation(needLocation).build();
    try {
      GetListingResponseProto result = rpcProxy.getListing(null, req);
      
      if (result.hasDirList()) {
        return PBHelper.convert(result.getDirList());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void renewLease(String clientName) throws AccessControlException,
      IOException {
    RenewLeaseRequestProto req = RenewLeaseRequestProto.newBuilder()
        .setClientName(clientName).build();
    try {
      rpcProxy.renewLease(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean recoverLease(String src, String clientName)
      throws IOException {
    RecoverLeaseRequestProto req = RecoverLeaseRequestProto.newBuilder()
        .setSrc(src)
        .setClientName(clientName).build();
    try {
      return rpcProxy.recoverLease(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }  
  }

  @Override
  public long[] getStats() throws IOException {
    try {
      return PBHelper.convert(rpcProxy.getFsStats(null,
          VOID_GET_FSSTATUS_REQUEST));
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    GetDatanodeReportRequestProto req = GetDatanodeReportRequestProto
        .newBuilder()
        .setType(PBHelper.convert(type)).build();
    try {
      return PBHelper.convert(
          rpcProxy.getDatanodeReport(null, req).getDiList());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public long getPreferredBlockSize(String filename) throws IOException,
      UnresolvedLinkException {
    GetPreferredBlockSizeRequestProto req = GetPreferredBlockSizeRequestProto
        .newBuilder()
        .setFilename(filename)
        .build();
    try {
      return rpcProxy.getPreferredBlockSize(null, req).getBsize();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean setSafeMode(SafeModeAction action, boolean isChecked) throws IOException {
    SetSafeModeRequestProto req = SetSafeModeRequestProto.newBuilder()
        .setAction(PBHelper.convert(action)).setChecked(isChecked).build();
    try {
      return rpcProxy.setSafeMode(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void saveNamespace() throws AccessControlException, IOException {
    try {
      rpcProxy.saveNamespace(null, VOID_SAVE_NAMESPACE_REQUEST);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
  
  @Override
  public long rollEdits() throws AccessControlException, IOException {
    try {
      RollEditsResponseProto resp = rpcProxy.rollEdits(null,
          VOID_ROLLEDITS_REQUEST);
      return resp.getNewSegmentTxId();
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public boolean restoreFailedStorage(String arg) 
      throws AccessControlException, IOException{
    RestoreFailedStorageRequestProto req = RestoreFailedStorageRequestProto
        .newBuilder()
        .setArg(arg).build();
    try {
      return rpcProxy.restoreFailedStorage(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void refreshNodes() throws IOException {
    try {
      rpcProxy.refreshNodes(null, VOID_REFRESH_NODES_REQUEST);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    try {
      rpcProxy.finalizeUpgrade(null, VOID_FINALIZE_UPGRADE_REQUEST);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    ListCorruptFileBlocksRequestProto.Builder req = 
        ListCorruptFileBlocksRequestProto.newBuilder().setPath(path);   
    if (cookie != null) 
      req.setCookie(cookie);
    try {
      return PBHelper.convert(
          rpcProxy.listCorruptFileBlocks(null, req.build()).getCorrupt());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void metaSave(String filename) throws IOException {
    MetaSaveRequestProto req = MetaSaveRequestProto.newBuilder()
        .setFilename(filename).build();
    try {
      rpcProxy.metaSave(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

  }

  @Override
  public HdfsFileStatus getFileInfo(String src) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    GetFileInfoRequestProto req = GetFileInfoRequestProto.newBuilder()
        .setSrc(src).build();
    try {
      GetFileInfoResponseProto res = rpcProxy.getFileInfo(null, req);
      return res.hasFs() ? PBHelper.convert(res.getFs()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public HdfsFileStatus getFileLinkInfo(String src)
      throws AccessControlException, UnresolvedLinkException, IOException {
    GetFileLinkInfoRequestProto req = GetFileLinkInfoRequestProto.newBuilder()
        .setSrc(src).build();
    try {
      GetFileLinkInfoResponseProto result = rpcProxy.getFileLinkInfo(null, req);
      return result.hasFs() ?  
          PBHelper.convert(rpcProxy.getFileLinkInfo(null, req).getFs()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public ContentSummary getContentSummary(String path)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    GetContentSummaryRequestProto req = GetContentSummaryRequestProto
        .newBuilder()
        .setPath(path)
        .build();
    try {
      return PBHelper.convert(rpcProxy.getContentSummary(null, req)
          .getSummary());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    SetQuotaRequestProto req = SetQuotaRequestProto.newBuilder()
        .setPath(path)
        .setNamespaceQuota(namespaceQuota)
        .setDiskspaceQuota(diskspaceQuota)
        .build();
    try {
      rpcProxy.setQuota(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void fsync(String src, String client, long lastBlockLength)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    FsyncRequestProto req = FsyncRequestProto.newBuilder().setSrc(src)
        .setClient(client).setLastBlockLength(lastBlockLength).build();
    try {
      rpcProxy.fsync(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setTimes(String src, long mtime, long atime)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    SetTimesRequestProto req = SetTimesRequestProto.newBuilder()
        .setSrc(src)
        .setMtime(mtime)
        .setAtime(atime)
        .build();
    try {
      rpcProxy.setTimes(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void createSymlink(String target, String link, FsPermission dirPerm,
      boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException {
    CreateSymlinkRequestProto req = CreateSymlinkRequestProto.newBuilder()
        .setTarget(target)
        .setLink(link)
        .setDirPerm(PBHelper.convert(dirPerm))
        .setCreateParent(createParent)
        .build();
    try {
      rpcProxy.createSymlink(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public String getLinkTarget(String path) throws AccessControlException,
      FileNotFoundException, IOException {
    GetLinkTargetRequestProto req = GetLinkTargetRequestProto.newBuilder()
        .setPath(path).build();
    try {
      GetLinkTargetResponseProto rsp = rpcProxy.getLinkTarget(null, req);
      return rsp.hasTargetPath() ? rsp.getTargetPath() : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block,
      String clientName) throws IOException {
    UpdateBlockForPipelineRequestProto req = UpdateBlockForPipelineRequestProto
        .newBuilder()
        .setBlock(PBHelper.convert(block))
        .setClientName(clientName)
        .build();
    try {
      return PBHelper.convert(
          rpcProxy.updateBlockForPipeline(null, req).getBlock());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes) throws IOException {
    UpdatePipelineRequestProto req = UpdatePipelineRequestProto.newBuilder()
        .setClientName(clientName)
        .setOldBlock(PBHelper.convert(oldBlock))
        .setNewBlock(PBHelper.convert(newBlock))
        .addAllNewNodes(Arrays.asList(PBHelper.convert(newNodes)))
        .build();
    try {
      rpcProxy.updatePipeline(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    GetDelegationTokenRequestProto req = GetDelegationTokenRequestProto
        .newBuilder()
        .setRenewer(renewer.toString())
        .build();
    try {
      GetDelegationTokenResponseProto resp = rpcProxy.getDelegationToken(null, req);
      return resp.hasToken() ? PBHelper.convertDelegationToken(resp.getToken())
          : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    RenewDelegationTokenRequestProto req = RenewDelegationTokenRequestProto.newBuilder().
        setToken(PBHelper.convert(token)).
        build();
    try {
      return rpcProxy.renewDelegationToken(null, req).getNewExpiryTime();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    CancelDelegationTokenRequestProto req = CancelDelegationTokenRequestProto
        .newBuilder()
        .setToken(PBHelper.convert(token))
        .build();
    try {
      rpcProxy.cancelDelegationToken(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    SetBalancerBandwidthRequestProto req = SetBalancerBandwidthRequestProto.newBuilder()
        .setBandwidth(bandwidth)
        .build();
    try {
      rpcProxy.setBalancerBandwidth(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        ClientNamenodeProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(ClientNamenodeProtocolPB.class), methodName);
  }
  
  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    try {
      GetDataEncryptionKeyResponseProto rsp = rpcProxy.getDataEncryptionKey(
          null, VOID_GET_DATA_ENCRYPTIONKEY_REQUEST);
     return rsp.hasDataEncryptionKey() ? 
          PBHelper.convert(rsp.getDataEncryptionKey()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }
  
}
