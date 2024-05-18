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

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.protocol.BatchedDirectoryListing;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsPartialListing;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.proto.SecurityProtos;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.ProtocolStringList;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncRouterServer;

public class RouterClientNamenodeProtocolServerSideTranslatorPB
    extends ClientNamenodeProtocolServerSideTranslatorPB{
  private final RouterRpcServer server;
  public RouterClientNamenodeProtocolServerSideTranslatorPB(
      ClientProtocol server) throws IOException {
    super(server);
    this.server = (RouterRpcServer) server;
  }


  @Override
  public ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto getBlockLocations(
      RpcController controller, ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto req) {
    asyncRouterServer(() -> server.getBlockLocations(req.getSrc(), req.getOffset(),
        req.getLength()),
        b -> {
          ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto.Builder builder
              = ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto
              .newBuilder();
          if (b != null) {
            builder.setLocations(PBHelperClient.convert(b)).build();
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto getServerDefaults(
      RpcController controller, ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto req) {
    asyncRouterServer(server::getServerDefaults,
        result -> ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto.newBuilder()
            .setServerDefaults(PBHelperClient.convert(result))
            .build());
    return null;
  }


  @Override
  public ClientNamenodeProtocolProtos.CreateResponseProto create(
      RpcController controller,
      ClientNamenodeProtocolProtos.CreateRequestProto req) {
    asyncRouterServer(() -> {
      FsPermission masked = req.hasUnmasked() ?
          FsCreateModes.create(PBHelperClient.convert(req.getMasked()),
              PBHelperClient.convert(req.getUnmasked())) :
          PBHelperClient.convert(req.getMasked());
      return server.create(req.getSrc(),
          masked, req.getClientName(),
          PBHelperClient.convertCreateFlag(req.getCreateFlag()), req.getCreateParent(),
          (short) req.getReplication(), req.getBlockSize(),
          PBHelperClient.convertCryptoProtocolVersions(
              req.getCryptoProtocolVersionList()),
          req.getEcPolicyName(), req.getStoragePolicy());
    }, result -> {
      if (result != null) {
        return ClientNamenodeProtocolProtos
            .CreateResponseProto.newBuilder().setFs(PBHelperClient.convert(result))
            .build();
      }
      return VOID_CREATE_RESPONSE;
    });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.AppendResponseProto append(
      RpcController controller,
      ClientNamenodeProtocolProtos.AppendRequestProto req) {
    asyncRouterServer(() -> {
      EnumSetWritable<CreateFlag> flags = req.hasFlag() ?
          PBHelperClient.convertCreateFlag(req.getFlag()) :
          new EnumSetWritable<>(EnumSet.of(CreateFlag.APPEND));
      return server.append(req.getSrc(),
          req.getClientName(), flags);
    }, result -> {
      ClientNamenodeProtocolProtos.AppendResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.AppendResponseProto.newBuilder();
      if (result.getLastBlock() != null) {
        builder.setBlock(PBHelperClient.convertLocatedBlock(
            result.getLastBlock()));
      }
      if (result.getFileStatus() != null) {
        builder.setStat(PBHelperClient.convert(result.getFileStatus()));
      }
      return builder.build();
    });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.SetReplicationResponseProto setReplication(
      RpcController controller,
      ClientNamenodeProtocolProtos.SetReplicationRequestProto req) {
    asyncRouterServer(() ->
        server.setReplication(req.getSrc(), (short) req.getReplication()),
        result -> ClientNamenodeProtocolProtos
        .SetReplicationResponseProto.newBuilder().setResult(result).build());
    return null;
  }


  @Override
  public ClientNamenodeProtocolProtos.SetPermissionResponseProto setPermission(
      RpcController controller,
      ClientNamenodeProtocolProtos.SetPermissionRequestProto req) {
    asyncRouterServer(() -> {
      server.setPermission(req.getSrc(), PBHelperClient.convert(req.getPermission()));
      return null;
    }, result -> VOID_SET_PERM_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.SetOwnerResponseProto setOwner(
      RpcController controller,
      ClientNamenodeProtocolProtos.SetOwnerRequestProto req) {
    asyncRouterServer(() -> {
      server.setOwner(req.getSrc(),
          req.hasUsername() ? req.getUsername() : null,
          req.hasGroupname() ? req.getGroupname() : null);
      return null;
    }, result -> VOID_SET_OWNER_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.AbandonBlockResponseProto abandonBlock(
      RpcController controller,
      ClientNamenodeProtocolProtos.AbandonBlockRequestProto req) {
    asyncRouterServer(() -> {
      server.abandonBlock(PBHelperClient.convert(req.getB()), req.getFileId(),
          req.getSrc(), req.getHolder());
      return null;
    }, result -> VOID_ADD_BLOCK_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.AddBlockResponseProto addBlock(
      RpcController controller,
      ClientNamenodeProtocolProtos.AddBlockRequestProto req) {
    asyncRouterServer(() -> {
      List<HdfsProtos.DatanodeInfoProto> excl = req.getExcludeNodesList();
      List<String> favor = req.getFavoredNodesList();
      EnumSet<AddBlockFlag> flags =
          PBHelperClient.convertAddBlockFlags(req.getFlagsList());
      return server.addBlock(
          req.getSrc(),
          req.getClientName(),
          req.hasPrevious() ? PBHelperClient.convert(req.getPrevious()) : null,
          (excl == null || excl.size() == 0) ? null : PBHelperClient.convert(excl
              .toArray(new HdfsProtos.DatanodeInfoProto[excl.size()])), req.getFileId(),
          (favor == null || favor.size() == 0) ? null : favor
              .toArray(new String[favor.size()]),
          flags);
    }, result -> ClientNamenodeProtocolProtos.AddBlockResponseProto.newBuilder()
        .setBlock(PBHelperClient.convertLocatedBlock(result)).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetAdditionalDatanodeResponseProto getAdditionalDatanode(
      RpcController controller, ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto req) {
    asyncRouterServer(() -> {
      List<HdfsProtos.DatanodeInfoProto> existingList = req.getExistingsList();
      List<String> existingStorageIDsList = req.getExistingStorageUuidsList();
      List<HdfsProtos.DatanodeInfoProto> excludesList = req.getExcludesList();
      LocatedBlock result = server.getAdditionalDatanode(req.getSrc(),
          req.getFileId(), PBHelperClient.convert(req.getBlk()),
          PBHelperClient.convert(existingList.toArray(
              new HdfsProtos.DatanodeInfoProto[existingList.size()])),
          existingStorageIDsList.toArray(
              new String[existingStorageIDsList.size()]),
          PBHelperClient.convert(excludesList.toArray(
              new HdfsProtos.DatanodeInfoProto[excludesList.size()])),
          req.getNumAdditionalNodes(), req.getClientName());
      return result;
    }, result -> ClientNamenodeProtocolProtos
        .GetAdditionalDatanodeResponseProto.newBuilder()
        .setBlock(
            PBHelperClient.convertLocatedBlock(result))
        .build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.CompleteResponseProto complete(
      RpcController controller,
      ClientNamenodeProtocolProtos.CompleteRequestProto req) {
    asyncRouterServer(() -> {
      boolean result =
          server.complete(req.getSrc(), req.getClientName(),
              req.hasLast() ? PBHelperClient.convert(req.getLast()) : null,
              req.hasFileId() ? req.getFileId() : HdfsConstants.GRANDFATHER_INODE_ID);
      return result;
    }, result -> ClientNamenodeProtocolProtos
        .CompleteResponseProto.newBuilder().setResult(result).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.ReportBadBlocksResponseProto reportBadBlocks(
      RpcController controller,
      ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto req) {
    asyncRouterServer(() -> {
      List<HdfsProtos.LocatedBlockProto> bl = req.getBlocksList();
      server.reportBadBlocks(PBHelperClient.convertLocatedBlocks(
          bl.toArray(new HdfsProtos.LocatedBlockProto[bl.size()])));
      return null;
    }, result -> VOID_REP_BAD_BLOCK_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.ConcatResponseProto concat(
      RpcController controller,
      ClientNamenodeProtocolProtos.ConcatRequestProto req) {
    asyncRouterServer(() -> {
      List<String> srcs = req.getSrcsList();
      server.concat(req.getTrg(), srcs.toArray(new String[srcs.size()]));
      return null;
    }, result -> VOID_CONCAT_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.RenameResponseProto rename(
      RpcController controller,
      ClientNamenodeProtocolProtos.RenameRequestProto req) {
    asyncRouterServer(() -> {
      return server.rename(req.getSrc(), req.getDst());
    }, result -> ClientNamenodeProtocolProtos
        .RenameResponseProto.newBuilder().setResult(result).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.Rename2ResponseProto rename2(
      RpcController controller,
      ClientNamenodeProtocolProtos.Rename2RequestProto req) {
    asyncRouterServer(() -> {
      // resolve rename options
      ArrayList<Options.Rename> optionList = new ArrayList<Options.Rename>();
      if (req.getOverwriteDest()) {
        optionList.add(Options.Rename.OVERWRITE);
      }
      if (req.hasMoveToTrash() && req.getMoveToTrash()) {
        optionList.add(Options.Rename.TO_TRASH);
      }

      if (optionList.isEmpty()) {
        optionList.add(Options.Rename.NONE);
      }
      server.rename2(req.getSrc(), req.getDst(),
          optionList.toArray(new Options.Rename[optionList.size()]));
      return null;
    }, result -> VOID_RENAME2_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.TruncateResponseProto truncate(
      RpcController controller,
      ClientNamenodeProtocolProtos.TruncateRequestProto req) {
    asyncRouterServer(() -> server.truncate(req.getSrc(), req.getNewLength(),
        req.getClientName()), result -> ClientNamenodeProtocolProtos
            .TruncateResponseProto.newBuilder().setResult(result).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.DeleteResponseProto delete(
      RpcController controller,
      ClientNamenodeProtocolProtos.DeleteRequestProto req) {
    asyncRouterServer(() -> server.delete(req.getSrc(), req.getRecursive()),
        result -> ClientNamenodeProtocolProtos
        .DeleteResponseProto.newBuilder().setResult(result).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.MkdirsResponseProto mkdirs(
      RpcController controller,
      ClientNamenodeProtocolProtos.MkdirsRequestProto req) {
    asyncRouterServer(() -> {
      FsPermission masked = req.hasUnmasked() ?
          FsCreateModes.create(PBHelperClient.convert(req.getMasked()),
              PBHelperClient.convert(req.getUnmasked())) :
          PBHelperClient.convert(req.getMasked());
      boolean result = server.mkdirs(req.getSrc(), masked,
          req.getCreateParent());
      return result;
    }, result -> ClientNamenodeProtocolProtos
        .MkdirsResponseProto.newBuilder().setResult(result).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetListingResponseProto getListing(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetListingRequestProto req) {
    asyncRouterServer(() -> {
      DirectoryListing result = server.getListing(
          req.getSrc(), req.getStartAfter().toByteArray(),
          req.getNeedLocation());
      return result;
    }, result -> {
      if (result !=null) {
        return ClientNamenodeProtocolProtos
            .GetListingResponseProto.newBuilder().setDirList(
            PBHelperClient.convert(result)).build();
      } else {
        return VOID_GETLISTING_RESPONSE;
      }
    });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetBatchedListingResponseProto getBatchedListing(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetBatchedListingRequestProto request) {
    asyncRouterServer(() -> {
      BatchedDirectoryListing result = server.getBatchedListing(
          request.getPathsList().toArray(new String[]{}),
          request.getStartAfter().toByteArray(),
          request.getNeedLocation());
      return result;
    }, result -> {
      if (result != null) {
        ClientNamenodeProtocolProtos.GetBatchedListingResponseProto.Builder builder =
            ClientNamenodeProtocolProtos.GetBatchedListingResponseProto.newBuilder();
        for (HdfsPartialListing partialListing : result.getListings()) {
          HdfsProtos.BatchedDirectoryListingProto.Builder listingBuilder =
              HdfsProtos.BatchedDirectoryListingProto.newBuilder();
          if (partialListing.getException() != null) {
            RemoteException ex = partialListing.getException();
            HdfsProtos.RemoteExceptionProto.Builder rexBuilder =
                HdfsProtos.RemoteExceptionProto.newBuilder();
            rexBuilder.setClassName(ex.getClassName());
            if (ex.getMessage() != null) {
              rexBuilder.setMessage(ex.getMessage());
            }
            listingBuilder.setException(rexBuilder.build());
          } else {
            for (HdfsFileStatus f : partialListing.getPartialListing()) {
              listingBuilder.addPartialListing(PBHelperClient.convert(f));
            }
          }
          listingBuilder.setParentIdx(partialListing.getParentIdx());
          builder.addListings(listingBuilder);
        }
        builder.setHasMore(result.hasMore());
        builder.setStartAfter(ByteString.copyFrom(result.getStartAfter()));
        return builder.build();
      } else {
        return VOID_GETBATCHEDLISTING_RESPONSE;
      }
    });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.RenewLeaseResponseProto renewLease(
      RpcController controller,
      ClientNamenodeProtocolProtos.RenewLeaseRequestProto req) {
    asyncRouterServer(() -> {
      server.renewLease(req.getClientName(), req.getNamespacesList());
      return null;
    }, result -> VOID_RENEWLEASE_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.RecoverLeaseResponseProto recoverLease(
      RpcController controller,
      ClientNamenodeProtocolProtos.RecoverLeaseRequestProto req) {
    asyncRouterServer(() -> server.recoverLease(req.getSrc(), req.getClientName()),
        result -> ClientNamenodeProtocolProtos
            .RecoverLeaseResponseProto.newBuilder()
            .setResult(result).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.RestoreFailedStorageResponseProto restoreFailedStorage(
      RpcController controller, ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto req) {
    asyncRouterServer(() -> server.restoreFailedStorage(req.getArg()),
        result -> ClientNamenodeProtocolProtos
            .RestoreFailedStorageResponseProto.newBuilder().setResult(result)
            .build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetFsStatsResponseProto getFsStats(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetFsStatusRequestProto req) {
    asyncRouterServer(server::getStats, PBHelperClient::convert);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetFsReplicatedBlockStatsResponseProto getFsReplicatedBlockStats(
      RpcController controller, ClientNamenodeProtocolProtos.GetFsReplicatedBlockStatsRequestProto request) {
    asyncRouterServer(server::getReplicatedBlockStats, PBHelperClient::convert);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetFsECBlockGroupStatsResponseProto getFsECBlockGroupStats(
      RpcController controller, ClientNamenodeProtocolProtos.GetFsECBlockGroupStatsRequestProto request) {
    asyncRouterServer(server::getECBlockGroupStats, PBHelperClient::convert);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetDatanodeReportResponseProto getDatanodeReport(
      RpcController controller, ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto req) {
    asyncRouterServer(() -> server.getDatanodeReport(PBHelperClient.convert(req.getType())),
        result -> {
          List<? extends HdfsProtos.DatanodeInfoProto> re = PBHelperClient.convert(result);
          return ClientNamenodeProtocolProtos.GetDatanodeReportResponseProto.newBuilder()
          .addAllDi(re).build();
      });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetDatanodeStorageReportResponseProto getDatanodeStorageReport(
      RpcController controller, ClientNamenodeProtocolProtos.GetDatanodeStorageReportRequestProto req) {
    asyncRouterServer(() -> server.getDatanodeStorageReport(PBHelperClient.convert(req.getType())),
        result -> {
          List<ClientNamenodeProtocolProtos.DatanodeStorageReportProto> reports =
              PBHelperClient.convertDatanodeStorageReports(result);
          return ClientNamenodeProtocolProtos.GetDatanodeStorageReportResponseProto.newBuilder()
              .addAllDatanodeStorageReports(reports)
              .build();
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetPreferredBlockSizeResponseProto getPreferredBlockSize(
      RpcController controller, ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto req) {
    asyncRouterServer(() -> server.getPreferredBlockSize(req.getFilename()),
        result -> ClientNamenodeProtocolProtos
            .GetPreferredBlockSizeResponseProto.newBuilder().setBsize(result)
            .build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.SetSafeModeResponseProto setSafeMode(
      RpcController controller,
      ClientNamenodeProtocolProtos.SetSafeModeRequestProto req) {
    asyncRouterServer(() -> server.setSafeMode(PBHelperClient.convert(req.getAction()),
        req.getChecked()),
        result -> ClientNamenodeProtocolProtos
            .SetSafeModeResponseProto.newBuilder().setResult(result).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.SaveNamespaceResponseProto saveNamespace(
      RpcController controller,
      ClientNamenodeProtocolProtos.SaveNamespaceRequestProto req) {
    asyncRouterServer(() -> {
      final long timeWindow = req.hasTimeWindow() ? req.getTimeWindow() : 0;
      final long txGap = req.hasTxGap() ? req.getTxGap() : 0;
      return server.saveNamespace(timeWindow, txGap);
    }, result -> ClientNamenodeProtocolProtos
        .SaveNamespaceResponseProto.newBuilder().setSaved(result).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.RollEditsResponseProto rollEdits(
      RpcController controller,
      ClientNamenodeProtocolProtos.RollEditsRequestProto request) {
    asyncRouterServer(server::rollEdits,
        txid -> ClientNamenodeProtocolProtos.RollEditsResponseProto.newBuilder()
            .setNewSegmentTxId(txid)
            .build());
    return null;
  }


  @Override
  public ClientNamenodeProtocolProtos.RefreshNodesResponseProto refreshNodes(
      RpcController controller,
      ClientNamenodeProtocolProtos.RefreshNodesRequestProto req) {
    asyncRouterServer(() -> {
      server.refreshNodes();
      return null;
    }, result -> VOID_REFRESHNODES_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.FinalizeUpgradeResponseProto finalizeUpgrade(
      RpcController controller,
      ClientNamenodeProtocolProtos.FinalizeUpgradeRequestProto req) {
    asyncRouterServer(() -> {
      server.finalizeUpgrade();
      return null;
    }, result -> VOID_REFRESHNODES_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.UpgradeStatusResponseProto upgradeStatus(
      RpcController controller, ClientNamenodeProtocolProtos.UpgradeStatusRequestProto req) {
    asyncRouterServer(server::upgradeStatus,
        result -> {
          ClientNamenodeProtocolProtos.UpgradeStatusResponseProto.Builder b =
              ClientNamenodeProtocolProtos.UpgradeStatusResponseProto.newBuilder();
          b.setUpgradeFinalized(result);
          return b.build();
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.RollingUpgradeResponseProto rollingUpgrade(
      RpcController controller,
      ClientNamenodeProtocolProtos.RollingUpgradeRequestProto req) {
    asyncRouterServer(() ->
        server.rollingUpgrade(PBHelperClient.convert(req.getAction())),
        info -> {
          final ClientNamenodeProtocolProtos.RollingUpgradeResponseProto.Builder b =
              ClientNamenodeProtocolProtos.RollingUpgradeResponseProto.newBuilder();
          if (info != null) {
            b.setRollingUpgradeInfo(PBHelperClient.convert(info));
          }
          return b.build();
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.ListCorruptFileBlocksResponseProto listCorruptFileBlocks(
      RpcController controller, ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto req) {
    asyncRouterServer(() -> server.listCorruptFileBlocks(
        req.getPath(), req.hasCookie() ? req.getCookie(): null),
        result -> ClientNamenodeProtocolProtos.ListCorruptFileBlocksResponseProto.newBuilder()
            .setCorrupt(PBHelperClient.convert(result))
            .build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.MetaSaveResponseProto metaSave(
      RpcController controller,
      ClientNamenodeProtocolProtos.MetaSaveRequestProto req) {
    asyncRouterServer(() -> {
      server.metaSave(req.getFilename());
      return null;
    }, result -> VOID_METASAVE_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetFileInfoResponseProto getFileInfo(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetFileInfoRequestProto req) {
    asyncRouterServer(() -> server.getFileInfo(req.getSrc()),
        result -> {
          if (result != null) {
            return ClientNamenodeProtocolProtos.GetFileInfoResponseProto.newBuilder().setFs(
                PBHelperClient.convert(result)).build();
          }
          return VOID_GETFILEINFO_RESPONSE;
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetLocatedFileInfoResponseProto getLocatedFileInfo(
      RpcController controller, ClientNamenodeProtocolProtos.GetLocatedFileInfoRequestProto req) {
    asyncRouterServer(() -> server.getLocatedFileInfo(req.getSrc(),
        req.getNeedBlockToken()),
        result -> {
          if (result != null) {
            return ClientNamenodeProtocolProtos.GetLocatedFileInfoResponseProto.newBuilder().setFs(
                PBHelperClient.convert(result)).build();
          }
          return VOID_GETLOCATEDFILEINFO_RESPONSE;
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto getFileLinkInfo(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto req) {
    asyncRouterServer(() -> server.getFileLinkInfo(req.getSrc()),
        result -> {
          if (result != null) {
            return ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto.newBuilder().setFs(
                PBHelperClient.convert(result)).build();
          } else {
            return VOID_GETFILELINKINFO_RESPONSE;
          }
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetContentSummaryResponseProto getContentSummary(
      RpcController controller, ClientNamenodeProtocolProtos.GetContentSummaryRequestProto req) {
    asyncRouterServer(() -> server.getContentSummary(req.getPath()),
        result -> ClientNamenodeProtocolProtos.GetContentSummaryResponseProto.newBuilder()
            .setSummary(PBHelperClient.convert(result)).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.SetQuotaResponseProto setQuota(
      RpcController controller,
      ClientNamenodeProtocolProtos.SetQuotaRequestProto req) throws ServiceException {
    asyncRouterServer(() -> {
      server.setQuota(req.getPath(), req.getNamespaceQuota(),
          req.getStoragespaceQuota(),
          req.hasStorageType() ?
              PBHelperClient.convertStorageType(req.getStorageType()): null);
      return null;
    }, result -> VOID_SETQUOTA_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.FsyncResponseProto fsync(
      RpcController controller,
      ClientNamenodeProtocolProtos.FsyncRequestProto req) {
    asyncRouterServer(() -> {
      server.fsync(req.getSrc(), req.getFileId(),
          req.getClient(), req.getLastBlockLength());
      return null;
    }, result -> VOID_FSYNC_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.SetTimesResponseProto setTimes(
      RpcController controller,
      ClientNamenodeProtocolProtos.SetTimesRequestProto req) {
    asyncRouterServer(() -> {
      server.setTimes(req.getSrc(), req.getMtime(), req.getAtime());
      return null;
    }, result -> VOID_SETTIMES_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.CreateSymlinkResponseProto createSymlink(
      RpcController controller,
      ClientNamenodeProtocolProtos.CreateSymlinkRequestProto req) {
    asyncRouterServer(() -> {
      server.createSymlink(req.getTarget(), req.getLink(),
          PBHelperClient.convert(req.getDirPerm()), req.getCreateParent());
      return null;
    }, result -> VOID_CREATESYMLINK_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetLinkTargetResponseProto getLinkTarget(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetLinkTargetRequestProto req) {
    asyncRouterServer(() -> server.getLinkTarget(req.getPath()),
        result -> {
          ClientNamenodeProtocolProtos.GetLinkTargetResponseProto.Builder builder =
              ClientNamenodeProtocolProtos.GetLinkTargetResponseProto
              .newBuilder();
          if (result != null) {
            builder.setTargetPath(result);
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.UpdateBlockForPipelineResponseProto updateBlockForPipeline(
      RpcController controller, ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto req) {
    asyncRouterServer(() -> server.updateBlockForPipeline(PBHelperClient.convert(req.getBlock()),
        req.getClientName()),
        result -> {
          HdfsProtos.LocatedBlockProto res = PBHelperClient.convertLocatedBlock(result);
          return ClientNamenodeProtocolProtos
              .UpdateBlockForPipelineResponseProto.newBuilder().setBlock(res)
              .build();
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.UpdatePipelineResponseProto updatePipeline(
      RpcController controller,
      ClientNamenodeProtocolProtos.UpdatePipelineRequestProto req) {
    asyncRouterServer(() -> {
      List<HdfsProtos.DatanodeIDProto> newNodes = req.getNewNodesList();
      List<String> newStorageIDs = req.getStorageIDsList();
      server.updatePipeline(req.getClientName(),
          PBHelperClient.convert(req.getOldBlock()),
          PBHelperClient.convert(req.getNewBlock()),
          PBHelperClient.convert(newNodes.toArray(new HdfsProtos.DatanodeIDProto[newNodes.size()])),
          newStorageIDs.toArray(new String[newStorageIDs.size()]));
      return null;
    }, result -> VOID_UPDATEPIPELINE_RESPONSE);
    return null;
  }

  @Override
  public SecurityProtos.GetDelegationTokenResponseProto getDelegationToken(
      RpcController controller, SecurityProtos.GetDelegationTokenRequestProto req) {
    asyncRouterServer(() -> server
        .getDelegationToken(new Text(req.getRenewer())),
        token -> {
          SecurityProtos.GetDelegationTokenResponseProto.Builder rspBuilder =
              SecurityProtos.GetDelegationTokenResponseProto.newBuilder();
          if (token != null) {
            rspBuilder.setToken(PBHelperClient.convert(token));
          }
          return rspBuilder.build();
        });
    return null;
  }

  @Override
  public SecurityProtos.RenewDelegationTokenResponseProto renewDelegationToken(
      RpcController controller, SecurityProtos.RenewDelegationTokenRequestProto req) {
    asyncRouterServer(() -> server.renewDelegationToken(PBHelperClient
        .convertDelegationToken(req.getToken())),
        result -> SecurityProtos.RenewDelegationTokenResponseProto.newBuilder()
            .setNewExpiryTime(result).build());
    return null;
  }

  @Override
  public SecurityProtos.CancelDelegationTokenResponseProto cancelDelegationToken(
      RpcController controller, SecurityProtos.CancelDelegationTokenRequestProto req) {
    asyncRouterServer(() -> {
      server.cancelDelegationToken(PBHelperClient.convertDelegationToken(req
          .getToken()));
      return null;
    }, result -> VOID_CANCELDELEGATIONTOKEN_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.SetBalancerBandwidthResponseProto setBalancerBandwidth(
      RpcController controller, ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto req) {
    asyncRouterServer(() -> {
      server.setBalancerBandwidth(req.getBandwidth());
      return null;
    }, result -> VOID_SETBALANCERBANDWIDTH_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto getDataEncryptionKey(
      RpcController controller, ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto request) {
    asyncRouterServer(server::getDataEncryptionKey, encryptionKey -> {
      ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto.newBuilder();
      if (encryptionKey != null) {
        builder.setDataEncryptionKey(PBHelperClient.convert(encryptionKey));
      }
      return builder.build();
    });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.CreateSnapshotResponseProto createSnapshot(
      RpcController controller,
      ClientNamenodeProtocolProtos.CreateSnapshotRequestProto req) throws ServiceException {
    asyncRouterServer(() -> server.createSnapshot(req.getSnapshotRoot(),
        req.hasSnapshotName()? req.getSnapshotName(): null),
        snapshotPath -> {
          final ClientNamenodeProtocolProtos.CreateSnapshotResponseProto.Builder builder
              = ClientNamenodeProtocolProtos.CreateSnapshotResponseProto.newBuilder();
          if (snapshotPath != null) {
            builder.setSnapshotPath(snapshotPath);
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.DeleteSnapshotResponseProto deleteSnapshot(
      RpcController controller,
      ClientNamenodeProtocolProtos.DeleteSnapshotRequestProto req) {
    asyncRouterServer(() -> {
      server.deleteSnapshot(req.getSnapshotRoot(), req.getSnapshotName());
      return null;
    }, result -> VOID_DELETE_SNAPSHOT_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.AllowSnapshotResponseProto allowSnapshot(
      RpcController controller,
      ClientNamenodeProtocolProtos.AllowSnapshotRequestProto req) {
    asyncRouterServer(() -> {
      server.allowSnapshot(req.getSnapshotRoot());
      return null;
    }, result -> VOID_ALLOW_SNAPSHOT_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.DisallowSnapshotResponseProto disallowSnapshot(
      RpcController controller,
      ClientNamenodeProtocolProtos.DisallowSnapshotRequestProto req) {
    asyncRouterServer(() -> {
      server.disallowSnapshot(req.getSnapshotRoot());
      return null;
    }, result -> VOID_DISALLOW_SNAPSHOT_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.RenameSnapshotResponseProto renameSnapshot(
      RpcController controller,
      ClientNamenodeProtocolProtos.RenameSnapshotRequestProto request) {
    asyncRouterServer(() -> {
      server.renameSnapshot(request.getSnapshotRoot(),
          request.getSnapshotOldName(), request.getSnapshotNewName());
      return null;
    }, result -> VOID_RENAME_SNAPSHOT_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto getSnapshottableDirListing(
      RpcController controller, ClientNamenodeProtocolProtos.GetSnapshottableDirListingRequestProto request) {
    asyncRouterServer(server::getSnapshottableDirListing,
        result -> {
          if (result != null) {
            return ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto.newBuilder().
                setSnapshottableDirList(PBHelperClient.convert(result)).build();
          } else {
            return NULL_GET_SNAPSHOTTABLE_DIR_LISTING_RESPONSE;
          }
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetSnapshotListingResponseProto getSnapshotListing(
      RpcController controller, ClientNamenodeProtocolProtos.GetSnapshotListingRequestProto request) {
    asyncRouterServer(() -> server
        .getSnapshotListing(request.getSnapshotRoot()),
        result -> {
          if (result != null) {
            return ClientNamenodeProtocolProtos.GetSnapshotListingResponseProto.newBuilder().
                setSnapshotList(PBHelperClient.convert(result)).build();
          } else {
            return NULL_GET_SNAPSHOT_LISTING_RESPONSE;
          }
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto getSnapshotDiffReport(
      RpcController controller, ClientNamenodeProtocolProtos.GetSnapshotDiffReportRequestProto request) {
    asyncRouterServer(() -> server.getSnapshotDiffReport(
        request.getSnapshotRoot(), request.getFromSnapshot(),
        request.getToSnapshot()),
        report -> ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto.newBuilder()
            .setDiffReport(PBHelperClient.convert(report)).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetSnapshotDiffReportListingResponseProto getSnapshotDiffReportListing(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetSnapshotDiffReportListingRequestProto request) {
    asyncRouterServer(() -> server
        .getSnapshotDiffReportListing(request.getSnapshotRoot(),
            request.getFromSnapshot(), request.getToSnapshot(),
            request.getCursor().getStartPath().toByteArray(),
            request.getCursor().getIndex()),
        report -> ClientNamenodeProtocolProtos
            .GetSnapshotDiffReportListingResponseProto.newBuilder()
            .setDiffReport(PBHelperClient.convert(report)).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.IsFileClosedResponseProto isFileClosed(
      RpcController controller, ClientNamenodeProtocolProtos.IsFileClosedRequestProto request) {
    asyncRouterServer(() -> server.isFileClosed(request.getSrc()),
        result -> ClientNamenodeProtocolProtos
            .IsFileClosedResponseProto.newBuilder()
            .setResult(result).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.AddCacheDirectiveResponseProto addCacheDirective(
      RpcController controller, ClientNamenodeProtocolProtos.AddCacheDirectiveRequestProto request) {
    asyncRouterServer(() -> server.addCacheDirective(
        PBHelperClient.convert(request.getInfo()),
        PBHelperClient.convertCacheFlags(request.getCacheFlags())),
        id -> ClientNamenodeProtocolProtos.AddCacheDirectiveResponseProto.newBuilder().
            setId(id).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.ModifyCacheDirectiveResponseProto modifyCacheDirective(
      RpcController controller, ClientNamenodeProtocolProtos.ModifyCacheDirectiveRequestProto request) {
    asyncRouterServer(() -> {
      server.modifyCacheDirective(
          PBHelperClient.convert(request.getInfo()),
          PBHelperClient.convertCacheFlags(request.getCacheFlags()));
      return null;
    }, result -> ClientNamenodeProtocolProtos.ModifyCacheDirectiveResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.RemoveCacheDirectiveResponseProto
      removeCacheDirective(
          RpcController controller,
          ClientNamenodeProtocolProtos.RemoveCacheDirectiveRequestProto request) {
    asyncRouterServer(() -> {
      server.removeCacheDirective(request.getId());
      return null;
    }, result -> ClientNamenodeProtocolProtos.RemoveCacheDirectiveResponseProto.
        newBuilder().build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto listCacheDirectives(
      RpcController controller, ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto request) {
    asyncRouterServer(() -> {
      CacheDirectiveInfo filter =
          PBHelperClient.convert(request.getFilter());
      return  server.listCacheDirectives(request.getPrevId(), filter);
    }, entries -> {
      ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto.newBuilder();
      builder.setHasMore(entries.hasMore());
      for (int i=0, n=entries.size(); i<n; i++) {
        builder.addElements(PBHelperClient.convert(entries.get(i)));
      }
      return builder.build();
    });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.AddCachePoolResponseProto addCachePool(
      RpcController controller,
      ClientNamenodeProtocolProtos.AddCachePoolRequestProto request) {
    asyncRouterServer(() -> {
      server.addCachePool(PBHelperClient.convert(request.getInfo()));
      return null;
    }, result -> ClientNamenodeProtocolProtos.AddCachePoolResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.ModifyCachePoolResponseProto modifyCachePool(
      RpcController controller,
      ClientNamenodeProtocolProtos.ModifyCachePoolRequestProto request) {
    asyncRouterServer(() -> {
      server.modifyCachePool(PBHelperClient.convert(request.getInfo()));
      return null;
    }, result -> ClientNamenodeProtocolProtos.ModifyCachePoolResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.RemoveCachePoolResponseProto removeCachePool(
      RpcController controller,
      ClientNamenodeProtocolProtos.RemoveCachePoolRequestProto request) {
    asyncRouterServer(() -> {
      server.removeCachePool(request.getPoolName());
      return null;
    }, result -> ClientNamenodeProtocolProtos.RemoveCachePoolResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.ListCachePoolsResponseProto listCachePools(
      RpcController controller,
      ClientNamenodeProtocolProtos.ListCachePoolsRequestProto request) {
    asyncRouterServer(() -> server.listCachePools(request.getPrevPoolName()),
        entries -> {
          ClientNamenodeProtocolProtos.ListCachePoolsResponseProto.Builder responseBuilder =
              ClientNamenodeProtocolProtos.ListCachePoolsResponseProto.newBuilder();
          responseBuilder.setHasMore(entries.hasMore());
          for (int i=0, n=entries.size(); i<n; i++) {
            responseBuilder.addEntries(PBHelperClient.convert(entries.get(i)));
          }
          return responseBuilder.build();
        });
    return null;
  }

  @Override
  public AclProtos.ModifyAclEntriesResponseProto modifyAclEntries(
      RpcController controller, AclProtos.ModifyAclEntriesRequestProto req)
      throws ServiceException {
    asyncRouterServer(() -> {
      server.modifyAclEntries(req.getSrc(), PBHelperClient.convertAclEntry(req.getAclSpecList()));
      return null;
    }, vo -> VOID_MODIFYACLENTRIES_RESPONSE);
    return null;
  }

  @Override
  public AclProtos.RemoveAclEntriesResponseProto removeAclEntries(
      RpcController controller, AclProtos.RemoveAclEntriesRequestProto req) {
    asyncRouterServer(() -> {
      server.removeAclEntries(req.getSrc(),
          PBHelperClient.convertAclEntry(req.getAclSpecList()));
      return null;
    }, vo -> VOID_REMOVEACLENTRIES_RESPONSE);
    return null;
  }

  @Override
  public AclProtos.RemoveDefaultAclResponseProto removeDefaultAcl(
      RpcController controller, AclProtos.RemoveDefaultAclRequestProto req) {
    asyncRouterServer(() -> {
      server.removeDefaultAcl(req.getSrc());
      return null;
    }, vo -> VOID_REMOVEDEFAULTACL_RESPONSE);
    return null;
  }

  @Override
  public AclProtos.RemoveAclResponseProto removeAcl(
      RpcController controller,
      AclProtos.RemoveAclRequestProto req) {
    asyncRouterServer(() -> {
      server.removeAcl(req.getSrc());
      return null;
    }, vo -> VOID_REMOVEACL_RESPONSE);
    return null;
  }

  @Override
  public AclProtos.SetAclResponseProto setAcl(
      RpcController controller,
      AclProtos.SetAclRequestProto req) {
    asyncRouterServer(() -> {
      server.setAcl(req.getSrc(), PBHelperClient.convertAclEntry(req.getAclSpecList()));
      return null;
    }, vo -> VOID_SETACL_RESPONSE);
    return null;
  }

  @Override
  public AclProtos.GetAclStatusResponseProto getAclStatus(
      RpcController controller,
      AclProtos.GetAclStatusRequestProto req) {
    asyncRouterServer(() -> server.getAclStatus(req.getSrc()),
        PBHelperClient::convert);
    return null;
  }

  @Override
  public EncryptionZonesProtos.CreateEncryptionZoneResponseProto createEncryptionZone(
      RpcController controller, EncryptionZonesProtos.CreateEncryptionZoneRequestProto req) {
    asyncRouterServer(() -> {
      server.createEncryptionZone(req.getSrc(), req.getKeyName());
      return null;
    }, vo -> EncryptionZonesProtos.CreateEncryptionZoneResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public EncryptionZonesProtos.GetEZForPathResponseProto getEZForPath(
      RpcController controller, EncryptionZonesProtos.GetEZForPathRequestProto req) {
    asyncRouterServer(() -> server.getEZForPath(req.getSrc()),
        ret -> {
          EncryptionZonesProtos.GetEZForPathResponseProto.Builder builder =
              EncryptionZonesProtos.GetEZForPathResponseProto.newBuilder();
          if (ret != null) {
            builder.setZone(PBHelperClient.convert(ret));
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public EncryptionZonesProtos.ListEncryptionZonesResponseProto listEncryptionZones(
      RpcController controller, EncryptionZonesProtos.ListEncryptionZonesRequestProto req) {
    asyncRouterServer(() -> server.listEncryptionZones(req.getId()),
        entries -> {
          EncryptionZonesProtos.ListEncryptionZonesResponseProto.Builder builder =
              EncryptionZonesProtos.ListEncryptionZonesResponseProto.newBuilder();
          builder.setHasMore(entries.hasMore());
          for (int i=0; i<entries.size(); i++) {
            builder.addZones(PBHelperClient.convert(entries.get(i)));
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public EncryptionZonesProtos.ReencryptEncryptionZoneResponseProto reencryptEncryptionZone(
      RpcController controller, EncryptionZonesProtos.ReencryptEncryptionZoneRequestProto req) {
    asyncRouterServer(() -> {
      server.reencryptEncryptionZone(req.getZone(),
          PBHelperClient.convert(req.getAction()));
      return null;
    }, vo -> EncryptionZonesProtos.ReencryptEncryptionZoneResponseProto.newBuilder().build());
    return null;
  }

  public EncryptionZonesProtos.ListReencryptionStatusResponseProto listReencryptionStatus(
      RpcController controller, EncryptionZonesProtos.ListReencryptionStatusRequestProto req) {
    asyncRouterServer(() -> server.listReencryptionStatus(req.getId()),
        entries -> {
          EncryptionZonesProtos.ListReencryptionStatusResponseProto.Builder builder =
              EncryptionZonesProtos.ListReencryptionStatusResponseProto.newBuilder();
          builder.setHasMore(entries.hasMore());
          for (int i=0; i<entries.size(); i++) {
            builder.addStatuses(PBHelperClient.convert(entries.get(i)));
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public ErasureCodingProtos.SetErasureCodingPolicyResponseProto setErasureCodingPolicy(
      RpcController controller, ErasureCodingProtos.SetErasureCodingPolicyRequestProto req) {
    asyncRouterServer(() -> {
      String ecPolicyName = req.hasEcPolicyName() ?
          req.getEcPolicyName() : null;
      server.setErasureCodingPolicy(req.getSrc(), ecPolicyName);
      return null;
    }, vo -> ErasureCodingProtos
        .SetErasureCodingPolicyResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public ErasureCodingProtos.UnsetErasureCodingPolicyResponseProto unsetErasureCodingPolicy(
      RpcController controller, ErasureCodingProtos.UnsetErasureCodingPolicyRequestProto req) {
    asyncRouterServer(() -> {
      server.unsetErasureCodingPolicy(req.getSrc());
      return null;
    }, vo -> ErasureCodingProtos
        .UnsetErasureCodingPolicyResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public ErasureCodingProtos.GetECTopologyResultForPoliciesResponseProto getECTopologyResultForPolicies(
      RpcController controller, ErasureCodingProtos.GetECTopologyResultForPoliciesRequestProto req) {
    asyncRouterServer(() -> {
      ProtocolStringList policies = req.getPoliciesList();
      return server.getECTopologyResultForPolicies(
          policies.toArray(policies.toArray(new String[policies.size()])));
    }, result -> {
      ErasureCodingProtos.GetECTopologyResultForPoliciesResponseProto.Builder builder =
          ErasureCodingProtos.GetECTopologyResultForPoliciesResponseProto.newBuilder();
      builder
          .setResponse(PBHelperClient.convertECTopologyVerifierResult(result));
      return builder.build();
    });
    return null;
  }

  @Override
  public XAttrProtos.SetXAttrResponseProto setXAttr(
      RpcController controller,
      XAttrProtos.SetXAttrRequestProto req) {
    asyncRouterServer(() -> {
      server.setXAttr(req.getSrc(), PBHelperClient.convertXAttr(req.getXAttr()),
          PBHelperClient.convert(req.getFlag()));
      return null;
    }, vo -> VOID_SETXATTR_RESPONSE);
    return null;
  }

  @Override
  public XAttrProtos.GetXAttrsResponseProto getXAttrs(
      RpcController controller,
      XAttrProtos.GetXAttrsRequestProto req) {
    asyncRouterServer(() -> server.getXAttrs(req.getSrc(),
        PBHelperClient.convertXAttrs(req.getXAttrsList())),
        PBHelperClient::convertXAttrsResponse);
    return null;
  }

  @Override
  public XAttrProtos.ListXAttrsResponseProto listXAttrs(
      RpcController controller,
      XAttrProtos.ListXAttrsRequestProto req) {
    asyncRouterServer(() -> server.listXAttrs(req.getSrc()),
        PBHelperClient::convertListXAttrsResponse);
    return null;
  }

  @Override
  public XAttrProtos.RemoveXAttrResponseProto removeXAttr(
      RpcController controller,
      XAttrProtos.RemoveXAttrRequestProto req) {
    asyncRouterServer(() -> {
      server.removeXAttr(req.getSrc(), PBHelperClient.convertXAttr(req.getXAttr()));
      return null;
    }, vo -> VOID_REMOVEXATTR_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.CheckAccessResponseProto checkAccess(
      RpcController controller,
      ClientNamenodeProtocolProtos.CheckAccessRequestProto req) {
    asyncRouterServer(() -> {
      server.checkAccess(req.getPath(), PBHelperClient.convert(req.getMode()));
      return null;
    }, vo -> VOID_CHECKACCESS_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.SetStoragePolicyResponseProto setStoragePolicy(
      RpcController controller, ClientNamenodeProtocolProtos.SetStoragePolicyRequestProto request) {
    asyncRouterServer(() -> {
      server.setStoragePolicy(request.getSrc(), request.getPolicyName());
      return null;
    }, vo -> VOID_SET_STORAGE_POLICY_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.UnsetStoragePolicyResponseProto unsetStoragePolicy(
      RpcController controller, ClientNamenodeProtocolProtos.UnsetStoragePolicyRequestProto request) {
    asyncRouterServer(() -> {
      server.unsetStoragePolicy(request.getSrc());
      return null;
    }, vo -> VOID_UNSET_STORAGE_POLICY_RESPONSE);
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetStoragePolicyResponseProto getStoragePolicy(
      RpcController controller, ClientNamenodeProtocolProtos.GetStoragePolicyRequestProto request) {
    asyncRouterServer(() -> server.getStoragePolicy(request.getPath()),
        result -> {
          HdfsProtos.BlockStoragePolicyProto policy = PBHelperClient.convert(result);
          return ClientNamenodeProtocolProtos.GetStoragePolicyResponseProto.newBuilder()
              .setStoragePolicy(policy).build();
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto getStoragePolicies(
      RpcController controller, ClientNamenodeProtocolProtos.GetStoragePoliciesRequestProto request) {
    asyncRouterServer(server::getStoragePolicies,
        policies -> {
          ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto.Builder builder =
              ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto.newBuilder();
          if (policies == null) {
            return builder.build();
          }
          for (BlockStoragePolicy policy : policies) {
            builder.addPolicies(PBHelperClient.convert(policy));
          }
          return builder.build();
        });
    return null;
  }

  public ClientNamenodeProtocolProtos.GetCurrentEditLogTxidResponseProto getCurrentEditLogTxid(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetCurrentEditLogTxidRequestProto req) throws ServiceException {
    asyncRouterServer(server::getCurrentEditLogTxid,
        result -> ClientNamenodeProtocolProtos
            .GetCurrentEditLogTxidResponseProto.newBuilder()
            .setTxid(result).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetEditsFromTxidResponseProto getEditsFromTxid(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetEditsFromTxidRequestProto req) {
    asyncRouterServer(() -> server.getEditsFromTxid(req.getTxid()),
        PBHelperClient::convertEditsResponse);
    return null;
  }

  @Override
  public ErasureCodingProtos.GetErasureCodingPoliciesResponseProto getErasureCodingPolicies(
      RpcController controller,
      ErasureCodingProtos.GetErasureCodingPoliciesRequestProto request) {
    asyncRouterServer(server::getErasureCodingPolicies,
        ecpInfos -> {
          ErasureCodingProtos.GetErasureCodingPoliciesResponseProto.Builder resBuilder =
              ErasureCodingProtos.GetErasureCodingPoliciesResponseProto
              .newBuilder();
          for (ErasureCodingPolicyInfo info : ecpInfos) {
            resBuilder.addEcPolicies(
                PBHelperClient.convertErasureCodingPolicy(info));
          }
          return resBuilder.build();
        });
    return null;
  }

  @Override
  public ErasureCodingProtos.GetErasureCodingCodecsResponseProto getErasureCodingCodecs(
      RpcController controller, ErasureCodingProtos.GetErasureCodingCodecsRequestProto request) {
    asyncRouterServer(server::getErasureCodingCodecs,
        codecs -> {
          ErasureCodingProtos.GetErasureCodingCodecsResponseProto.Builder resBuilder =
              ErasureCodingProtos.GetErasureCodingCodecsResponseProto.newBuilder();
          for (Map.Entry<String, String> codec : codecs.entrySet()) {
            resBuilder.addCodec(
                PBHelperClient.convertErasureCodingCodec(
                    codec.getKey(), codec.getValue()));
          }
          return resBuilder.build();
        });
    return null;
  }

  @Override
  public ErasureCodingProtos.AddErasureCodingPoliciesResponseProto addErasureCodingPolicies(
      RpcController controller, ErasureCodingProtos.AddErasureCodingPoliciesRequestProto request) {
    asyncRouterServer(() -> {
      ErasureCodingPolicy[] policies = request.getEcPoliciesList().stream()
          .map(PBHelperClient::convertErasureCodingPolicy)
          .toArray(ErasureCodingPolicy[]::new);
      return server
          .addErasureCodingPolicies(policies);
    }, result -> {
      List<HdfsProtos.AddErasureCodingPolicyResponseProto> responseProtos =
          Arrays.stream(result)
              .map(PBHelperClient::convertAddErasureCodingPolicyResponse)
              .collect(Collectors.toList());
      ErasureCodingProtos.AddErasureCodingPoliciesResponseProto response =
          ErasureCodingProtos.AddErasureCodingPoliciesResponseProto.newBuilder()
              .addAllResponses(responseProtos).build();
      return response;
    });
    return null;
  }

  @Override
  public ErasureCodingProtos.RemoveErasureCodingPolicyResponseProto removeErasureCodingPolicy(
      RpcController controller, ErasureCodingProtos.RemoveErasureCodingPolicyRequestProto request) {
    asyncRouterServer(() -> {
      server.removeErasureCodingPolicy(request.getEcPolicyName());
      return null;
    }, vo -> ErasureCodingProtos.RemoveErasureCodingPolicyResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public ErasureCodingProtos.EnableErasureCodingPolicyResponseProto enableErasureCodingPolicy(
      RpcController controller, ErasureCodingProtos.EnableErasureCodingPolicyRequestProto request) {
    asyncRouterServer(() -> {
      server.enableErasureCodingPolicy(request.getEcPolicyName());
      return null;
    }, vo -> ErasureCodingProtos.EnableErasureCodingPolicyResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public ErasureCodingProtos.DisableErasureCodingPolicyResponseProto disableErasureCodingPolicy(
      RpcController controller, ErasureCodingProtos.DisableErasureCodingPolicyRequestProto request) {
    asyncRouterServer(() -> {
      server.disableErasureCodingPolicy(request.getEcPolicyName());
      return null;
    }, vo -> ErasureCodingProtos.DisableErasureCodingPolicyResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public ErasureCodingProtos.GetErasureCodingPolicyResponseProto getErasureCodingPolicy(
      RpcController controller,
      ErasureCodingProtos.GetErasureCodingPolicyRequestProto request) {
    asyncRouterServer(() -> server.getErasureCodingPolicy(request.getSrc()),
        ecPolicy -> {
          ErasureCodingProtos.GetErasureCodingPolicyResponseProto.Builder builder =
              ErasureCodingProtos.GetErasureCodingPolicyResponseProto.newBuilder();
          if (ecPolicy != null) {
            builder.setEcPolicy(PBHelperClient.convertErasureCodingPolicy(ecPolicy));
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetQuotaUsageResponseProto getQuotaUsage(
      RpcController controller, ClientNamenodeProtocolProtos.GetQuotaUsageRequestProto req) {
    asyncRouterServer(() -> server.getQuotaUsage(req.getPath()),
        result -> ClientNamenodeProtocolProtos.GetQuotaUsageResponseProto.newBuilder()
            .setUsage(PBHelperClient.convert(result)).build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.ListOpenFilesResponseProto listOpenFiles(
      RpcController controller,
      ClientNamenodeProtocolProtos.ListOpenFilesRequestProto req) {
    asyncRouterServer(() -> {
      EnumSet<OpenFilesIterator.OpenFilesType> openFilesTypes =
          PBHelperClient.convertOpenFileTypes(req.getTypesList());
      return server.listOpenFiles(req.getId(),
          openFilesTypes, req.getPath());
    }, entries -> {
      ClientNamenodeProtocolProtos.ListOpenFilesResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.ListOpenFilesResponseProto.newBuilder();
      builder.setHasMore(entries.hasMore());
      for (int i = 0; i < entries.size(); i++) {
        builder.addEntries(PBHelperClient.convert(entries.get(i)));
      }
      builder.addAllTypes(req.getTypesList());
      return builder.build();
    });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.MsyncResponseProto msync(
      RpcController controller,
      ClientNamenodeProtocolProtos.MsyncRequestProto req) {
    asyncRouterServer(() -> {
      server.msync();
      return null;
    }, vo -> ClientNamenodeProtocolProtos.MsyncResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.SatisfyStoragePolicyResponseProto satisfyStoragePolicy(
      RpcController controller,
      ClientNamenodeProtocolProtos.SatisfyStoragePolicyRequestProto request) throws ServiceException {
    try {
      server.satisfyStoragePolicy(request.getSrc());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_SATISFYSTORAGEPOLICY_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.HAServiceStateResponseProto getHAServiceState(
      RpcController controller,
      ClientNamenodeProtocolProtos.HAServiceStateRequestProto request) {
    asyncRouterServer(server::getHAServiceState,
        state -> {
          HAServiceProtocolProtos.HAServiceStateProto retState;
          switch (state) {
            case ACTIVE:
              retState = HAServiceProtocolProtos.HAServiceStateProto.ACTIVE;
              break;
            case STANDBY:
              retState = HAServiceProtocolProtos.HAServiceStateProto.STANDBY;
              break;
            case OBSERVER:
              retState = HAServiceProtocolProtos.HAServiceStateProto.OBSERVER;
              break;
            case INITIALIZING:
            default:
              retState = HAServiceProtocolProtos.HAServiceStateProto.INITIALIZING;
              break;
          }
          ClientNamenodeProtocolProtos.HAServiceStateResponseProto.Builder builder =
              ClientNamenodeProtocolProtos.HAServiceStateResponseProto.newBuilder();
          builder.setState(retState);
          return builder.build();
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetSlowDatanodeReportResponseProto getSlowDatanodeReport(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetSlowDatanodeReportRequestProto request) {
    asyncRouterServer(server::getSlowDatanodeReport,
        res -> {
          List<? extends HdfsProtos.DatanodeInfoProto> result = PBHelperClient.convert(res);
          return ClientNamenodeProtocolProtos.GetSlowDatanodeReportResponseProto.newBuilder()
              .addAllDatanodeInfoProto(result)
              .build();
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetEnclosingRootResponseProto getEnclosingRoot(
      RpcController controller, ClientNamenodeProtocolProtos.GetEnclosingRootRequestProto req) {
    asyncRouterServer(() -> server.getEnclosingRoot(req.getFilename()),
        enclosingRootPath -> ClientNamenodeProtocolProtos
            .GetEnclosingRootResponseProto.newBuilder()
            .setEnclosingRootPath(enclosingRootPath.toUri().toString())
            .build());
    return null;
  }
}
