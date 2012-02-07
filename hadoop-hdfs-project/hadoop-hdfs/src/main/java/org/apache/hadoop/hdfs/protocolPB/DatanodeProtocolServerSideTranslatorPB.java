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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReceivedAndDeletedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReceivedAndDeletedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CommitBlockSynchronizationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CommitBlockSynchronizationResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ErrorReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ErrorReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ProcessUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ProcessUpgradeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReportBadBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeIDProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.VersionResponseProto;
import org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class DatanodeProtocolServerSideTranslatorPB implements
    DatanodeProtocolPB {

  private final DatanodeProtocol impl;
  private static final ErrorReportResponseProto ERROR_REPORT_RESPONSE_PROTO = 
      ErrorReportResponseProto.newBuilder().build();
  private static final BlockReceivedAndDeletedResponseProto 
      BLOCK_RECEIVED_AND_DELETE_RESPONSE = 
          BlockReceivedAndDeletedResponseProto.newBuilder().build();
  private static final ReportBadBlocksResponseProto REPORT_BAD_BLOCK_RESPONSE = 
      ReportBadBlocksResponseProto.newBuilder().build();
  private static final CommitBlockSynchronizationResponseProto 
      COMMIT_BLOCK_SYNCHRONIZATION_RESPONSE_PROTO =
          CommitBlockSynchronizationResponseProto.newBuilder().build();

  public DatanodeProtocolServerSideTranslatorPB(DatanodeProtocol impl) {
    this.impl = impl;
  }

  @Override
  public RegisterDatanodeResponseProto registerDatanode(
      RpcController controller, RegisterDatanodeRequestProto request)
      throws ServiceException {
    DatanodeRegistration registration = PBHelper.convert(request
        .getRegistration());
    DatanodeRegistration registrationResp;
    try {
      registrationResp = impl.registerDatanode(registration);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return RegisterDatanodeResponseProto.newBuilder()
        .setRegistration(PBHelper.convert(registrationResp)).build();
  }

  @Override
  public HeartbeatResponseProto sendHeartbeat(RpcController controller,
      HeartbeatRequestProto request) throws ServiceException {
    DatanodeCommand[] cmds = null;
    try {
      StorageReportProto report = request.getReports(0);
      cmds = impl.sendHeartbeat(PBHelper.convert(request.getRegistration()),
          report.getCapacity(), report.getDfsUsed(), report.getRemaining(),
          report.getBlockPoolUsed(), request.getXmitsInProgress(),
          request.getXceiverCount(), request.getFailedVolumes());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    HeartbeatResponseProto.Builder builder = HeartbeatResponseProto
        .newBuilder();
    if (cmds != null) {
      for (int i = 0; i < cmds.length; i++) {
        if (cmds[i] != null) {
          builder.addCmds(PBHelper.convert(cmds[i]));
        }
      }
    }
    return builder.build();
  }

  @Override
  public BlockReportResponseProto blockReport(RpcController controller,
      BlockReportRequestProto request) throws ServiceException {
    DatanodeCommand cmd = null;
    List<Long> blockIds = request.getReports(0).getBlocksList();
    long[] blocks = new long[blockIds.size()];
    for (int i = 0; i < blockIds.size(); i++) {
      blocks[i] = blockIds.get(i);
    }
    try {
      cmd = impl.blockReport(PBHelper.convert(request.getRegistration()),
          request.getBlockPoolId(), blocks);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    BlockReportResponseProto.Builder builder = 
        BlockReportResponseProto.newBuilder();
    if (cmd != null) {
      builder.setCmd(PBHelper.convert(cmd));
    }
    return builder.build();
  }

  @Override
  public BlockReceivedAndDeletedResponseProto blockReceivedAndDeleted(
      RpcController controller, BlockReceivedAndDeletedRequestProto request)
      throws ServiceException {
    List<ReceivedDeletedBlockInfoProto> rdbip = request.getBlocks(0)
        .getBlocksList();
    ReceivedDeletedBlockInfo[] info = 
        new ReceivedDeletedBlockInfo[rdbip.size()];
    for (int i = 0; i < rdbip.size(); i++) {
      info[i] = PBHelper.convert(rdbip.get(i));
    }
    try {
      impl.blockReceivedAndDeleted(PBHelper.convert(request.getRegistration()),
          request.getBlockPoolId(), info);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return BLOCK_RECEIVED_AND_DELETE_RESPONSE;
  }

  @Override
  public ErrorReportResponseProto errorReport(RpcController controller,
      ErrorReportRequestProto request) throws ServiceException {
    try {
      impl.errorReport(PBHelper.convert(request.getRegistartion()),
          request.getErrorCode(), request.getMsg());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return ERROR_REPORT_RESPONSE_PROTO;
  }

  @Override
  public VersionResponseProto versionRequest(RpcController controller,
      VersionRequestProto request) throws ServiceException {
    NamespaceInfo info;
    try {
      info = impl.versionRequest();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VersionResponseProto.newBuilder()
        .setInfo(PBHelper.convert(info)).build();
  }

  @Override
  public ProcessUpgradeResponseProto processUpgrade(RpcController controller,
      ProcessUpgradeRequestProto request) throws ServiceException {
    UpgradeCommand ret;
    try {
      UpgradeCommand cmd = request.hasCmd() ? PBHelper
          .convert(request.getCmd()) : null;
      ret = impl.processUpgradeCommand(cmd);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    ProcessUpgradeResponseProto.Builder builder = 
        ProcessUpgradeResponseProto.newBuilder();
    if (ret != null) {
      builder.setCmd(PBHelper.convert(ret));
    }
    return builder.build();
  }

  @Override
  public ReportBadBlocksResponseProto reportBadBlocks(RpcController controller,
      ReportBadBlocksRequestProto request) throws ServiceException {
    List<LocatedBlockProto> lbps = request.getBlocksList();
    LocatedBlock [] blocks = new LocatedBlock [lbps.size()];
    for(int i=0; i<lbps.size(); i++) {
      blocks[i] = PBHelper.convert(lbps.get(i));
    }
    try {
      impl.reportBadBlocks(blocks);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return REPORT_BAD_BLOCK_RESPONSE;
  }

  @Override
  public CommitBlockSynchronizationResponseProto commitBlockSynchronization(
      RpcController controller, CommitBlockSynchronizationRequestProto request)
      throws ServiceException {
    List<DatanodeIDProto> dnprotos = request.getNewTaragetsList();
    DatanodeID[] dns = new DatanodeID[dnprotos.size()];
    for (int i = 0; i < dnprotos.size(); i++) {
      dns[i] = PBHelper.convert(dnprotos.get(i));
    }
    try {
      impl.commitBlockSynchronization(PBHelper.convert(request.getBlock()),
          request.getNewGenStamp(), request.getNewLength(),
          request.getCloseFile(), request.getDeleteBlock(), dns);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return COMMIT_BLOCK_SYNCHRONIZATION_RESPONSE_PROTO;
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return RPC.getProtocolVersion(DatanodeProtocolPB.class);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link DatanodeProtocol}
     */
    if (!protocol.equals(RPC.getProtocolName(DatanodeProtocolPB.class))) {
      throw new IOException("Namenode Serverside implements " +
          RPC.getProtocolName(DatanodeProtocolPB.class) +
          ". The following requested protocol is unknown: " + protocol);
    }

    return ProtocolSignature.getProtocolSignature(clientMethodsHash,
        RPC.getProtocolVersion(DatanodeProtocolPB.class),
        DatanodeProtocolPB.class);
  }

  @Override
  public ProtocolSignatureWritable getProtocolSignature2(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link DatanodeProtocolPB}
     */
    return ProtocolSignatureWritable.convert(
        this.getProtocolSignature(protocol, clientVersion, clientMethodsHash));
  }

}
