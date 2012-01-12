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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReceivedAndDeletedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CommitBlockSynchronizationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ErrorReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ProcessUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ProcessUpgradeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.server.protocolR23Compatible.DatanodeWireProtocol;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is the client side translator to translate the requests made on
 * {@link DatanodeProtocolProtocol} interfaces to the RPC server implementing
 * {@link DatanodeProtocolProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class DatanodeProtocolClientSideTranslatorPB implements DatanodeProtocol,
    Closeable {
  
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final DatanodeProtocolPB rpcProxy;
  private static final VersionRequestProto VERSION_REQUEST = 
      VersionRequestProto.newBuilder().build();
  
  public DatanodeProtocolClientSideTranslatorPB(InetSocketAddress nameNodeAddr,
      Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, DatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    rpcProxy = createNamenodeWithRetry(createNamenode(nameNodeAddr, conf, ugi));
  }

  private static DatanodeProtocolPB createNamenode(
      InetSocketAddress nameNodeAddr, Configuration conf,
      UserGroupInformation ugi) throws IOException {
    return RPC.getProxy(DatanodeProtocolPB.class,
        RPC.getProtocolVersion(DatanodeProtocolPB.class), nameNodeAddr, ugi,
        conf, NetUtils.getSocketFactory(conf, DatanodeWireProtocol.class));
  }

  /** Create a {@link NameNode} proxy */
  static DatanodeProtocolPB createNamenodeWithRetry(
      DatanodeProtocolPB rpcNamenode) {
    RetryPolicy createPolicy = RetryPolicies
        .retryUpToMaximumCountWithFixedSleep(5,
            HdfsConstants.LEASE_SOFTLIMIT_PERIOD, TimeUnit.MILLISECONDS);

    Map<Class<? extends Exception>, RetryPolicy> remoteExceptionToPolicyMap = 
        new HashMap<Class<? extends Exception>, RetryPolicy>();
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

    return (DatanodeProtocolPB) RetryProxy.create(DatanodeProtocolPB.class,
        rpcNamenode, methodNameToPolicyMap);
  }
  
  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return rpcProxy.getProtocolVersion(protocol, clientVersion);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocolName,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignatureWritable.convert(rpcProxy.getProtocolSignature2(
        protocolName, clientVersion, clientMethodsHash));
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public DatanodeRegistration registerDatanode(DatanodeRegistration registration)
      throws IOException {
    RegisterDatanodeRequestProto req = RegisterDatanodeRequestProto
        .newBuilder().setRegistration(PBHelper.convert(registration)).build();
    RegisterDatanodeResponseProto resp;
    try {
      resp = rpcProxy.registerDatanode(NULL_CONTROLLER, req);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
    return PBHelper.convert(resp.getRegistration());
  }

  @Override
  public DatanodeCommand[] sendHeartbeat(DatanodeRegistration registration,
      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
      int xmitsInProgress, int xceiverCount, int failedVolumes)
      throws IOException {
    HeartbeatRequestProto req = HeartbeatRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration)).setCapacity(capacity)
        .setCapacity(dfsUsed).setRemaining(remaining)
        .setBlockPoolUsed(blockPoolUsed).setXmitsInProgress(xmitsInProgress)
        .setXceiverCount(xceiverCount).setFailedVolumes(failedVolumes).build();
    HeartbeatResponseProto resp;
    try {
      resp = rpcProxy.sendHeartbeat(NULL_CONTROLLER, req);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
    DatanodeCommand[] cmds = new DatanodeCommand[resp.getCmdsList().size()];
    int index = 0;
    for (DatanodeCommandProto p : resp.getCmdsList()) {
      cmds[index] = PBHelper.convert(p);
      index++;
    }
    return cmds;
  }

  @Override
  public DatanodeCommand blockReport(DatanodeRegistration registration,
      String poolId, long[] blocks) throws IOException {
    BlockReportRequestProto.Builder builder = BlockReportRequestProto
        .newBuilder().setRegistration(PBHelper.convert(registration))
        .setBlockPoolId(poolId);
    if (blocks != null) {
      for (int i = 0; i < blocks.length; i++) {
        builder.setBlocks(i, blocks[i]);
      }
    }
    BlockReportRequestProto req = builder.build();
    BlockReportResponseProto resp;
    try {
      resp = rpcProxy.blockReport(NULL_CONTROLLER, req);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
    return PBHelper.convert(resp.getCmd());
  }

  @Override
  public void blockReceivedAndDeleted(DatanodeRegistration registration,
      String poolId, ReceivedDeletedBlockInfo[] receivedAndDeletedBlocks)
      throws IOException {
    BlockReceivedAndDeletedRequestProto.Builder builder = 
        BlockReceivedAndDeletedRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration))
        .setBlockPoolId(poolId);
    if (receivedAndDeletedBlocks != null) {
      for (int i = 0; i < receivedAndDeletedBlocks.length; i++) {
        builder.setBlocks(i, PBHelper.convert(receivedAndDeletedBlocks[i]));
      }
    }
    BlockReceivedAndDeletedRequestProto req = builder.build();
    try {
      rpcProxy.blockReceivedAndDeleted(NULL_CONTROLLER, req);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public void errorReport(DatanodeRegistration registration, int errorCode,
      String msg) throws IOException {
    ErrorReportRequestProto req = ErrorReportRequestProto.newBuilder()
        .setRegistartion(PBHelper.convert(registration))
        .setErrorCode(errorCode).setMsg(msg).build();
    try {
      rpcProxy.errorReport(NULL_CONTROLLER, req);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public NamespaceInfo versionRequest() throws IOException {
    try {
      return PBHelper.convert(rpcProxy.versionRequest(NULL_CONTROLLER,
          VERSION_REQUEST).getInfo());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public UpgradeCommand processUpgradeCommand(UpgradeCommand comm)
      throws IOException {
    ProcessUpgradeRequestProto req = ProcessUpgradeRequestProto.newBuilder()
        .setCmd(PBHelper.convert(comm)).build();
    ProcessUpgradeResponseProto resp;
    try {
      resp = rpcProxy.processUpgrade(NULL_CONTROLLER, req);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
    return PBHelper.convert(resp.getCmd());
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    ReportBadBlocksRequestProto.Builder builder = ReportBadBlocksRequestProto
        .newBuilder();
    for (int i = 0; i < blocks.length; i++) {
      builder.addBlocks(i, PBHelper.convert(blocks[i]));
    }
    ReportBadBlocksRequestProto req = builder.build();
    try {
      rpcProxy.reportBadBlocks(NULL_CONTROLLER, req);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public void commitBlockSynchronization(ExtendedBlock block,
      long newgenerationstamp, long newlength, boolean closeFile,
      boolean deleteblock, DatanodeID[] newtargets) throws IOException {
    CommitBlockSynchronizationRequestProto.Builder builder = 
        CommitBlockSynchronizationRequestProto.newBuilder()
        .setBlock(PBHelper.convert(block)).setNewGenStamp(newgenerationstamp)
        .setNewLength(newlength).setCloseFile(closeFile)
        .setDeleteBlock(deleteblock);
    for (int i = 0; i < newtargets.length; i++) {
      builder.setNewTaragets(i, PBHelper.convert(newtargets[i]));
    }
    CommitBlockSynchronizationRequestProto req = builder.build();
    try {
      rpcProxy.commitBlockSynchronization(NULL_CONTROLLER, req);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }
}
