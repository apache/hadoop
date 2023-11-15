
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
import java.util.ArrayList;
import java.util.List;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeVolumeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DeleteBlockPoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.EvictWritersRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBalancerBandwidthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBalancerBandwidthResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBlockLocalPathInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBlockLocalPathInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetDatanodeInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetDatanodeInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetVolumeReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetVolumeReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeVolumeInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ListReconfigurablePropertiesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ListReconfigurablePropertiesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.RefreshNamenodesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ShutdownDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.StartReconfigurationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.TriggerBlockReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.SubmitDiskBalancerPlanRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.CancelPlanRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.QueryPlanStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.QueryPlanStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DiskBalancerSettingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DiskBalancerSettingResponseProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus.Result;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.thirdparty.protobuf.RpcController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

/**
 * This class is the client side translator to translate the requests made on
 * {@link ClientDatanodeProtocol} interfaces to the RPC server implementing
 * {@link ClientDatanodeProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ClientDatanodeProtocolTranslatorPB implements
    ProtocolMetaInterface, ClientDatanodeProtocol,
    ProtocolTranslator, Closeable {
  public static final Logger LOG = LoggerFactory
      .getLogger(ClientDatanodeProtocolTranslatorPB.class);

  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final ClientDatanodeProtocolPB rpcProxy;
  private final static RefreshNamenodesRequestProto VOID_REFRESH_NAMENODES =
      RefreshNamenodesRequestProto.newBuilder().build();
  private final static GetDatanodeInfoRequestProto VOID_GET_DATANODE_INFO =
      GetDatanodeInfoRequestProto.newBuilder().build();
  private final static GetVolumeReportRequestProto
      VOID_GET_DATANODE_STORAGE_INFO =
      GetVolumeReportRequestProto.newBuilder().build();
  private final static GetReconfigurationStatusRequestProto VOID_GET_RECONFIG_STATUS =
      GetReconfigurationStatusRequestProto.newBuilder().build();
  private final static StartReconfigurationRequestProto VOID_START_RECONFIG =
      StartReconfigurationRequestProto.newBuilder().build();
  private static final ListReconfigurablePropertiesRequestProto
      VOID_LIST_RECONFIGURABLE_PROPERTIES =
      ListReconfigurablePropertiesRequestProto.newBuilder().build();
  private static final GetBalancerBandwidthRequestProto
      VOID_GET_BALANCER_BANDWIDTH =
      GetBalancerBandwidthRequestProto.newBuilder().build();
  private final static EvictWritersRequestProto VOID_EVICT_WRITERS =
      EvictWritersRequestProto.newBuilder().build();

  public ClientDatanodeProtocolTranslatorPB(DatanodeID datanodeid,
      Configuration conf, int socketTimeout, boolean connectToDnViaHostname,
      LocatedBlock locatedBlock) throws IOException {
    rpcProxy = createClientDatanodeProtocolProxy( datanodeid, conf,
        socketTimeout, connectToDnViaHostname, locatedBlock);
  }

  public ClientDatanodeProtocolTranslatorPB(InetSocketAddress addr,
      UserGroupInformation ticket, Configuration conf, SocketFactory factory)
      throws IOException {
    rpcProxy = createClientDatanodeProtocolProxy(addr, ticket, conf, factory, 0);
  }

  /**
   * Constructor.
   * @param datanodeid Datanode to connect to.
   * @param conf Configuration.
   * @param socketTimeout Socket timeout to use.
   * @param connectToDnViaHostname connect to the Datanode using its hostname
   * @throws IOException
   */
  public ClientDatanodeProtocolTranslatorPB(DatanodeID datanodeid,
      Configuration conf, int socketTimeout, boolean connectToDnViaHostname)
      throws IOException {
    final String dnAddr = datanodeid.getIpcAddr(connectToDnViaHostname);
    InetSocketAddress addr = NetUtils.createSocketAddr(dnAddr);
    LOG.debug("Connecting to datanode {} addr={}", dnAddr, addr);
    rpcProxy = createClientDatanodeProtocolProxy(addr,
        UserGroupInformation.getCurrentUser(), conf,
        NetUtils.getDefaultSocketFactory(conf), socketTimeout);
  }

  static ClientDatanodeProtocolPB createClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout,
      boolean connectToDnViaHostname, LocatedBlock locatedBlock)
      throws IOException {
    final String dnAddr = datanodeid.getIpcAddr(connectToDnViaHostname);
    InetSocketAddress addr = NetUtils.createSocketAddr(dnAddr);
    LOG.debug("Connecting to datanode {} addr={}", dnAddr, addr);

    // Since we're creating a new UserGroupInformation here, we know that no
    // future RPC proxies will be able to re-use the same connection. And
    // usages of this proxy tend to be one-off calls.
    //
    // This is a temporary fix: callers should really achieve this by using
    // RPC.stopProxy() on the resulting object, but this is currently not
    // working in trunk. See the discussion on HDFS-1965.
    Configuration confWithNoIpcIdle = new Configuration(conf);
    confWithNoIpcIdle.setInt(CommonConfigurationKeysPublic
        .IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY, 0);

    UserGroupInformation ticket = UserGroupInformation
        .createRemoteUser(locatedBlock.getBlock().getLocalBlock().toString());
    ticket.addToken(locatedBlock.getBlockToken());
    return createClientDatanodeProtocolProxy(addr, ticket, confWithNoIpcIdle,
        NetUtils.getDefaultSocketFactory(conf), socketTimeout);
  }

  static ClientDatanodeProtocolPB createClientDatanodeProtocolProxy(
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int socketTimeout) throws IOException {
    RPC.setProtocolEngine(conf, ClientDatanodeProtocolPB.class,
        ProtobufRpcEngine2.class);
    return RPC.getProxy(ClientDatanodeProtocolPB.class,
        RPC.getProtocolVersion(ClientDatanodeProtocolPB.class), addr, ticket,
        conf, factory, socketTimeout);
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public long getReplicaVisibleLength(ExtendedBlock b) throws IOException {
    GetReplicaVisibleLengthRequestProto req =
        GetReplicaVisibleLengthRequestProto.newBuilder()
            .setBlock(PBHelperClient.convert(b)).build();
    return ipc(() -> rpcProxy.getReplicaVisibleLength(NULL_CONTROLLER, req).getLength());
  }

  @Override
  public void refreshNamenodes() throws IOException {
    ipc(() -> rpcProxy.refreshNamenodes(NULL_CONTROLLER, VOID_REFRESH_NAMENODES));
  }

  @Override
  public void deleteBlockPool(String bpid, boolean force) throws IOException {
    DeleteBlockPoolRequestProto req = DeleteBlockPoolRequestProto.newBuilder()
        .setBlockPool(bpid).setForce(force).build();
    ipc(() -> rpcProxy.deleteBlockPool(NULL_CONTROLLER, req));
  }

  @Override
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block,
      Token<BlockTokenIdentifier> token) throws IOException {
    GetBlockLocalPathInfoRequestProto req =
        GetBlockLocalPathInfoRequestProto.newBuilder()
            .setBlock(PBHelperClient.convert(block))
            .setToken(PBHelperClient.convert(token)).build();
    GetBlockLocalPathInfoResponseProto resp;
    resp = ipc(() -> rpcProxy.getBlockLocalPathInfo(NULL_CONTROLLER, req));
    return new BlockLocalPathInfo(PBHelperClient.convert(resp.getBlock()),
        resp.getLocalPath(), resp.getLocalMetaPath());
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        ClientDatanodeProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(ClientDatanodeProtocolPB.class), methodName);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public void shutdownDatanode(boolean forUpgrade) throws IOException {
    ShutdownDatanodeRequestProto request = ShutdownDatanodeRequestProto
        .newBuilder().setForUpgrade(forUpgrade).build();
    ipc(() -> rpcProxy.shutdownDatanode(NULL_CONTROLLER, request));
  }

  @Override
  public void evictWriters() throws IOException {
    ipc(() -> rpcProxy.evictWriters(NULL_CONTROLLER, VOID_EVICT_WRITERS));
  }

  @Override
  public DatanodeLocalInfo getDatanodeInfo() throws IOException {
    GetDatanodeInfoResponseProto response;
    response = ipc(() -> rpcProxy.getDatanodeInfo(NULL_CONTROLLER,
        VOID_GET_DATANODE_INFO));
    return PBHelperClient.convert(response.getLocalInfo());
  }

  @Override
  public void startReconfiguration() throws IOException {
    ipc(() -> rpcProxy.startReconfiguration(NULL_CONTROLLER, VOID_START_RECONFIG));
  }

  @Override
  public ReconfigurationTaskStatus getReconfigurationStatus()
      throws IOException {
    return ReconfigurationProtocolUtils.getReconfigurationStatus(
        ipc(() -> rpcProxy.getReconfigurationStatus(
            NULL_CONTROLLER,
            VOID_GET_RECONFIG_STATUS)));
  }

  @Override
  public List<String> listReconfigurableProperties() throws IOException {
    ListReconfigurablePropertiesResponseProto response;
    response = ipc(() -> rpcProxy.listReconfigurableProperties(NULL_CONTROLLER,
        VOID_LIST_RECONFIGURABLE_PROPERTIES));
    return response.getNameList();
  }

  @Override
  public void triggerBlockReport(BlockReportOptions options)
      throws IOException {
    TriggerBlockReportRequestProto.Builder builder = TriggerBlockReportRequestProto.newBuilder().
        setIncremental(options.isIncremental());
    if (options.getNamenodeAddr() != null) {
      builder.setNnAddress(NetUtils.getHostPortString(options.getNamenodeAddr()));
    }
    ipc(() -> rpcProxy.triggerBlockReport(NULL_CONTROLLER, builder.build()));
  }

  @Override
  public long getBalancerBandwidth() throws IOException {
    GetBalancerBandwidthResponseProto response;
    response = ipc(() -> rpcProxy.getBalancerBandwidth(NULL_CONTROLLER,
        VOID_GET_BALANCER_BANDWIDTH));
    return response.getBandwidth();
  }

  /**
   * Submits a disk balancer plan to the datanode.
   * @param planID - Plan ID is the hash512 string of the plan that is
   *               submitted. This is used by clients when they want to find
   *               local copies of these plans.
   * @param planVersion - The data format of the plans - for future , not
   *                    used now.
   * @param planFile - Plan file name
   * @param planData - Actual plan data in json format
   * @param skipDateCheck - Skips the date check.
   * @throws IOException
   */
  @Override
  public void submitDiskBalancerPlan(String planID, long planVersion,
        String planFile, String planData, boolean skipDateCheck)
      throws IOException {
    SubmitDiskBalancerPlanRequestProto request =
        SubmitDiskBalancerPlanRequestProto.newBuilder()
            .setPlanID(planID)
            .setPlanVersion(planVersion)
            .setPlanFile(planFile)
            .setPlan(planData)
            .setIgnoreDateCheck(skipDateCheck)
            .build();
    ipc(() -> rpcProxy.submitDiskBalancerPlan(NULL_CONTROLLER, request));
  }

  /**
   * Cancels an executing disk balancer plan.
   *
   * @param planID - A SHA-1 hash of the plan string.
   * @throws IOException on error
   */
  @Override
  public void cancelDiskBalancePlan(String planID)
      throws IOException {
    CancelPlanRequestProto request = CancelPlanRequestProto.newBuilder()
        .setPlanID(planID).build();
    ipc(() -> rpcProxy.cancelDiskBalancerPlan(NULL_CONTROLLER, request));
  }

  /**
   * Gets the status of an executing diskbalancer Plan.
   */
  @Override
  public DiskBalancerWorkStatus queryDiskBalancerPlan() throws IOException {
    QueryPlanStatusRequestProto request =
        QueryPlanStatusRequestProto.newBuilder().build();
    QueryPlanStatusResponseProto response =
        ipc(() -> rpcProxy.queryDiskBalancerPlan(NULL_CONTROLLER, request));
    DiskBalancerWorkStatus.Result result = Result.NO_PLAN;
    if(response.hasResult()) {
      result = DiskBalancerWorkStatus.Result.values()[
          response.getResult()];
    }

    return new DiskBalancerWorkStatus(result,
        response.hasPlanID() ? response.getPlanID() : null,
        response.hasPlanFile() ? response.getPlanFile() : null,
        response.hasCurrentStatus() ? response.getCurrentStatus() : null);
  }

  @Override
  public String getDiskBalancerSetting(String key) throws IOException {
    DiskBalancerSettingRequestProto request =
        DiskBalancerSettingRequestProto.newBuilder().setKey(key).build();
    DiskBalancerSettingResponseProto response =
        ipc(() -> rpcProxy.getDiskBalancerSetting(NULL_CONTROLLER, request));
    return response.hasValue() ? response.getValue() : null;
  }

  @Override
  public List<DatanodeVolumeInfo> getVolumeReport() throws IOException {
    List<DatanodeVolumeInfo> volumeInfoList = new ArrayList<>();
    GetVolumeReportResponseProto volumeReport = ipc(() -> rpcProxy.getVolumeReport(
        NULL_CONTROLLER, VOID_GET_DATANODE_STORAGE_INFO));
    List<DatanodeVolumeInfoProto> volumeProtoList = volumeReport
        .getVolumeInfoList();
    for (DatanodeVolumeInfoProto proto : volumeProtoList) {
      volumeInfoList.add(new DatanodeVolumeInfo(proto.getPath(), proto
          .getUsedSpace(), proto.getFreeSpace(), proto.getReservedSpace(),
          proto.getReservedSpaceForReplicas(), proto.getNumBlocks(),
          PBHelperClient.convertStorageType(proto.getStorageType())));
    }
    return volumeInfoList;
  }
}
