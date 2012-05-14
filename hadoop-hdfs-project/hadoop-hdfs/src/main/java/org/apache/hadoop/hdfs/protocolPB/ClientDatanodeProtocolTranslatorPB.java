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

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DeleteBlockPoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBlockLocalPathInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBlockLocalPathInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.RefreshNamenodesRequestProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

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
  public static final Log LOG = LogFactory
      .getLog(ClientDatanodeProtocolTranslatorPB.class);
  
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final ClientDatanodeProtocolPB rpcProxy;
  private final static RefreshNamenodesRequestProto REFRESH_NAMENODES = 
      RefreshNamenodesRequestProto.newBuilder().build();

  public ClientDatanodeProtocolTranslatorPB(DatanodeID datanodeid,
      Configuration conf, int socketTimeout, LocatedBlock locatedBlock)
      throws IOException {
    rpcProxy = createClientDatanodeProtocolProxy( datanodeid, conf, 
                  socketTimeout, locatedBlock);
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
   * @throws IOException
   */
  public ClientDatanodeProtocolTranslatorPB(DatanodeID datanodeid,
      Configuration conf, int socketTimeout) throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(datanodeid.getIpcAddr());
    rpcProxy = createClientDatanodeProtocolProxy(addr,
        UserGroupInformation.getCurrentUser(), conf,
        NetUtils.getDefaultSocketFactory(conf), socketTimeout);
  }

  static ClientDatanodeProtocolPB createClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout,
      LocatedBlock locatedBlock) throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(datanodeid.getIpcAddr());
    if (LOG.isDebugEnabled()) {
      LOG.debug("ClientDatanodeProtocol addr=" + addr);
    }
    
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
        ProtobufRpcEngine.class);
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
    GetReplicaVisibleLengthRequestProto req = GetReplicaVisibleLengthRequestProto
        .newBuilder().setBlock(PBHelper.convert(b)).build();
    try {
      return rpcProxy.getReplicaVisibleLength(NULL_CONTROLLER, req).getLength();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void refreshNamenodes() throws IOException {
    try {
      rpcProxy.refreshNamenodes(NULL_CONTROLLER, REFRESH_NAMENODES);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void deleteBlockPool(String bpid, boolean force) throws IOException {
    DeleteBlockPoolRequestProto req = DeleteBlockPoolRequestProto.newBuilder()
        .setBlockPool(bpid).setForce(force).build();
    try {
      rpcProxy.deleteBlockPool(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block,
      Token<BlockTokenIdentifier> token) throws IOException {
    GetBlockLocalPathInfoRequestProto req =
        GetBlockLocalPathInfoRequestProto.newBuilder()
        .setBlock(PBHelper.convert(block))
        .setToken(PBHelper.convert(token)).build();
    GetBlockLocalPathInfoResponseProto resp;
    try {
      resp = rpcProxy.getBlockLocalPathInfo(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    return new BlockLocalPathInfo(PBHelper.convert(resp.getBlock()),
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
}