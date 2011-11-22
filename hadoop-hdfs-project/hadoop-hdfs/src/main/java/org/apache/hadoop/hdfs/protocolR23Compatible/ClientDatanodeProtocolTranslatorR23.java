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

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;


/**
 * This class forwards ClientDatanodeProtocol calls as RPC to the DN server
 * while translating from the parameter types used in ClientDatanodeProtocol to
 * those used in protocolR23Compatile.*.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ClientDatanodeProtocolTranslatorR23 implements 
  ClientDatanodeProtocol {
  
  final private ClientDatanodeWireProtocol rpcProxy;
  
  public ClientDatanodeProtocolTranslatorR23(DatanodeID datanodeid,
      Configuration conf, int socketTimeout, LocatedBlock locatedBlock)
      throws IOException {
    rpcProxy = createClientDatanodeProtocolProxy( datanodeid, conf, 
                  socketTimeout, locatedBlock);
  }
  
  /** used for testing */
  public ClientDatanodeProtocolTranslatorR23(InetSocketAddress addr,
      UserGroupInformation ticket,
      Configuration conf,
      SocketFactory factory) throws IOException {
    rpcProxy = createClientDatanodeProtocolProxy(addr, ticket, conf, factory);
  }
  
  /**
   * Constructor.
   * @param datanodeid Datanode to connect to.
   * @param conf Configuration.
   * @param socketTimeout Socket timeout to use.
   * @throws IOException
   */
  public ClientDatanodeProtocolTranslatorR23(DatanodeID datanodeid,
      Configuration conf, int socketTimeout) throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(datanodeid.getHost()
        + ":" + datanodeid.getIpcPort());
    rpcProxy = RPC.getProxy(ClientDatanodeWireProtocol.class,
        ClientDatanodeWireProtocol.versionID, addr,
        UserGroupInformation.getCurrentUser(), conf,
        NetUtils.getDefaultSocketFactory(conf), socketTimeout);
  }

  static ClientDatanodeWireProtocol createClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout,
      LocatedBlock locatedBlock)
      throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(
      datanodeid.getHost() + ":" + datanodeid.getIpcPort());
    if (ClientDatanodeWireProtocol.LOG.isDebugEnabled()) {
      ClientDatanodeWireProtocol.LOG.debug(
          "ClientDatanodeProtocol addr=" + addr);
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
    return RPC.getProxy(ClientDatanodeWireProtocol.class,
      ClientDatanodeWireProtocol.versionID, addr, ticket, confWithNoIpcIdle,
        NetUtils.getDefaultSocketFactory(conf), socketTimeout);
  }
  
  static ClientDatanodeWireProtocol createClientDatanodeProtocolProxy(
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory) throws IOException {
    return RPC.getProxy(ClientDatanodeWireProtocol.class,
        ClientDatanodeWireProtocol.versionID, addr, ticket, conf,
        factory);
  }

  @Override
  public ProtocolSignature getProtocolSignature(
      String protocolName, long clientVersion, int clientMethodHash)
      throws IOException {
    return ProtocolSignatureWritable.convert(
        rpcProxy.getProtocolSignature2(
            protocolName, clientVersion, clientMethodHash));
  }

  @Override
  public long getProtocolVersion(String protocolName, long clientVersion)
      throws IOException {
    return rpcProxy.getProtocolVersion(protocolName, clientVersion);
  }

  @Override
  public long getReplicaVisibleLength(ExtendedBlock b) throws IOException {
    return rpcProxy.getReplicaVisibleLength(
        ExtendedBlockWritable.convertExtendedBlock(b));
  }

  @Override
  public void refreshNamenodes() throws IOException {
    rpcProxy.refreshNamenodes();

  }

  @Override
  public void deleteBlockPool(String bpid, boolean force) throws IOException {
    rpcProxy.deleteBlockPool(bpid, force);

  }

  @Override
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block,
      Token<BlockTokenIdentifier> token) throws IOException {
    return rpcProxy.getBlockLocalPathInfo(block, token);
  }
}
