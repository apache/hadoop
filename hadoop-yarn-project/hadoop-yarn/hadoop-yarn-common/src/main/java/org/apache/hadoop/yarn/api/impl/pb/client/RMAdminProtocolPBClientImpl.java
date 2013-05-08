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

package org.apache.hadoop.yarn.api.impl.pb.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.RMAdminProtocol;
import org.apache.hadoop.yarn.api.RMAdminProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RefreshAdminAclsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RefreshAdminAclsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RefreshNodesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RefreshNodesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RefreshQueuesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RefreshQueuesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RefreshServiceAclsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RefreshServiceAclsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RefreshSuperUserGroupsConfigurationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RefreshSuperUserGroupsConfigurationResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RefreshUserToGroupsMappingsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RefreshUserToGroupsMappingsResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetGroupsForUserRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetGroupsForUserResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshAdminAclsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsRequestProto;

import com.google.protobuf.ServiceException;


public class RMAdminProtocolPBClientImpl implements RMAdminProtocol, Closeable {

  private RMAdminProtocolPB proxy;
  
  public RMAdminProtocolPBClientImpl(long clientVersion, InetSocketAddress addr, 
      Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, RMAdminProtocolPB.class, 
        ProtobufRpcEngine.class);
    proxy = (RMAdminProtocolPB)RPC.getProxy(
        RMAdminProtocolPB.class, clientVersion, addr, conf);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws YarnRemoteException, IOException {
    RefreshQueuesRequestProto requestProto = 
      ((RefreshQueuesRequestPBImpl)request).getProto();
    try {
      return new RefreshQueuesResponsePBImpl(
          proxy.refreshQueues(null, requestProto));
    } catch (ServiceException e) {
      throw RPCUtil.unwrapAndThrowException(e);
    }
  }

  @Override
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
  throws YarnRemoteException, IOException {
    RefreshNodesRequestProto requestProto = 
      ((RefreshNodesRequestPBImpl)request).getProto();
    try {
      return new RefreshNodesResponsePBImpl(
          proxy.refreshNodes(null, requestProto));
    } catch (ServiceException e) {
      throw RPCUtil.unwrapAndThrowException(e);
    }
  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
      throws YarnRemoteException, IOException {
    RefreshSuperUserGroupsConfigurationRequestProto requestProto = 
      ((RefreshSuperUserGroupsConfigurationRequestPBImpl)request).getProto();
    try {
      return new RefreshSuperUserGroupsConfigurationResponsePBImpl(
          proxy.refreshSuperUserGroupsConfiguration(null, requestProto));
    } catch (ServiceException e) {
      throw RPCUtil.unwrapAndThrowException(e);
    }
  }

  @Override
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request) throws YarnRemoteException,
      IOException {
    RefreshUserToGroupsMappingsRequestProto requestProto = 
      ((RefreshUserToGroupsMappingsRequestPBImpl)request).getProto();
    try {
      return new RefreshUserToGroupsMappingsResponsePBImpl(
          proxy.refreshUserToGroupsMappings(null, requestProto));
    } catch (ServiceException e) {
      throw RPCUtil.unwrapAndThrowException(e);
    }
  }

  @Override
  public RefreshAdminAclsResponse refreshAdminAcls(
      RefreshAdminAclsRequest request) throws YarnRemoteException, IOException {
    RefreshAdminAclsRequestProto requestProto = 
      ((RefreshAdminAclsRequestPBImpl)request).getProto();
    try {
      return new RefreshAdminAclsResponsePBImpl(
          proxy.refreshAdminAcls(null, requestProto));
    } catch (ServiceException e) {
      throw RPCUtil.unwrapAndThrowException(e);
    }
  }

  @Override
  public RefreshServiceAclsResponse refreshServiceAcls(
      RefreshServiceAclsRequest request) throws YarnRemoteException,
      IOException {
    RefreshServiceAclsRequestProto requestProto = 
        ((RefreshServiceAclsRequestPBImpl)request).getProto();
    try {
      return new RefreshServiceAclsResponsePBImpl(proxy.refreshServiceAcls(
          null, requestProto));
    } catch (ServiceException e) {
      throw RPCUtil.unwrapAndThrowException(e);
    }
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    GetGroupsForUserRequestProto requestProto = 
        GetGroupsForUserRequestProto.newBuilder().setUser(user).build();
    try {
      GetGroupsForUserResponseProto responseProto =
          proxy.getGroupsForUser(null, requestProto);
      return (String[]) responseProto.getGroupsList().toArray(
          new String[responseProto.getGroupsCount()]);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
  
}
