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

package org.apache.hadoop.yarn.server.api.impl.pb.service;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetGroupsForUserRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetGroupsForUserResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.NodesToAttributesMappingRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.NodesToAttributesMappingResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshAdminAclsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshAdminAclsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshClusterMaxPriorityRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshClusterMaxPriorityResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesResourcesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesResourcesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshQueuesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshServiceAclsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.CheckForDecommissioningNodesRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.CheckForDecommissioningNodesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodesToAttributesMappingRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodesToAttributesMappingResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshAdminAclsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshAdminAclsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshClusterMaxPriorityRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshClusterMaxPriorityResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesResourcesRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesResourcesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshQueuesRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshQueuesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshServiceAclsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshServiceAclsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshSuperUserGroupsConfigurationRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshSuperUserGroupsConfigurationResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshUserToGroupsMappingsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshUserToGroupsMappingsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveFromClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveFromClusterNodeLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UpdateNodeResourceRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UpdateNodeResourceResponsePBImpl;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

@Private
public class ResourceManagerAdministrationProtocolPBServiceImpl implements ResourceManagerAdministrationProtocolPB {

  private ResourceManagerAdministrationProtocol real;
  
  public ResourceManagerAdministrationProtocolPBServiceImpl(ResourceManagerAdministrationProtocol impl) {
    this.real = impl;
  }
  
  @Override
  public RefreshQueuesResponseProto refreshQueues(RpcController controller,
      RefreshQueuesRequestProto proto) throws ServiceException {
    RefreshQueuesRequestPBImpl request = new RefreshQueuesRequestPBImpl(proto);
    try {
      RefreshQueuesResponse response = real.refreshQueues(request);
      return ((RefreshQueuesResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RefreshAdminAclsResponseProto refreshAdminAcls(
      RpcController controller, RefreshAdminAclsRequestProto proto)
      throws ServiceException {
    RefreshAdminAclsRequestPBImpl request = 
      new RefreshAdminAclsRequestPBImpl(proto);
    try {
      RefreshAdminAclsResponse response = real.refreshAdminAcls(request);
      return ((RefreshAdminAclsResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RefreshNodesResponseProto refreshNodes(RpcController controller,
      RefreshNodesRequestProto proto) throws ServiceException {
    RefreshNodesRequestPBImpl request = new RefreshNodesRequestPBImpl(proto);
    try {
      RefreshNodesResponse response = real.refreshNodes(request);
      return ((RefreshNodesResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponseProto 
  refreshSuperUserGroupsConfiguration(
      RpcController controller,
      RefreshSuperUserGroupsConfigurationRequestProto proto)
      throws ServiceException {
    RefreshSuperUserGroupsConfigurationRequestPBImpl request = 
      new RefreshSuperUserGroupsConfigurationRequestPBImpl(proto);
    try {
      RefreshSuperUserGroupsConfigurationResponse response = 
        real.refreshSuperUserGroupsConfiguration(request);
      return ((RefreshSuperUserGroupsConfigurationResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RefreshUserToGroupsMappingsResponseProto refreshUserToGroupsMappings(
      RpcController controller, RefreshUserToGroupsMappingsRequestProto proto)
      throws ServiceException {
    RefreshUserToGroupsMappingsRequestPBImpl request = 
      new RefreshUserToGroupsMappingsRequestPBImpl(proto);
    try {
      RefreshUserToGroupsMappingsResponse response = 
        real.refreshUserToGroupsMappings(request);
      return ((RefreshUserToGroupsMappingsResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RefreshServiceAclsResponseProto refreshServiceAcls(
      RpcController controller, RefreshServiceAclsRequestProto proto)
      throws ServiceException {
    RefreshServiceAclsRequestPBImpl request = 
        new RefreshServiceAclsRequestPBImpl(proto);
      try {
        RefreshServiceAclsResponse response = 
          real.refreshServiceAcls(request);
        return ((RefreshServiceAclsResponsePBImpl)response).getProto();
      } catch (YarnException e) {
        throw new ServiceException(e);
      } catch (IOException e) {
        throw new ServiceException(e);
      }
  }

  @Override
  public GetGroupsForUserResponseProto getGroupsForUser(
      RpcController controller, GetGroupsForUserRequestProto request)
      throws ServiceException {
    String user = request.getUser();
    try {
      String[] groups = real.getGroupsForUser(user);
      GetGroupsForUserResponseProto.Builder responseBuilder =
          GetGroupsForUserResponseProto.newBuilder();
      for (String group : groups) {
        responseBuilder.addGroups(group);
      }
      return responseBuilder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
  
  @Override
  public UpdateNodeResourceResponseProto updateNodeResource(RpcController controller,
      UpdateNodeResourceRequestProto proto) throws ServiceException {
    UpdateNodeResourceRequestPBImpl request = 
        new UpdateNodeResourceRequestPBImpl(proto);
    try {
      UpdateNodeResourceResponse response = real.updateNodeResource(request);
      return ((UpdateNodeResourceResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RefreshNodesResourcesResponseProto refreshNodesResources(
      RpcController controller, RefreshNodesResourcesRequestProto proto)
          throws ServiceException {
    RefreshNodesResourcesRequestPBImpl request =
        new RefreshNodesResourcesRequestPBImpl(proto);
    try {
      RefreshNodesResourcesResponse response =
          real.refreshNodesResources(request);
      return ((RefreshNodesResourcesResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public AddToClusterNodeLabelsResponseProto addToClusterNodeLabels(
      RpcController controller, AddToClusterNodeLabelsRequestProto proto)
      throws ServiceException {
    AddToClusterNodeLabelsRequestPBImpl request =
        new AddToClusterNodeLabelsRequestPBImpl(proto);
    try {
      AddToClusterNodeLabelsResponse response =
          real.addToClusterNodeLabels(request);
      return ((AddToClusterNodeLabelsResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RemoveFromClusterNodeLabelsResponseProto removeFromClusterNodeLabels(
      RpcController controller, RemoveFromClusterNodeLabelsRequestProto proto)
      throws ServiceException {
    RemoveFromClusterNodeLabelsRequestPBImpl request =
        new RemoveFromClusterNodeLabelsRequestPBImpl(proto);
    try {
      RemoveFromClusterNodeLabelsResponse response =
          real.removeFromClusterNodeLabels(request);
      return ((RemoveFromClusterNodeLabelsResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ReplaceLabelsOnNodeResponseProto replaceLabelsOnNodes(
      RpcController controller, ReplaceLabelsOnNodeRequestProto proto)
      throws ServiceException {
    ReplaceLabelsOnNodeRequestPBImpl request =
        new ReplaceLabelsOnNodeRequestPBImpl(proto);
    try {
      ReplaceLabelsOnNodeResponse response = real.replaceLabelsOnNode(request);
      return ((ReplaceLabelsOnNodeResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CheckForDecommissioningNodesResponseProto checkForDecommissioningNodes(
      RpcController controller, CheckForDecommissioningNodesRequestProto proto)
      throws ServiceException {
    CheckForDecommissioningNodesRequest request = new CheckForDecommissioningNodesRequestPBImpl(
        proto);
    try {
      CheckForDecommissioningNodesResponse response = real
          .checkForDecommissioningNodes(request);
      return ((CheckForDecommissioningNodesResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RefreshClusterMaxPriorityResponseProto refreshClusterMaxPriority(
      RpcController arg0, RefreshClusterMaxPriorityRequestProto proto)
      throws ServiceException {
    RefreshClusterMaxPriorityRequest request =
        new RefreshClusterMaxPriorityRequestPBImpl(proto);
    try {
      RefreshClusterMaxPriorityResponse response =
          real.refreshClusterMaxPriority(request);
      return ((RefreshClusterMaxPriorityResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public NodesToAttributesMappingResponseProto mapAttributesToNodes(
      RpcController controller, NodesToAttributesMappingRequestProto proto)
      throws ServiceException {
    NodesToAttributesMappingRequest request =
        new NodesToAttributesMappingRequestPBImpl(proto);
    try {
      NodesToAttributesMappingResponse response =
          real.mapAttributesToNodes(request);
      return ((NodesToAttributesMappingResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
