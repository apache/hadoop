/*
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

package org.apache.slider.server.appmaster.rpc;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.api.proto.Messages;

import java.io.IOException;

/**
 * Server-side Relay from Protobuf to internal RPC.
 *
 */
public class SliderClusterProtocolPBImpl implements SliderClusterProtocolPB {

  private SliderClusterProtocol real;

  public SliderClusterProtocolPBImpl(SliderClusterProtocol real) {
    this.real = real;
  }

  private ServiceException wrap(Exception e) {
    if (e instanceof ServiceException) {
      return (ServiceException) e;
    }
    return new ServiceException(e);
  }

  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return SliderClusterProtocol.versionID;
  }

  @Override
  public Messages.StopClusterResponseProto stopCluster(RpcController controller,
       Messages.StopClusterRequestProto request) throws ServiceException {
    try {
      return real.stopCluster(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.UpgradeContainersResponseProto upgradeContainers(RpcController controller,
        Messages.UpgradeContainersRequestProto request) throws ServiceException {
    try {
      return real.upgradeContainers(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.FlexClusterResponseProto flexCluster(RpcController controller,
      Messages.FlexClusterRequestProto request) throws ServiceException {
    try {
      return real.flexCluster(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.GetJSONClusterStatusResponseProto getJSONClusterStatus(
    RpcController controller,
    Messages.GetJSONClusterStatusRequestProto request) throws ServiceException {
    try {
      return real.getJSONClusterStatus(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }


  @Override
  public Messages.GetInstanceDefinitionResponseProto getInstanceDefinition(
    RpcController controller,
    Messages.GetInstanceDefinitionRequestProto request)
      throws ServiceException {
    try {
      return real.getInstanceDefinition(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.ListNodeUUIDsByRoleResponseProto listNodeUUIDsByRole(
    RpcController controller,
    Messages.ListNodeUUIDsByRoleRequestProto request) throws ServiceException {
    try {
      return real.listNodeUUIDsByRole(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.GetNodeResponseProto getNode(RpcController controller,
      Messages.GetNodeRequestProto request) throws ServiceException {
    try {
      return real.getNode(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.GetClusterNodesResponseProto getClusterNodes(RpcController controller,
      Messages.GetClusterNodesRequestProto request) throws ServiceException {
    try {
      return real.getClusterNodes(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.EchoResponseProto echo(RpcController controller,
      Messages.EchoRequestProto request) throws ServiceException {
    try {
      return real.echo(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.KillContainerResponseProto killContainer(RpcController controller,
      Messages.KillContainerRequestProto request) throws ServiceException {
    try {
      return real.killContainer(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }
  
  @Override
  public Messages.AMSuicideResponseProto amSuicide(RpcController controller,
      Messages.AMSuicideRequestProto request) throws ServiceException {
    try {
      return real.amSuicide(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.ApplicationLivenessInformationProto getLivenessInformation(
      RpcController controller,
      Messages.GetApplicationLivenessRequestProto request) throws ServiceException {
    try {
      return real.getLivenessInformation(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }
  
  @Override
  public Messages.GetLiveContainersResponseProto getLiveContainers(RpcController controller,
      Messages.GetLiveContainersRequestProto request) throws ServiceException {
    try {
      return real.getLiveContainers(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.ContainerInformationProto getLiveContainer(RpcController controller,
      Messages.GetLiveContainerRequestProto request) throws ServiceException {
    try {
      return real.getLiveContainer(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.GetLiveComponentsResponseProto getLiveComponents(RpcController controller,
      Messages.GetLiveComponentsRequestProto request) throws ServiceException {
    try {
      return real.getLiveComponents(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.ComponentInformationProto getLiveComponent(RpcController controller,
      Messages.GetLiveComponentRequestProto request) throws ServiceException {
    try {
      return real.getLiveComponent(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.GetLiveNodesResponseProto getLiveNodes(RpcController controller,
      Messages.GetLiveNodesRequestProto request) throws ServiceException {
    try {
      return real.getLiveNodes(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.NodeInformationProto getLiveNode(RpcController controller,
      Messages.GetLiveNodeRequestProto request) throws ServiceException {
    try {
      return real.getLiveNode(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.WrappedJsonProto getModelDesired(RpcController controller,
      Messages.EmptyPayloadProto request) throws ServiceException {
    try {
      return real.getModelDesired(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.WrappedJsonProto getModelDesiredAppconf(RpcController controller,
      Messages.EmptyPayloadProto request) throws ServiceException {
    try {
      return real.getModelDesiredAppconf(request);
    } catch (Exception e) {
      throw wrap(e);
    }  }

  @Override
  public Messages.WrappedJsonProto getModelDesiredResources(RpcController controller,
      Messages.EmptyPayloadProto request) throws ServiceException {
    try {
      return real.getModelDesiredResources(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.WrappedJsonProto getModelResolved(RpcController controller,
      Messages.EmptyPayloadProto request) throws ServiceException {
    try {
      return real.getModelResolved(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.WrappedJsonProto getModelResolvedAppconf(RpcController controller,
      Messages.EmptyPayloadProto request) throws ServiceException {
    try {
      return real.getModelResolvedAppconf(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.WrappedJsonProto getModelResolvedResources(RpcController controller,
      Messages.EmptyPayloadProto request) throws ServiceException {
    try {
      return real.getModelResolvedResources(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }

  @Override
  public Messages.WrappedJsonProto getLiveResources(RpcController controller,
      Messages.EmptyPayloadProto request) throws ServiceException {
    try {
      return real.getLiveResources(request);
    } catch (Exception e) {
      throw wrap(e);
    }
  }
}
