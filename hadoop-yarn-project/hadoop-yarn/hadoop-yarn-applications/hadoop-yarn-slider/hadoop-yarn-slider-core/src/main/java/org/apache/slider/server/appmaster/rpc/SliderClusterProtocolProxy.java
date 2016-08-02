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

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.api.proto.Messages;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SliderClusterProtocolProxy implements SliderClusterProtocol {

  private static final RpcController NULL_CONTROLLER = null;
  private final SliderClusterProtocolPB endpoint;
  private final InetSocketAddress address;

  public SliderClusterProtocolProxy(SliderClusterProtocolPB endpoint,
      InetSocketAddress address) {
    Preconditions.checkArgument(endpoint != null, "null endpoint");
    Preconditions.checkNotNull(address != null, "null address");
    this.endpoint = endpoint;
    this.address = address;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("SliderClusterProtocolProxy{");
    sb.append("address=").append(address);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion,
      int clientMethodsHash)
      throws IOException {
    if (!protocol.equals(RPC.getProtocolName(SliderClusterProtocolPB.class))) {
      throw new IOException("Serverside implements " +
                            RPC.getProtocolName(SliderClusterProtocolPB.class) +
                            ". The following requested protocol is unknown: " +
                            protocol);
    }

    return ProtocolSignature.getProtocolSignature(clientMethodsHash,
        RPC.getProtocolVersion(
            SliderClusterProtocol.class),
        SliderClusterProtocol.class);
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return SliderClusterProtocol.versionID;
  }
  
  private IOException convert(ServiceException se) {
    IOException ioe = ProtobufHelper.getRemoteException(se);
    if (ioe instanceof RemoteException) {
      RemoteException remoteException = (RemoteException) ioe;
      return remoteException.unwrapRemoteException();
    }
    return ioe;
  }
  
  @Override
  public Messages.StopClusterResponseProto stopCluster(Messages.StopClusterRequestProto request) throws
                                                                                                 IOException,
                                                                                                 YarnException {
    try {
      return endpoint.stopCluster(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.UpgradeContainersResponseProto upgradeContainers(
      Messages.UpgradeContainersRequestProto request) throws IOException,
      YarnException {
    try {
      return endpoint.upgradeContainers(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.FlexClusterResponseProto flexCluster(Messages.FlexClusterRequestProto request)
      throws IOException {
    try {
      return endpoint.flexCluster(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.GetJSONClusterStatusResponseProto getJSONClusterStatus(
    Messages.GetJSONClusterStatusRequestProto request) throws
                                                       IOException,
                                                       YarnException {
    try {
      return endpoint.getJSONClusterStatus(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }


  @Override
  public Messages.GetInstanceDefinitionResponseProto getInstanceDefinition(
    Messages.GetInstanceDefinitionRequestProto request) throws
                                                        IOException,
                                                        YarnException {
    try {
      return endpoint.getInstanceDefinition(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.ListNodeUUIDsByRoleResponseProto listNodeUUIDsByRole(Messages.ListNodeUUIDsByRoleRequestProto request) throws
                                                                                                                         IOException,
                                                                                                                         YarnException {
    try {
      return endpoint.listNodeUUIDsByRole(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.GetNodeResponseProto getNode(Messages.GetNodeRequestProto request) throws
                                                                                     IOException,
                                                                                     YarnException {
    try {
      return endpoint.getNode(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.GetClusterNodesResponseProto getClusterNodes(Messages.GetClusterNodesRequestProto request) throws
                                                                                                             IOException,
                                                                                                             YarnException {
    try {
      return endpoint.getClusterNodes(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }


  @Override
  public Messages.EchoResponseProto echo(Messages.EchoRequestProto request) throws
                                                                                                             IOException,
                                                                                                             YarnException {
    try {
      return endpoint.echo(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }


  @Override
  public Messages.KillContainerResponseProto killContainer(Messages.KillContainerRequestProto request) throws
                                                                                                             IOException,
                                                                                                             YarnException {
    try {
      return endpoint.killContainer(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.AMSuicideResponseProto amSuicide(Messages.AMSuicideRequestProto request) throws
                                                                                           IOException {
    try {
      return endpoint.amSuicide(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.ApplicationLivenessInformationProto getLivenessInformation(
      Messages.GetApplicationLivenessRequestProto request) throws IOException {
    try {
      return endpoint.getLivenessInformation(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.GetLiveContainersResponseProto getLiveContainers(Messages.GetLiveContainersRequestProto request) throws
      IOException {
    try {
      return endpoint.getLiveContainers(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.ContainerInformationProto getLiveContainer(Messages.GetLiveContainerRequestProto request) throws
      IOException {
    try {
      return endpoint.getLiveContainer(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.GetLiveComponentsResponseProto getLiveComponents(Messages.GetLiveComponentsRequestProto request) throws
      IOException {
    try {
      return endpoint.getLiveComponents(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.ComponentInformationProto getLiveComponent(Messages.GetLiveComponentRequestProto request) throws
      IOException {
    try {
      return endpoint.getLiveComponent(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.GetLiveNodesResponseProto getLiveNodes(Messages.GetLiveNodesRequestProto request)
      throws IOException {
    try {
      return endpoint.getLiveNodes(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.NodeInformationProto getLiveNode(Messages.GetLiveNodeRequestProto request)
      throws IOException {
    try {
      return endpoint.getLiveNode(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.WrappedJsonProto getModelDesired(Messages.EmptyPayloadProto request) throws IOException {
    try {
      return endpoint.getModelDesired(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.WrappedJsonProto getModelDesiredAppconf(Messages.EmptyPayloadProto request) throws IOException {
    try {
      return endpoint.getModelDesiredAppconf(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.WrappedJsonProto getModelDesiredResources(Messages.EmptyPayloadProto request) throws IOException {
    try {
      return endpoint.getModelDesiredResources(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.WrappedJsonProto getModelResolved(Messages.EmptyPayloadProto request) throws IOException {
    try {
      return endpoint.getModelResolved(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.WrappedJsonProto getModelResolvedAppconf(Messages.EmptyPayloadProto request) throws IOException {
    try {
      return endpoint.getModelResolvedAppconf(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.WrappedJsonProto getModelResolvedResources(Messages.EmptyPayloadProto request) throws IOException {
    try {
      return endpoint.getModelResolvedResources(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }

  @Override
  public Messages.WrappedJsonProto getLiveResources(Messages.EmptyPayloadProto request) throws IOException {
    try {
      return endpoint.getLiveResources(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }

  }

  @Override
  public Messages.GetCertificateStoreResponseProto getClientCertificateStore(Messages.GetCertificateStoreRequestProto request) throws
      IOException {
    try {
      return endpoint.getClientCertificateStore(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw convert(e);
    }
  }
}
