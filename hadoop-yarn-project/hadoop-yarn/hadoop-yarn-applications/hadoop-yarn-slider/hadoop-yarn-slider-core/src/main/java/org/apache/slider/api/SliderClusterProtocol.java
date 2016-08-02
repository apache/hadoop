/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License") throws IOException, YarnException; you may not use this file except in compliance
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

package org.apache.slider.api;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.api.proto.Messages;
import org.apache.slider.common.SliderXmlConfKeys;

import java.io.IOException;

/**
 * Cluster protocol. This can currently act as a versioned IPC
 * endpoint or be relayed via protobuf
 */
@KerberosInfo(serverPrincipal = SliderXmlConfKeys.KEY_KERBEROS_PRINCIPAL)
public interface SliderClusterProtocol extends VersionedProtocol {
  long versionID = 0x01;

  /**
   * Stop the cluster
   */

  Messages.StopClusterResponseProto stopCluster(Messages.StopClusterRequestProto request) throws
                                                                                          IOException, YarnException;
  /**
   * Upgrade the application containers
   * 
   * @param request upgrade containers request object
   * @return upgrade containers response object
   * @throws IOException
   * @throws YarnException
   */
  Messages.UpgradeContainersResponseProto upgradeContainers(
      Messages.UpgradeContainersRequestProto request) throws IOException,
      YarnException;

  /**
   * Flex the cluster. 
   */
  Messages.FlexClusterResponseProto flexCluster(Messages.FlexClusterRequestProto request)
      throws IOException;


  /**
   * Get the current cluster status
   */
  Messages.GetJSONClusterStatusResponseProto getJSONClusterStatus(Messages.GetJSONClusterStatusRequestProto request)
      throws IOException, YarnException;


  /**
   * List all running nodes in a role
   */
  Messages.ListNodeUUIDsByRoleResponseProto listNodeUUIDsByRole(Messages.ListNodeUUIDsByRoleRequestProto request)
      throws IOException, YarnException;


  /**
   * Get the details on a node
   */
  Messages.GetNodeResponseProto getNode(Messages.GetNodeRequestProto request)
      throws IOException, YarnException;

  /**
   * Get the 
   * details on a list of nodes.
   * Unknown nodes are not returned
   * <i>Important: the order of the results are undefined</i>
   */
  Messages.GetClusterNodesResponseProto getClusterNodes(Messages.GetClusterNodesRequestProto request)
      throws IOException, YarnException;

  /**
   * Echo back the submitted text (after logging it).
   * Useful for adding information to the log, and for testing round trip
   * operations of the protocol
   * @param request request
   * @return response
   * @throws IOException
   * @throws YarnException
   */
  Messages.EchoResponseProto echo(Messages.EchoRequestProto request) throws IOException, YarnException;

  /**
   * Kill an identified container
   * @param request request containing the container to kill
   * @return the response
   * @throws IOException
   * @throws YarnException
   */
  Messages.KillContainerResponseProto killContainer(Messages.KillContainerRequestProto request)
      throws IOException, YarnException;

  /**
   * AM to commit suicide. If the Hadoop halt entry point has not been disabled,
   * this will fail rather than return with a response.
   * @param request request
   * @return response (this is not the expected outcome)
   * @throws IOException
   * @throws YarnException
   */
  Messages.AMSuicideResponseProto amSuicide(Messages.AMSuicideRequestProto request)
      throws IOException;

  /**
   * Get the instance definition
   */
  Messages.GetInstanceDefinitionResponseProto getInstanceDefinition(
    Messages.GetInstanceDefinitionRequestProto request)
    throws IOException, YarnException;

  /**
   * Get the application liveness
   * @return current liveness information
   * @throws IOException
   */
  Messages.ApplicationLivenessInformationProto getLivenessInformation(
      Messages.GetApplicationLivenessRequestProto request
  ) throws IOException;

  Messages.GetLiveContainersResponseProto getLiveContainers(
      Messages.GetLiveContainersRequestProto request
  ) throws IOException;

  Messages.ContainerInformationProto getLiveContainer(
      Messages.GetLiveContainerRequestProto request
  ) throws IOException;

  Messages.GetLiveComponentsResponseProto getLiveComponents(
      Messages.GetLiveComponentsRequestProto request
  ) throws IOException;

  Messages.ComponentInformationProto getLiveComponent(
      Messages.GetLiveComponentRequestProto request
  ) throws IOException;

  Messages.GetLiveNodesResponseProto getLiveNodes(
      Messages.GetLiveNodesRequestProto request
  ) throws IOException;

  Messages.NodeInformationProto getLiveNode(
      Messages.GetLiveNodeRequestProto request
  ) throws IOException;

  Messages.WrappedJsonProto getModelDesired(Messages.EmptyPayloadProto request) throws IOException;

  Messages.WrappedJsonProto getModelDesiredAppconf(Messages.EmptyPayloadProto request) throws IOException;

  Messages.WrappedJsonProto getModelDesiredResources(Messages.EmptyPayloadProto request) throws IOException;

  Messages.WrappedJsonProto getModelResolved(Messages.EmptyPayloadProto request) throws IOException;

  Messages.WrappedJsonProto getModelResolvedAppconf(Messages.EmptyPayloadProto request) throws IOException;

  Messages.WrappedJsonProto getModelResolvedResources(Messages.EmptyPayloadProto request) throws IOException;

  Messages.WrappedJsonProto getLiveResources(Messages.EmptyPayloadProto request) throws IOException;

  Messages.GetCertificateStoreResponseProto getClientCertificateStore(Messages.GetCertificateStoreRequestProto request)
      throws IOException;
}
