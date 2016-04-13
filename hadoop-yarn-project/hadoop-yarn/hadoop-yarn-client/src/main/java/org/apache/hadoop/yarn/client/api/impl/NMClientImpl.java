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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;

/**
 * <p>
 * This class implements {@link NMClient}. All the APIs are blocking.
 * </p>
 *
 * <p>
 * By default, this client stops all the running containers that are started by
 * it when it stops. It can be disabled via
 * {@link #cleanupRunningContainersOnStop}, in which case containers will
 * continue to run even after this client is stopped and till the application
 * runs at which point ResourceManager will forcefully kill them.
 * </p>
 *
 * <p>
 * Note that the blocking APIs ensure the RPC calls to <code>NodeManager</code>
 * are executed immediately, and the responses are received before these APIs
 * return. However, when {@link #startContainer} or {@link #stopContainer}
 * returns, <code>NodeManager</code> may still need some time to either start
 * or stop the container because of its asynchronous implementation. Therefore,
 * {@link #getContainerStatus} is likely to return a transit container status
 * if it is executed immediately after {@link #startContainer} or
 * {@link #stopContainer}.
 * </p>
 */
@Private
@Unstable
public class NMClientImpl extends NMClient {

  private static final Log LOG = LogFactory.getLog(NMClientImpl.class);

  // The logically coherent operations on startedContainers is synchronized to
  // ensure they are atomic
  protected ConcurrentMap<ContainerId, StartedContainer> startedContainers =
      new ConcurrentHashMap<ContainerId, StartedContainer>();

  //enabled by default
  private final AtomicBoolean cleanupRunningContainers = new AtomicBoolean(true);
  private ContainerManagementProtocolProxy cmProxy;

  public NMClientImpl() {
    super(NMClientImpl.class.getName());
  }

  public NMClientImpl(String name) {
    super(name);
  }

  @Override
  protected void serviceStop() throws Exception {
    // Usually, started-containers are stopped when this client stops. Unless
    // the flag cleanupRunningContainers is set to false.
    if (getCleanupRunningContainers().get()) {
      cleanupRunningContainers();
    }
    cmProxy.stopAllProxies();
    super.serviceStop();
  }

  protected synchronized void cleanupRunningContainers() {
    for (StartedContainer startedContainer : startedContainers.values()) {
      try {
        stopContainer(startedContainer.getContainerId(),
            startedContainer.getNodeId());
      } catch (YarnException e) {
        LOG.error("Failed to stop Container " +
            startedContainer.getContainerId() +
            "when stopping NMClientImpl");
      } catch (IOException e) {
        LOG.error("Failed to stop Container " +
            startedContainer.getContainerId() +
            "when stopping NMClientImpl");
      }
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    if (getNMTokenCache() == null) {
      throw new IllegalStateException("NMTokenCache has not been set");
    }
    cmProxy = new ContainerManagementProtocolProxy(conf, getNMTokenCache());
  }
  
  @Override
  public void cleanupRunningContainersOnStop(boolean enabled) {
    getCleanupRunningContainers().set(enabled);
  }
  
  protected static class StartedContainer {
    private ContainerId containerId;
    private NodeId nodeId;
    private ContainerState state;
    
    
    public StartedContainer(ContainerId containerId, NodeId nodeId) {
      this.containerId = containerId;
      this.nodeId = nodeId;
      state = ContainerState.NEW;
    }

    public ContainerId getContainerId() {
      return containerId;
    }

    public NodeId getNodeId() {
      return nodeId;
    }
  }

  private void addStartingContainer(StartedContainer startedContainer)
      throws YarnException {
    if (startedContainers.putIfAbsent(startedContainer.containerId,
        startedContainer) != null) {
      throw RPCUtil.getRemoteException("Container "
          + startedContainer.containerId.toString() + " is already started");
    }
  }

  @Override
  public Map<String, ByteBuffer> startContainer(
      Container container, ContainerLaunchContext containerLaunchContext)
          throws YarnException, IOException {
    // Do synchronization on StartedContainer to prevent race condition
    // between startContainer and stopContainer only when startContainer is
    // in progress for a given container.
    StartedContainer startingContainer =
        new StartedContainer(container.getId(), container.getNodeId());
    synchronized (startingContainer) {
      addStartingContainer(startingContainer);
      
      Map<String, ByteBuffer> allServiceResponse;
      ContainerManagementProtocolProxyData proxy = null;
      try {
        proxy =
            cmProxy.getProxy(container.getNodeId().toString(),
                container.getId());
        StartContainerRequest scRequest =
            StartContainerRequest.newInstance(containerLaunchContext,
              container.getContainerToken());
        List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
        list.add(scRequest);
        StartContainersRequest allRequests =
            StartContainersRequest.newInstance(list);
        StartContainersResponse response =
            proxy
                .getContainerManagementProtocol().startContainers(allRequests);
        if (response.getFailedRequests() != null
            && response.getFailedRequests().containsKey(container.getId())) {
          Throwable t =
              response.getFailedRequests().get(container.getId()).deSerialize();
          parseAndThrowException(t);
        }
        allServiceResponse = response.getAllServicesMetaData();
        startingContainer.state = ContainerState.RUNNING;
      } catch (YarnException | IOException e) {
        startingContainer.state = ContainerState.COMPLETE;
        // Remove the started container if it failed to start
        startedContainers.remove(startingContainer.containerId);
        throw e;
      } catch (Throwable t) {
        startingContainer.state = ContainerState.COMPLETE;
        startedContainers.remove(startingContainer.containerId);
        throw RPCUtil.getRemoteException(t);
      } finally {
        if (proxy != null) {
          cmProxy.mayBeCloseProxy(proxy);
        }
      }
      return allServiceResponse;
    }
  }

  @Override
  public void stopContainer(ContainerId containerId, NodeId nodeId)
      throws YarnException, IOException {
    StartedContainer startedContainer = startedContainers.get(containerId);

    // Only allow one request of stopping the container to move forward
    // When entering the block, check whether the precursor has already stopped
    // the container
    if (startedContainer != null) {
      synchronized (startedContainer) {
        if (startedContainer.state != ContainerState.RUNNING) {
          return;
        }
        stopContainerInternal(containerId, nodeId);
        // Only after successful
        startedContainer.state = ContainerState.COMPLETE;
        startedContainers.remove(startedContainer.containerId);
      }
    } else {
      stopContainerInternal(containerId, nodeId);
    }

  }

  @Override
  public ContainerStatus getContainerStatus(ContainerId containerId,
      NodeId nodeId) throws YarnException, IOException {

    ContainerManagementProtocolProxyData proxy = null;
    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(containerId);
    try {
      proxy = cmProxy.getProxy(nodeId.toString(), containerId);
      GetContainerStatusesResponse response =
          proxy.getContainerManagementProtocol().getContainerStatuses(
              GetContainerStatusesRequest.newInstance(containerIds));
      if (response.getFailedRequests() != null
          && response.getFailedRequests().containsKey(containerId)) {
        Throwable t =
            response.getFailedRequests().get(containerId).deSerialize();
        parseAndThrowException(t);
      }
      ContainerStatus containerStatus = response.getContainerStatuses().get(0);
      return containerStatus;
    } finally {
      if (proxy != null) {
        cmProxy.mayBeCloseProxy(proxy);
      }
    }
  }

  private void stopContainerInternal(ContainerId containerId, NodeId nodeId)
      throws IOException, YarnException {
    ContainerManagementProtocolProxyData proxy = null;
    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(containerId);
    try {
      proxy = cmProxy.getProxy(nodeId.toString(), containerId);
      StopContainersResponse response =
          proxy.getContainerManagementProtocol().stopContainers(
            StopContainersRequest.newInstance(containerIds));
      if (response.getFailedRequests() != null
          && response.getFailedRequests().containsKey(containerId)) {
        Throwable t = response.getFailedRequests().get(containerId)
          .deSerialize();
        parseAndThrowException(t);
      }
    } finally {
      if (proxy != null) {
        cmProxy.mayBeCloseProxy(proxy);
      }
    }
  }

  public AtomicBoolean getCleanupRunningContainers() {
    return cleanupRunningContainers;
  }

  private void parseAndThrowException(Throwable t) throws YarnException,
      IOException {
    if (t instanceof YarnException) {
      throw (YarnException) t;
    } else if (t instanceof InvalidToken) {
      throw (InvalidToken) t;
    } else {
      throw (IOException) t;
    }
  }
}
