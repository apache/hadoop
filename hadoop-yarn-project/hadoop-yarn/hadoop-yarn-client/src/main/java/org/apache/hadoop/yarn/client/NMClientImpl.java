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

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.ProtoUtils;
import org.apache.hadoop.yarn.util.Records;

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
public class NMClientImpl extends AbstractService implements NMClient {

  private static final Log LOG = LogFactory.getLog(NMClientImpl.class);

  // The logically coherent operations on startedContainers is synchronized to
  // ensure they are atomic
  protected ConcurrentMap<ContainerId, StartedContainer> startedContainers =
      new ConcurrentHashMap<ContainerId, StartedContainer>();

  //enabled by default
  protected final AtomicBoolean cleanupRunningContainers = new AtomicBoolean(true);

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
    if (cleanupRunningContainers.get()) {
      cleanupRunningContainers();
    }
    super.serviceStop();
  }

  protected synchronized void cleanupRunningContainers() {
    for (StartedContainer startedContainer : startedContainers.values()) {
      try {
        stopContainer(startedContainer.getContainerId(),
            startedContainer.getNodeId(),
            startedContainer.getContainerToken());
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
  public void cleanupRunningContainersOnStop(boolean enabled) {
    cleanupRunningContainers.set(enabled);
  }

  protected static class StartedContainer {
    private ContainerId containerId;
    private NodeId nodeId;
    private Token containerToken;
    private boolean stopped;

    public StartedContainer(ContainerId containerId, NodeId nodeId,
        Token containerToken) {
      this.containerId = containerId;
      this.nodeId = nodeId;
      this.containerToken = containerToken;
      stopped = false;
    }

    public ContainerId getContainerId() {
      return containerId;
    }

    public NodeId getNodeId() {
      return nodeId;
    }

    public Token getContainerToken() {
      return containerToken;
    }
  }

  protected static final class NMCommunicator extends AbstractService {
    private ContainerId containerId;
    private NodeId nodeId;
    private Token containerToken;
    private ContainerManager containerManager;

    public NMCommunicator(ContainerId containerId, NodeId nodeId,
        Token containerToken) {
      super(NMCommunicator.class.getName());
      this.containerId = containerId;
      this.nodeId = nodeId;
      this.containerToken = containerToken;
    }

    @Override
    protected void serviceStart() throws Exception {
      final YarnRPC rpc = YarnRPC.create(getConfig());

      final InetSocketAddress containerAddress =
          NetUtils.createSocketAddr(nodeId.toString());

      // the user in createRemoteUser in this context has to be ContainerId
      UserGroupInformation currentUser =
          UserGroupInformation.createRemoteUser(containerId.toString());

      org.apache.hadoop.security.token.Token<ContainerTokenIdentifier> token =
          ProtoUtils.convertFromProtoFormat(containerToken, containerAddress);
      currentUser.addToken(token);

      containerManager = currentUser
          .doAs(new PrivilegedAction<ContainerManager>() {
            @Override
            public ContainerManager run() {
              return (ContainerManager) rpc.getProxy(ContainerManager.class,
                  containerAddress, getConfig());
            }
          });

      LOG.debug("Connecting to ContainerManager at " + containerAddress);
      super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
      if (this.containerManager != null) {
        RPC.stopProxy(this.containerManager);

        if (LOG.isDebugEnabled()) {
          InetSocketAddress containerAddress =
              NetUtils.createSocketAddr(nodeId.toString());
          LOG.debug("Disconnecting from ContainerManager at " +
              containerAddress);
        }
      }
      super.serviceStop();
    }

    public synchronized Map<String, ByteBuffer> startContainer(
        Container container, ContainerLaunchContext containerLaunchContext)
            throws YarnException, IOException {
      if (!container.getId().equals(containerId)) {
        throw new IllegalArgumentException(
            "NMCommunicator's containerId  mismatches the given Container's");
      }
      StartContainerResponse startResponse = null;
      try {
        StartContainerRequest startRequest =
            Records.newRecord(StartContainerRequest.class);
        startRequest.setContainerToken(container.getContainerToken());
        startRequest.setContainerLaunchContext(containerLaunchContext);
        startResponse = containerManager.startContainer(startRequest);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Started Container " + containerId);
        }
      } catch (YarnException e) {
        LOG.warn("Container " + containerId + " failed to start", e);
        throw e;
      } catch (IOException e) {
        LOG.warn("Container " + containerId + " failed to start", e);
        throw e;
      }
      return startResponse.getAllServiceResponse();
    }

    public synchronized void stopContainer() throws YarnException,
        IOException {
      try {
        StopContainerRequest stopRequest =
            Records.newRecord(StopContainerRequest.class);
        stopRequest.setContainerId(containerId);
        containerManager.stopContainer(stopRequest);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Stopped Container " + containerId);
        }
      } catch (YarnException e) {
        LOG.warn("Container " + containerId + " failed to stop", e);
        throw e;
      } catch (IOException e) {
        LOG.warn("Container " + containerId + " failed to stop", e);
        throw e;
      }
    }

    public synchronized ContainerStatus getContainerStatus()
        throws YarnException, IOException {
      GetContainerStatusResponse statusResponse = null;
      try {
        GetContainerStatusRequest statusRequest =
            Records.newRecord(GetContainerStatusRequest.class);
        statusRequest.setContainerId(containerId);
        statusResponse = containerManager.getContainerStatus(statusRequest);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got the status of Container " + containerId);
        }
      } catch (YarnException e) {
        LOG.warn(
            "Unable to get the status of Container " + containerId, e);
        throw e;
      } catch (IOException e) {
        LOG.warn(
            "Unable to get the status of Container " + containerId, e);
        throw e;
      }
      return statusResponse.getStatus();
    }
  }

  @Override
  public Map<String, ByteBuffer> startContainer(
      Container container, ContainerLaunchContext containerLaunchContext)
          throws YarnException, IOException {
    // Do synchronization on StartedContainer to prevent race condition
    // between startContainer and stopContainer
    synchronized (addStartedContainer(container)) {
      Map<String, ByteBuffer> allServiceResponse;
      NMCommunicator nmCommunicator = null;
      try {
        nmCommunicator = new NMCommunicator(container.getId(),
            container.getNodeId(), container.getContainerToken());
        nmCommunicator.init(getConfig());
        nmCommunicator.start();
        allServiceResponse =
            nmCommunicator.startContainer(container, containerLaunchContext);
      } catch (YarnException e) {
        // Remove the started container if it failed to start
        removeStartedContainer(container.getId());
        throw e;
      } catch (IOException e) {
        removeStartedContainer(container.getId());
        throw e;
      } catch (Throwable t) {
        removeStartedContainer(container.getId());
        throw RPCUtil.getRemoteException(t);
      } finally {
        if (nmCommunicator != null) {
          nmCommunicator.stop();
        }
      }
      return allServiceResponse;
    }

    // Three choices:
    // 1. starting and releasing the proxy before and after each interaction
    // 2. starting the proxy when starting the container and releasing it when
    // stopping the container
    // 3. starting the proxy when starting the container and releasing it when
    // stopping the client
    // Adopt 1 currently
  }

  @Override
  public void stopContainer(ContainerId containerId, NodeId nodeId,
      Token containerToken) throws YarnException, IOException {
    StartedContainer startedContainer = getStartedContainer(containerId);
    if (startedContainer == null) {
      throw RPCUtil.getRemoteException("Container " + containerId +
          " is either not started yet or already stopped");
    }
    // Only allow one request of stopping the container to move forward
    // When entering the block, check whether the precursor has already stopped
    // the container
    synchronized (startedContainer) {
      if (startedContainer.stopped) {
        return;
      }
      NMCommunicator nmCommunicator = null;
      try {
        nmCommunicator =
            new NMCommunicator(containerId, nodeId, containerToken);
        nmCommunicator.init(getConfig());
        nmCommunicator.start();
        nmCommunicator.stopContainer();
      } finally {
        if (nmCommunicator != null) {
          nmCommunicator.stop();
        }
        startedContainer.stopped = true;
        removeStartedContainer(containerId);
      }
    }
  }

  @Override
  public ContainerStatus getContainerStatus(ContainerId containerId,
      NodeId nodeId, Token containerToken)
          throws YarnException, IOException {
    NMCommunicator nmCommunicator = null;
    try {
      nmCommunicator = new NMCommunicator(containerId, nodeId, containerToken);
      nmCommunicator.init(getConfig());
      nmCommunicator.start();
      ContainerStatus containerStatus = nmCommunicator.getContainerStatus();
      return containerStatus;
    } finally {
      if (nmCommunicator != null) {
        nmCommunicator.stop();
      }
    }
  }

  protected synchronized StartedContainer addStartedContainer(
      Container container) throws YarnException, IOException {
    if (startedContainers.containsKey(container.getId())) {
      throw RPCUtil.getRemoteException("Container " + container.getId() +
          " is already started");
    }
    StartedContainer startedContainer = new StartedContainer(container.getId(),
        container.getNodeId(), container.getContainerToken());
    startedContainers.put(startedContainer.getContainerId(), startedContainer);
    return startedContainer;
  }

  protected synchronized void removeStartedContainer(ContainerId containerId) {
    startedContainers.remove(containerId);
  }

  protected synchronized StartedContainer getStartedContainer(
      ContainerId containerId) {
    return startedContainers.get(containerId);
  }

}
