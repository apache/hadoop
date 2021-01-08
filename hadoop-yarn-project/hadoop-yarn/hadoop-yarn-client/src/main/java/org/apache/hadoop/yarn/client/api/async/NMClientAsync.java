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

package org.apache.hadoop.yarn.client.api.async;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * <code>NMClientAsync</code> handles communication with all the NodeManagers
 * and provides asynchronous updates on getting responses from them. It
 * maintains a thread pool to communicate with individual NMs where a number of
 * worker threads process requests to NMs by using {@link NMClientImpl}. The max
 * size of the thread pool is configurable through
 * {@link YarnConfiguration#NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE}.
 *
 * It should be used in conjunction with a CallbackHandler. For example
 *
 * <pre>
 * {@code
 * class MyCallbackHandler extends NMClientAsync.AbstractCallbackHandler {
 *   public void onContainerStarted(ContainerId containerId,
 *       Map<String, ByteBuffer> allServiceResponse) {
 *     [post process after the container is started, process the response]
 *   }

 *   public void onContainerResourceIncreased(ContainerId containerId,
 *       Resource resource) {
 *     [post process after the container resource is increased]
 *   }
 *
 *   public void onContainerStatusReceived(ContainerId containerId,
 *       ContainerStatus containerStatus) {
 *     [make use of the status of the container]
 *   }
 *
 *   public void onContainerStopped(ContainerId containerId) {
 *     [post process after the container is stopped]
 *   }
 *
 *   public void onStartContainerError(
 *       ContainerId containerId, Throwable t) {
 *     [handle the raised exception]
 *   }
 *
 *   public void onGetContainerStatusError(
 *       ContainerId containerId, Throwable t) {
 *     [handle the raised exception]
 *   }
 *
 *   public void onStopContainerError(
 *       ContainerId containerId, Throwable t) {
 *     [handle the raised exception]
 *   }
 * }
 * }
 * </pre>
 *
 * The client's life-cycle should be managed like the following:
 *
 * <pre>
 * {@code
 * NMClientAsync asyncClient = 
 *     NMClientAsync.createNMClientAsync(new MyCallbackhandler());
 * asyncClient.init(conf);
 * asyncClient.start();
 * asyncClient.startContainer(container, containerLaunchContext);
 * [... wait for container being started]
 * asyncClient.getContainerStatus(container.getId(), container.getNodeId(),
 *     container.getContainerToken());
 * [... handle the status in the callback instance]
 * asyncClient.stopContainer(container.getId(), container.getNodeId(),
 *     container.getContainerToken());
 * [... wait for container being stopped]
 * asyncClient.stop();
 * }
 * </pre>
 */
@Public
@Stable
public abstract class NMClientAsync extends AbstractService {

  protected NMClient client;
  protected CallbackHandler callbackHandler;

  public static NMClientAsync createNMClientAsync(
      AbstractCallbackHandler callbackHandler) {
    return new NMClientAsyncImpl(callbackHandler);
  }

  protected NMClientAsync(AbstractCallbackHandler callbackHandler) {
    this (NMClientAsync.class.getName(), callbackHandler);
  }

  protected NMClientAsync(
      String name, AbstractCallbackHandler callbackHandler) {
    this (name, new NMClientImpl(), callbackHandler);
  }

  protected NMClientAsync(String name, NMClient client,
      AbstractCallbackHandler callbackHandler) {
    super(name);
    this.setClient(client);
    this.setCallbackHandler(callbackHandler);
  }

  /**
   * @deprecated Use {@link #createNMClientAsync(AbstractCallbackHandler)}
   *             instead.
   */
  @Deprecated
  public static NMClientAsync createNMClientAsync(
      CallbackHandler callbackHandler) {
    return new NMClientAsyncImpl(callbackHandler);
  }

  /**
   * @deprecated Use {@link #NMClientAsync(AbstractCallbackHandler)}
   *             instead.
   */
  @Deprecated
  protected NMClientAsync(CallbackHandler callbackHandler) {
    this (NMClientAsync.class.getName(), callbackHandler);
  }

  /**
   * @deprecated Use {@link #NMClientAsync(String, AbstractCallbackHandler)}
   *             instead.
   */
  @Deprecated
  protected NMClientAsync(String name, CallbackHandler callbackHandler) {
    this (name, new NMClientImpl(), callbackHandler);
  }

  @Private
  @VisibleForTesting
  @Deprecated
  protected NMClientAsync(String name, NMClient client,
      CallbackHandler callbackHandler) {
    super(name);
    this.setClient(client);
    this.setCallbackHandler(callbackHandler);
  }

  public abstract void startContainerAsync(
      Container container, ContainerLaunchContext containerLaunchContext);

  @Deprecated
  public abstract void increaseContainerResourceAsync(Container container);

  /**
   * <p>Update the resources of a container.</p>
   *
   * <p>The <code>ApplicationMaster</code> or other applications that use the
   * client must provide the details of the container, including the Id and
   * the target resource encapsulated in the updated container token via
   * {@link Container}.
   * </p>
   *
   * @param container the container with updated token.
   */
  public abstract void updateContainerResourceAsync(Container container);

  /**
   * <p>Re-Initialize the Container.</p>
   *
   * @param containerId the Id of the container to Re-Initialize.
   * @param containerLaunchContex the updated ContainerLaunchContext.
   * @param autoCommit commit re-initialization automatically ?
   */
  public abstract void reInitializeContainerAsync(ContainerId containerId,
      ContainerLaunchContext containerLaunchContex, boolean autoCommit);

  /**
   * <p>Restart the specified container.</p>
   *
   * @param containerId the Id of the container to restart.
   */
  public abstract void restartContainerAsync(ContainerId containerId);

  /**
   * <p>Rollback last reInitialization of the specified container.</p>
   *
   * @param containerId the Id of the container to restart.
   */
  public abstract void rollbackLastReInitializationAsync(
      ContainerId containerId);

  /**
   * <p>Commit last reInitialization of the specified container.</p>
   *
   * @param containerId the Id of the container to commit reInitialize.
   */
  public abstract void commitLastReInitializationAsync(ContainerId containerId);

  public abstract void stopContainerAsync(
      ContainerId containerId, NodeId nodeId);

  public abstract void getContainerStatusAsync(
      ContainerId containerId, NodeId nodeId);
  
  public NMClient getClient() {
    return client;
  }

  public void setClient(NMClient client) {
    this.client = client;
  }

  public CallbackHandler getCallbackHandler() {
    return callbackHandler;
  }

  public void setCallbackHandler(CallbackHandler callbackHandler) {
    this.callbackHandler = callbackHandler;
  }

  /**
   * <p>
   * The callback abstract class. The callback functions need to be implemented
   * by {@link NMClientAsync} users. The APIs are called when responses from
   * <code>NodeManager</code> are available.
   * </p>
   *
   * <p>
   * Once a callback happens, the users can chose to act on it in blocking or
   * non-blocking manner. If the action on callback is done in a blocking
   * manner, some of the threads performing requests on NodeManagers may get
   * blocked depending on how many threads in the pool are busy.
   * </p>
   *
   * <p>
   * The implementation of the callback functions should not throw the
   * unexpected exception. Otherwise, {@link NMClientAsync} will just
   * catch, log and then ignore it.
   * </p>
   */
  public abstract static class AbstractCallbackHandler
      implements CallbackHandler {
    /**
     * The API is called when <code>NodeManager</code> responds to indicate its
     * acceptance of the starting container request.
     *
     * @param containerId the Id of the container
     * @param allServiceResponse a Map between the auxiliary service names and
     *                           their outputs
     */
    public abstract void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse);

    /**
     * The API is called when <code>NodeManager</code> responds with the status
     * of the container.
     *
     * @param containerId the Id of the container
     * @param containerStatus the status of the container
     */
    public abstract void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus);

    /**
     * The API is called when <code>NodeManager</code> responds to indicate the
     * container is stopped.
     *
     * @param containerId the Id of the container
     */
    public abstract void onContainerStopped(ContainerId containerId);

    /**
     * The API is called when an exception is raised in the process of
     * starting a container.
     *
     * @param containerId the Id of the container
     * @param t the raised exception
     */
    public abstract void onStartContainerError(
        ContainerId containerId, Throwable t);

    /**
     * The API is called when <code>NodeManager</code> responds to indicate
     * the container resource has been successfully increased.
     *
     * @param containerId the Id of the container
     * @param resource the target resource of the container
     */
    @Deprecated
    public abstract void onContainerResourceIncreased(
        ContainerId containerId, Resource resource);

    /**
     * The API is called when <code>NodeManager</code> responds to indicate
     * the container resource has been successfully updated.
     *
     * @param containerId the Id of the container
     * @param resource the target resource of the container
     */
    public abstract void onContainerResourceUpdated(
        ContainerId containerId, Resource resource);

    /**
     * The API is called when an exception is raised in the process of
     * querying the status of a container.
     *
     * @param containerId the Id of the container
     * @param t the raised exception
     */
    public abstract void onGetContainerStatusError(
        ContainerId containerId, Throwable t);

    /**
     * The API is called when an exception is raised in the process of
     * increasing container resource.
     *
     * @param containerId the Id of the container
     * @param t the raised exception
     */
    @Deprecated
    public abstract void onIncreaseContainerResourceError(
        ContainerId containerId, Throwable t);

    /**
     * The API is called when an exception is raised in the process of
     * updating container resource.
     *
     * @param containerId the Id of the container
     * @param t the raised exception
     */
    public abstract void onUpdateContainerResourceError(
        ContainerId containerId, Throwable t);

    /**
     * The API is called when an exception is raised in the process of
     * stopping a container.
     *
     * @param containerId the Id of the container
     * @param t the raised exception
     */
    public abstract void onStopContainerError(
        ContainerId containerId, Throwable t);

    /**
     * Callback for container re-initialization request.
     *
     * @param containerId the Id of the container to be Re-Initialized.
     */
    public void onContainerReInitialize(ContainerId containerId) {}

    /**
     * Callback for container restart.
     *
     * @param containerId the Id of the container to restart.
     */
    public void onContainerRestart(ContainerId containerId) {}

    /**
     * Callback for rollback of last re-initialization.
     *
     * @param containerId the Id of the container to restart.
     */
    public void onRollbackLastReInitialization(ContainerId containerId) {}

    /**
     * Callback for commit of last re-initialization.
     *
     * @param containerId the Id of the container to commit reInitialize.
     */
    public void onCommitLastReInitialization(ContainerId containerId) {}

    /**
     * Error Callback for container re-initialization request.
     *
     * @param containerId the Id of the container to be Re-Initialized.
     * @param t a Throwable.
     */
    public void onContainerReInitializeError(ContainerId containerId,
        Throwable t) {}

    /**
     * Error Callback for container restart.
     *
     * @param containerId the Id of the container to restart.
     * @param t a Throwable.
     *
     */
    public void onContainerRestartError(ContainerId containerId, Throwable t) {}

    /**
     * Error Callback for rollback of last re-initialization.
     *
     * @param containerId the Id of the container to restart.
     * @param t a Throwable.
     */
    public void onRollbackLastReInitializationError(ContainerId containerId,
        Throwable t) {}

    /**
     * Error Callback for commit of last re-initialization.
     *
     * @param containerId the Id of the container to commit reInitialize.
     * @param t a Throwable.
     */
    public void onCommitLastReInitializationError(ContainerId containerId,
        Throwable t) {}
  }

  /**
   * @deprecated Use {@link NMClientAsync.AbstractCallbackHandler} instead.
   *
   * <p>
   * The callback interface needs to be implemented by {@link NMClientAsync}
   * users. The APIs are called when responses from <code>NodeManager</code> are
   * available.
   * </p>
   *
   * <p>
   * Once a callback happens, the users can chose to act on it in blocking or
   * non-blocking manner. If the action on callback is done in a blocking
   * manner, some of the threads performing requests on NodeManagers may get
   * blocked depending on how many threads in the pool are busy.
   * </p>
   *
   * <p>
   * The implementation of the callback function should not throw the
   * unexpected exception. Otherwise, {@link NMClientAsync} will just
   * catch, log and then ignore it.
   * </p>
   */
  @Deprecated
  public static interface CallbackHandler {
    /**
     * The API is called when <code>NodeManager</code> responds to indicate its
     * acceptance of the starting container request
     * @param containerId the Id of the container
     * @param allServiceResponse a Map between the auxiliary service names and
     *                           their outputs
     */
    void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse);

    /**
     * The API is called when <code>NodeManager</code> responds with the status
     * of the container
     * @param containerId the Id of the container
     * @param containerStatus the status of the container
     */
    void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus);

    /**
     * The API is called when <code>NodeManager</code> responds to indicate the
     * container is stopped.
     * @param containerId the Id of the container
     */
    void onContainerStopped(ContainerId containerId);

    /**
     * The API is called when an exception is raised in the process of
     * starting a container
     *
     * @param containerId the Id of the container
     * @param t the raised exception
     */
    void onStartContainerError(ContainerId containerId, Throwable t);

    /**
     * The API is called when an exception is raised in the process of
     * querying the status of a container
     *
     * @param containerId the Id of the container
     * @param t the raised exception
     */
    void onGetContainerStatusError(ContainerId containerId, Throwable t);

    /**
     * The API is called when an exception is raised in the process of
     * stopping a container
     *
     * @param containerId the Id of the container
     * @param t the raised exception
     */
    void onStopContainerError(ContainerId containerId, Throwable t);

  }

}
