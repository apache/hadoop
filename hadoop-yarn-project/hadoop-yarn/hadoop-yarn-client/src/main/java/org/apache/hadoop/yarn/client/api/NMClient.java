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

package org.apache.hadoop.yarn.client.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalizationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;

@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class NMClient extends AbstractService {

  /**
   * Create a new instance of NMClient.
   */
  @Public
  public static NMClient createNMClient() {
    NMClient client = new NMClientImpl();
    return client;
  }

  /**
   * Create a new instance of NMClient.
   */
  @Public
  public static NMClient createNMClient(String name) {
    NMClient client = new NMClientImpl(name);
    return client;
  }

  protected enum UpgradeOp {
    REINIT, RESTART, COMMIT, ROLLBACK
  }

  private NMTokenCache nmTokenCache = NMTokenCache.getSingleton();

  @Private
  protected NMClient(String name) {
    super(name);
  }

  /**
   * <p>Start an allocated container.</p>
   *
   * <p>The <code>ApplicationMaster</code> or other applications that use the
   * client must provide the details of the allocated container, including the
   * Id, the assigned node's Id and the token via {@link Container}. In
   * addition, the AM needs to provide the {@link ContainerLaunchContext} as
   * well.</p>
   *
   * @param container the allocated container
   * @param containerLaunchContext the context information needed by the
   *                               <code>NodeManager</code> to launch the
   *                               container
   * @return a map between the auxiliary service names and their outputs
   * @throws YarnException YarnException.
   * @throws IOException IOException.
   */
  public abstract Map<String, ByteBuffer> startContainer(Container container,
      ContainerLaunchContext containerLaunchContext)
          throws YarnException, IOException;

  /**
   * <p>Increase the resource of a container.</p>
   *
   * <p>The <code>ApplicationMaster</code> or other applications that use the
   * client must provide the details of the container, including the Id and
   * the target resource encapsulated in the updated container token via
   * {@link Container}.
   * </p>
   *
   * @param container the container with updated token.
   *
   * @throws YarnException YarnException.
   * @throws IOException IOException.
   */
  @Deprecated
  public abstract void increaseContainerResource(Container container)
      throws YarnException, IOException;

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
   *
   * @throws YarnException YarnException.
   * @throws IOException IOException.
   */
  public abstract void updateContainerResource(Container container)
      throws YarnException, IOException;

  /**
   * <p>Stop an started container.</p>
   *
   * @param containerId the Id of the started container
   * @param nodeId the Id of the <code>NodeManager</code>
   *
   * @throws YarnException YarnException.
   * @throws IOException IOException.
   */
  public abstract void stopContainer(ContainerId containerId, NodeId nodeId)
      throws YarnException, IOException;

  /**
   * <p>Query the status of a container.</p>
   *
   * @param containerId the Id of the started container
   * @param nodeId the Id of the <code>NodeManager</code>
   * 
   * @return the status of a container.
   *
   * @throws YarnException YarnException.
   * @throws IOException IOException.
   */
  public abstract ContainerStatus getContainerStatus(ContainerId containerId,
      NodeId nodeId) throws YarnException, IOException;

  /**
   * <p>Re-Initialize the Container.</p>
   *
   * @param containerId the Id of the container to Re-Initialize.
   * @param containerLaunchContex the updated ContainerLaunchContext.
   * @param autoCommit commit re-initialization automatically ?
   *
   * @throws YarnException YarnException.
   * @throws IOException IOException.
   */
  public abstract void reInitializeContainer(ContainerId containerId,
      ContainerLaunchContext containerLaunchContex, boolean autoCommit)
      throws YarnException, IOException;

  /**
   * <p>Restart the specified container.</p>
   *
   * @param containerId the Id of the container to restart.
   *
   * @throws YarnException YarnException.
   * @throws IOException IOException.
   */
  public abstract void restartContainer(ContainerId containerId)
      throws YarnException, IOException;

  /**
   * <p>Rollback last reInitialization of the specified container.</p>
   *
   * @param containerId the Id of the container to restart.
   *
   * @throws YarnException YarnException.
   * @throws IOException IOException.
   */
  public abstract void rollbackLastReInitialization(ContainerId containerId)
      throws YarnException, IOException;

  /**
   * <p>Commit last reInitialization of the specified container.</p>
   *
   * @param containerId the Id of the container to commit reInitialize.
   *
   * @throws YarnException YarnException.
   * @throws IOException IOException.
   */
  public abstract void commitLastReInitialization(ContainerId containerId)
      throws YarnException, IOException;

  /**
   * <p>Set whether the containers that are started by this client, and are
   * still running should be stopped when the client stops. By default, the
   * feature should be enabled.</p> However, containers will be stopped only  
   * when service is stopped. i.e. after {@link NMClient#stop()}. 
   *
   * @param enabled whether the feature is enabled or not
   */
  public abstract void cleanupRunningContainersOnStop(boolean enabled);

  /**
   * Set the NM Token cache of the <code>NMClient</code>. This cache must be
   * shared with the {@link AMRMClient} that requested the containers managed
   * by this <code>NMClient</code>
   * <p>
   * If a NM token cache is not set, the {@link NMTokenCache#getSingleton()}
   * singleton instance will be used.
   *
   * @param nmTokenCache the NM token cache to use.
   */
  public void setNMTokenCache(NMTokenCache nmTokenCache) {
    this.nmTokenCache = nmTokenCache;
  }

  /**
   * Get the NM token cache of the <code>NMClient</code>. This cache must be
   * shared with the {@link AMRMClient} that requested the containers managed
   * by this <code>NMClient</code>
   * <p>
   * If a NM token cache is not set, the {@link NMTokenCache#getSingleton()}
   * singleton instance will be used.
   *
   * @return the NM token cache
   */
  public NMTokenCache getNMTokenCache() {
    return nmTokenCache;
  }

  /**
   * Get the NodeId of the node on which container is running. It returns
   * null if the container if container is not found or if it is not running.
   *
   * @param containerId Container Id of the container.
   * @return NodeId of the container on which it is running.
   */
  public NodeId getNodeIdOfStartedContainer(ContainerId containerId) {

    return null;
  }

  /**
   * Localize resources for a container.
   * @param containerId     the ID of the container
   * @param nodeId          node Id of the container
   * @param localResources  resources to localize
   */
  @InterfaceStability.Unstable
  public void localize(ContainerId containerId, NodeId nodeId,
      Map<String, LocalResource> localResources) throws YarnException,
      IOException {
    // do nothing.
  }

  /**
   * Get the localization statuses of a container.
   *
   * @param containerId   the Id of the container
   * @param nodeId        node Id of the container
   *
   * @return the status of a container.
   *
   * @throws YarnException YarnException.
   * @throws IOException IOException.
   */
  @InterfaceStability.Unstable
  public List<LocalizationStatus> getLocalizationStatuses(
      ContainerId containerId, NodeId nodeId) throws YarnException,
      IOException {
    return null;
  }
}
