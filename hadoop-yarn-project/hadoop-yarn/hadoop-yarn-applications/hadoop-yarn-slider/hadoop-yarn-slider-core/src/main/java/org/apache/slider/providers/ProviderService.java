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

package org.apache.slider.providers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.main.ExitCodeProvider;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.slider.server.appmaster.operations.RMOperationHandlerActions;
import org.apache.slider.server.appmaster.state.ContainerReleaseSelector;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.agent.AgentRestOperations;
import org.apache.slider.server.services.yarnregistry.YarnRegistryViewForProviders;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

public interface ProviderService extends ProviderCore,
    Service,
    RMOperationHandlerActions,
    ExitCodeProvider {

  /**
   * Set up the entire container launch context
   * @param containerLauncher
   * @param instanceDefinition
   * @param container
   * @param providerRole
   * @param sliderFileSystem
   * @param generatedConfPath
   * @param appComponent
   * @param containerTmpDirPath
   */
  void buildContainerLaunchContext(ContainerLauncher containerLauncher,
      AggregateConf instanceDefinition,
      Container container,
      ProviderRole providerRole,
      SliderFileSystem sliderFileSystem,
      Path generatedConfPath,
      MapOperations resourceComponent,
      MapOperations appComponent,
      Path containerTmpDirPath) throws
      IOException,
      SliderException;

  /**
   * Notify the providers of container completion
   * @param containerId container that has completed
   */
  void notifyContainerCompleted(ContainerId containerId);

  /**
   * Execute a process in the AM
   * @param instanceDefinition cluster description
   * @param confDir configuration directory
   * @param env environment
   * @param execInProgress the callback for the exec events
   * @return true if a process was actually started
   * @throws IOException
   * @throws SliderException
   */
  boolean exec(AggregateConf instanceDefinition,
               File confDir,
               Map<String, String> env,
               ProviderCompleted execInProgress) throws IOException,
      SliderException;

  /**
   * Scan through the roles and see if it is supported.
   * @param role role to look for
   * @return true if the role is known about -and therefore
   * that a launcher thread can be deployed to launch it
   */
  boolean isSupportedRole(String role);

  /**
   * Load a specific XML configuration file for the provider config
   * @param confDir configuration directory
   * @return a configuration to be included in status
   * @throws BadCommandArgumentsException
   * @throws IOException
   */
  Configuration loadProviderConfigurationInformation(File confDir)
    throws BadCommandArgumentsException, IOException;

  /**
   * The application configuration should be initialized here
   * 
   * @param instanceDefinition
   * @param fileSystem
   * @throws IOException
   * @throws SliderException
   */
  void initializeApplicationConfiguration(AggregateConf instanceDefinition,
      SliderFileSystem fileSystem) throws IOException, SliderException;

  /**
   * This is a validation of the application configuration on the AM.
   * Here is where things like the existence of keytabs and other
   * not-seen-client-side properties can be tested, before
   * the actual process is spawned. 
   * @param instanceDefinition clusterSpecification
   * @param confDir configuration directory
   * @param secure flag to indicate that secure mode checks must exist
   * @throws IOException IO problemsn
   * @throws SliderException any failure
   */
  void validateApplicationConfiguration(AggregateConf instanceDefinition,
                                        File confDir,
                                        boolean secure
                                       ) throws IOException, SliderException;

  /*
     * Build the provider status, can be empty
     * @return the provider status - map of entries to add to the info section
     */
  Map<String, String> buildProviderStatus();
  
  /**
   * Build a map of data intended for the AM webapp that is specific
   * about this provider. The key is some text to be displayed, and the
   * value can be a URL that will create an anchor over the key text.
   * 
   * If no anchor is needed/desired, insert the key with a null value.
   * @return the details
   */
  Map<String, MonitorDetail> buildMonitorDetails(ClusterDescription clusterSpec);

  /**
   * Get a human friendly name for web UIs and messages
   * @return a name string. Default is simply the service instance name.
   */
  String getHumanName();

  public void bind(StateAccessForProviders stateAccessor,
      QueueAccess queueAccess,
      List<Container> liveContainers);

  /**
   * Bind to the YARN registry
   * @param yarnRegistry YARN registry
   */
  void bindToYarnRegistry(YarnRegistryViewForProviders yarnRegistry);

  /**
   * Returns the agent rest operations interface.
   * @return  the interface if available, null otherwise.
   */
  AgentRestOperations getAgentRestOperations();

  /**
   * Build up the endpoint details for this service
   * @param details
   */
  void buildEndpointDetails(Map<String, MonitorDetail> details);

  /**
   * Prior to going live -register the initial service registry data
   * @param amWebURI URL to the AM. This may be proxied, so use relative paths
   * @param agentOpsURI URI for agent operations. This will not be proxied
   * @param agentStatusURI URI For agent status. Again: no proxy
   * @param serviceRecord service record to build up
   */
  void applyInitialRegistryDefinitions(URL amWebURI,
      URL agentOpsURI,
      URL agentStatusURI,
      ServiceRecord serviceRecord)
      throws IOException;

  /**
   * Create the container release selector for this provider...any policy
   * can be implemented
   * @return the selector to use for choosing containers.
   */
  ContainerReleaseSelector createContainerReleaseSelector();

  /**
   * On AM restart (for whatever reason) this API is required to rebuild the AM
   * internal state with the containers which were already assigned and running
   * 
   * @param liveContainers
   * @param applicationId
   * @param providerRoles
   */
  void rebuildContainerDetails(List<Container> liveContainers,
      String applicationId, Map<Integer, ProviderRole> providerRoles);
}
