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
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.types.AddressTypes;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.main.ExitCodeProvider;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.state.ContainerReleaseSelector;
import org.apache.slider.server.appmaster.state.MostRecentContainerReleaseSelector;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.agent.AgentRestOperations;
import org.apache.slider.server.services.workflow.ForkedProcessService;
import org.apache.slider.server.services.workflow.ServiceParent;
import org.apache.slider.server.services.workflow.WorkflowSequenceService;
import org.apache.slider.server.services.yarnregistry.YarnRegistryViewForProviders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The base class for provider services. It lets the implementations
 * add sequences of operations, and propagates service failures
 * upstream
 */
public abstract class AbstractProviderService
    extends WorkflowSequenceService
    implements
    ProviderCore,
    SliderKeys,
    ProviderService {
  private static final Logger log =
    LoggerFactory.getLogger(AbstractProviderService.class);
  protected StateAccessForProviders amState;
  protected AgentRestOperations restOps;
  protected URL amWebAPI;
  protected YarnRegistryViewForProviders yarnRegistry;
  protected QueueAccess queueAccess;

  protected AbstractProviderService(String name) {
    super(name);
    setStopIfNoChildServicesAtStartup(false);
  }

  @Override
  public Configuration getConf() {
    return getConfig();
  }

  public StateAccessForProviders getAmState() {
    return amState;
  }

  public QueueAccess getQueueAccess() {
    return queueAccess;
  }

  public void setAmState(StateAccessForProviders amState) {
    this.amState = amState;
  }

  @Override
  public String getHumanName() {
    return getName().toLowerCase(Locale.ENGLISH);
  }
  
  @Override
  public void bind(StateAccessForProviders stateAccessor,
      QueueAccess queueAccess,
      List<Container> liveContainers) {
    this.amState = stateAccessor;
    this.queueAccess = queueAccess;
  }

  @Override
  public void bindToYarnRegistry(YarnRegistryViewForProviders yarnRegistry) {
    this.yarnRegistry = yarnRegistry;
  }

  public YarnRegistryViewForProviders getYarnRegistry() {
    return yarnRegistry;
  }

  @Override
  public AgentRestOperations getAgentRestOperations() {
    return restOps;
  }

  @Override
  public void notifyContainerCompleted(ContainerId containerId) {
  }

  public void setAgentRestOperations(AgentRestOperations agentRestOperations) {
    this.restOps = agentRestOperations;
  }

  /**
   * Load default Configuration
   * @param confDir configuration directory
   * @return configuration
   * @throws BadCommandArgumentsException
   * @throws IOException
   */
  @Override
  public Configuration loadProviderConfigurationInformation(File confDir)
      throws BadCommandArgumentsException, IOException {
    return new Configuration(false);
  }

  /**
   * Load a specific XML configuration file for the provider config
   * @param confDir configuration directory
   * @param siteXMLFilename provider-specific filename
   * @return a configuration to be included in status
   * @throws BadCommandArgumentsException argument problems
   * @throws IOException IO problems
   */
  protected Configuration loadProviderConfigurationInformation(File confDir,
                                                               String siteXMLFilename)
    throws BadCommandArgumentsException, IOException {
    Configuration siteConf;
    File siteXML = new File(confDir, siteXMLFilename);
    if (!siteXML.exists()) {
      throw new BadCommandArgumentsException(
        "Configuration directory %s doesn't contain %s - listing is %s",
        confDir, siteXMLFilename, SliderUtils.listDir(confDir));
    }

    //now read it in
    siteConf = ConfigHelper.loadConfFromFile(siteXML);
    log.info("{} file is at {}", siteXMLFilename, siteXML);
    log.info(ConfigHelper.dumpConfigToString(siteConf));
    return siteConf;
  }

  /**
   * No-op implementation of this method.
   */
  @Override
  public void initializeApplicationConfiguration(
      AggregateConf instanceDefinition, SliderFileSystem fileSystem,
      String roleGroup)
      throws IOException, SliderException {
  }

  /**
   * No-op implementation of this method.
   *
   * {@inheritDoc}
   */
  @Override
  public void validateApplicationConfiguration(AggregateConf instance,
                                               File confDir,
                                               boolean secure)
      throws IOException, SliderException {

  }

  /**
   * Scan through the roles and see if it is supported.
   * @param role role to look for
   * @return true if the role is known about -and therefore
   * that a launcher thread can be deployed to launch it
   */
  @Override
  public boolean isSupportedRole(String role) {
    Collection<ProviderRole> roles = getRoles();
    for (ProviderRole providedRole : roles) {
      if (providedRole.name.equals(role)) {
        return true;
      }
    }
    return false;
  }

  /**
   * override point to allow a process to start executing in this container
   * @param instanceDefinition cluster description
   * @param confDir configuration directory
   * @param env environment
   * @param execInProgress the callback for the exec events
   * @return false
   * @throws IOException
   * @throws SliderException
   */
  @Override
  public boolean exec(AggregateConf instanceDefinition,
      File confDir,
      Map<String, String> env,
      ProviderCompleted execInProgress) throws IOException, SliderException {
    return false;
  }

  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  @Override // ExitCodeProvider
  public int getExitCode() {
    Throwable cause = getFailureCause();
    if (cause != null) {
      //failed for some reason
      if (cause instanceof ExitCodeProvider) {
        return ((ExitCodeProvider) cause).getExitCode();
      }
    }
    ForkedProcessService lastProc = latestProcess();
    if (lastProc == null || !lastProc.isProcessTerminated()) {
      return 0;
    } else {
      return lastProc.getExitCode();
    }
  }

  /**
   * Return the latest forked process service that ran
   * @return the forkes service
   */
  protected ForkedProcessService latestProcess() {
    Service current = getActiveService();
    Service prev = getPreviousService();

    Service latest = current != null ? current : prev;
    if (latest instanceof ForkedProcessService) {
      return (ForkedProcessService) latest;
    } else {
      //its a composite object, so look inside it for a process
      if (latest instanceof ServiceParent) {
        return getFPSFromParentService((ServiceParent) latest);
      } else {
        //no match
        return null;
      }
    }
  }


  /**
   * Given a parent service, find the one that is a forked process
   * @param serviceParent parent
   * @return the forked process service or null if there is none
   */
  protected ForkedProcessService getFPSFromParentService(ServiceParent serviceParent) {
    List<Service> services = serviceParent.getServices();
    for (Service s : services) {
      if (s instanceof ForkedProcessService) {
        return (ForkedProcessService) s;
      }
    }
    return null;
  }

  /**
   * if we are already running, start this service
   */
  protected void maybeStartCommandSequence() {
    if (isInState(STATE.STARTED)) {
      startNextService();
    }
  }

  /**
   * Create a new forked process service with the given
   * name, environment and command list -then add it as a child
   * for execution in the sequence.
   *
   * @param name command name
   * @param env environment
   * @param commands command line
   * @throws IOException
   * @throws SliderException
   */
  protected ForkedProcessService queueCommand(String name,
                              Map<String, String> env,
                              List<String> commands) throws
                                                     IOException,
      SliderException {
    ForkedProcessService process = buildProcess(name, env, commands);
    //register the service for lifecycle management; when this service
    //is terminated, so is the master process
    addService(process);
    return process;
  }

  public ForkedProcessService buildProcess(String name,
                                           Map<String, String> env,
                                           List<String> commands) throws
                                                                  IOException,
      SliderException {
    ForkedProcessService process;
    process = new ForkedProcessService(name);
    process.init(getConfig());
    process.build(env, commands);
    return process;
  }

  /*
   * Build the provider status, can be empty
   * @return the provider status - map of entries to add to the info section
   */
  @Override
  public Map<String, String> buildProviderStatus() {
    return new HashMap<String, String>();
  }

  /*
  Build the monitor details. The base implementation includes all the external URL endpoints
  in the external view
   */
  @Override
  public Map<String, MonitorDetail> buildMonitorDetails(ClusterDescription clusterDesc) {
    Map<String, MonitorDetail> details = new LinkedHashMap<String, MonitorDetail>();

    // add in all the endpoints
    buildEndpointDetails(details);

    return details;
  }

  @Override
  public void buildEndpointDetails(Map<String, MonitorDetail> details) {
    ServiceRecord self = yarnRegistry.getSelfRegistration();

    List<Endpoint> externals = self.external;
    for (Endpoint endpoint : externals) {
      String addressType = endpoint.addressType;
      if (AddressTypes.ADDRESS_URI.equals(addressType)) {
        try {
          List<URL> urls = RegistryTypeUtils.retrieveAddressURLs(endpoint);
          if (!urls.isEmpty()) {
            details.put(endpoint.api, new MonitorDetail(urls.get(0).toString(), true));
          }
        } catch (InvalidRecordException  | MalformedURLException ignored) {
          // Ignored
        }

      }

    }
  }

  @Override
  public void applyInitialRegistryDefinitions(URL amWebURI,
      ServiceRecord serviceRecord)
    throws IOException {
      this.amWebAPI = amWebURI;
  }

  /**
   * {@inheritDoc}
   * 
   * 
   * @return The base implementation returns the most recent containers first.
   */
  @Override
  public ContainerReleaseSelector createContainerReleaseSelector() {
    return new MostRecentContainerReleaseSelector();
  }

  @Override
  public void releaseAssignedContainer(ContainerId containerId) {
    // no-op
  }

  @Override
  public void addContainerRequest(AMRMClient.ContainerRequest req) {
    // no-op
  }

  @Override
  public void cancelSingleRequest(AMRMClient.ContainerRequest request) {
    // no-op
  }

  @Override
  public int cancelContainerRequests(Priority priority1,
      Priority priority2,
      int count) {
    return 0;
  }

  @Override
  public void execute(List<AbstractRMOperation> operations) {
    for (AbstractRMOperation operation : operations) {
      operation.execute(this);
    }
  }
  /**
   * No-op implementation of this method.
   */
  @Override
  public void rebuildContainerDetails(List<Container> liveContainers,
      String applicationId, Map<Integer, ProviderRole> providerRoles) {
  }

  @Override
  public boolean processContainerStatus(ContainerId containerId,
      ContainerStatus status) {
    return false;
  }
}
