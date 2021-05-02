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

package org.apache.hadoop.yarn.service.client;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AppAdminClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.client.cli.ApplicationCLI;
import org.apache.hadoop.yarn.client.util.YarnClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CancelUpgradeRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CompInstancesUpgradeRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.ComponentCountProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.DecommissionCompInstancesRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.FlexComponentsRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetCompInstancesRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetCompInstancesResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.RestartServiceRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.StopRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.UpgradeServiceRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.UpgradeServiceResponseProto;
import org.apache.hadoop.yarn.service.ClientAMProtocol;
import org.apache.hadoop.yarn.service.ServiceMaster;
import org.apache.hadoop.yarn.service.api.records.ComponentContainers;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.service.api.records.ConfigFile.TypeEnum;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.conf.SliderExitCodes;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.apache.hadoop.yarn.service.containerlaunch.ClasspathConstructor;
import org.apache.hadoop.yarn.service.containerlaunch.JavaCommandLineBuilder;
import org.apache.hadoop.yarn.service.exceptions.BadClusterStateException;
import org.apache.hadoop.yarn.service.exceptions.BadConfigException;
import org.apache.hadoop.yarn.service.exceptions.ErrorStrings;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.apache.hadoop.yarn.service.provider.AbstractClientProvider;
import org.apache.hadoop.yarn.service.provider.ProviderUtils;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.ServiceRegistryUtils;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.apache.hadoop.yarn.service.utils.ZookeeperUtils;
import org.apache.hadoop.yarn.util.DockerClientConfigHandler;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.Times;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.api.records.YarnApplicationState.*;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.*;
import static org.apache.hadoop.yarn.service.utils.ServiceApiUtil.jsonSerDeser;
import static org.apache.hadoop.yarn.service.utils.ServiceUtils.*;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ServiceClient extends AppAdminClient implements SliderExitCodes,
    YarnServiceConstants {
  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceClient.class);
  private SliderFileSystem fs;
  //TODO disable retry so that client / rest API doesn't block?
  protected YarnClient yarnClient;
  // Avoid looking up applicationId from fs all the time.
  private Map<String, AppInfo> cachedAppInfo = new ConcurrentHashMap<>();

  private RegistryOperations registryClient;
  private CuratorFramework curatorClient;
  private YarnRPC rpc;

  private static EnumSet<YarnApplicationState> terminatedStates =
      EnumSet.of(FINISHED, FAILED, KILLED);
  private static EnumSet<YarnApplicationState> liveStates =
      EnumSet.of(NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING);
  private static EnumSet<YarnApplicationState> preRunningStates =
      EnumSet.of(NEW, NEW_SAVING, SUBMITTED, ACCEPTED);

  @Override protected void serviceInit(Configuration configuration)
      throws Exception {
    fs = new SliderFileSystem(configuration);
    yarnClient = YarnClient.createYarnClient();
    rpc = YarnRPC.create(configuration);
    addService(yarnClient);
    super.serviceInit(configuration);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (registryClient != null) {
      registryClient.stop();
    }
    fs.getFileSystem().close();
    super.serviceStop();
  }

  public Service loadAppJsonFromLocalFS(String fileName, String serviceName,
      Long lifetime, String queue) throws IOException, YarnException {
    File file = new File(fileName);
    if (!file.exists() && fileName.equals(file.getName())) {
      String examplesDirStr = System.getenv("YARN_SERVICE_EXAMPLES_DIR");
      String[] examplesDirs;
      if (examplesDirStr == null) {
        String yarnHome = System
            .getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
        examplesDirs = new String[]{
            yarnHome + "/share/hadoop/yarn/yarn-service-examples",
            yarnHome + "/yarn-service-examples"
        };
      } else {
        examplesDirs = StringUtils.split(examplesDirStr, ":");
      }
      for (String dir : examplesDirs) {
        file = new File(MessageFormat.format("{0}/{1}/{2}.json",
            dir, fileName, fileName));
        if (file.exists()) {
          break;
        }
        // Then look for secondary location.
        file = new File(MessageFormat.format("{0}/{1}.json",
            dir, fileName));
        if (file.exists()) {
          break;
        }
      }
    }
    if (!file.exists()) {
      throw new YarnException("File or example could not be found: " +
          fileName);
    }
    Path filePath = new Path(file.getAbsolutePath());
    LOG.info("Loading service definition from local FS: " + filePath);
    Service service = jsonSerDeser
        .load(FileSystem.getLocal(getConfig()), filePath);
    if (!StringUtils.isEmpty(serviceName)) {
      service.setName(serviceName);
    }
    if (lifetime != null && lifetime > 0) {
      service.setLifetime(lifetime);
    }
    if (!StringUtils.isEmpty(queue)) {
      service.setQueue(queue);
    }
    return service;
  }

  @Override
  public int actionSave(String fileName, String serviceName, Long lifetime,
      String queue) throws IOException, YarnException {
    return actionBuild(loadAppJsonFromLocalFS(fileName, serviceName,
        lifetime, queue));
  }

  public int actionBuild(Service service)
      throws YarnException, IOException {
    ServiceApiUtil.validateAndResolveService(service, fs, getConfig());
    Path appDir = checkAppNotExistOnHdfs(service, false);
    ServiceApiUtil.createDirAndPersistApp(fs, appDir, service);
    return EXIT_SUCCESS;
  }

  private ApplicationReport upgradePrecheck(Service service)
      throws YarnException, IOException {
    boolean upgradeEnabled = getConfig().getBoolean(
        YARN_SERVICE_UPGRADE_ENABLED, YARN_SERVICE_UPGRADE_ENABLED_DEFAULT);
    if (!upgradeEnabled) {
      throw new YarnException(ErrorStrings.SERVICE_UPGRADE_DISABLED);
    }
    Service persistedService = ServiceApiUtil.loadService(fs,
        service.getName());
    if (!StringUtils.isEmpty(persistedService.getId())) {
      cachedAppInfo.put(persistedService.getName(),
          new AppInfo(ApplicationId.fromString(persistedService.getId()),
              persistedService.getKerberosPrincipal().getPrincipalName()));
    }

    if (persistedService.getVersion().equals(service.getVersion())) {
      String message = service.getName() + " is already at version "
          + service.getVersion() + ". There is nothing to upgrade.";
      LOG.error(message);
      throw new YarnException(message);
    }
    boolean foundNotNeverComp = false;
    for (Component comp : persistedService.getComponents()) {
      // If restart policy of any component is not NEVER then upgrade is
      // allowed.
      if (!comp.getRestartPolicy().equals(Component.RestartPolicyEnum.NEVER)) {
        foundNotNeverComp = true;
        break;
      }
    }
    if (!foundNotNeverComp) {
      String message = "All the components of the service " + service.getName()
          + " have " + Component.RestartPolicyEnum.NEVER + " restart policy, " +
          "so it cannot be upgraded.";
      LOG.error(message);
      throw new YarnException(message);
    }
    Service liveService = getStatus(service.getName());
    if (!liveService.getState().equals(ServiceState.STABLE)) {
      String message = service.getName() + " is at " + liveService.getState()
          + " state and upgrade can only be initiated when service is STABLE.";
      LOG.error(message);
      throw new YarnException(message);
    }

    Path serviceUpgradeDir = checkAppNotExistOnHdfs(service, true);
    ServiceApiUtil.validateAndResolveService(service, fs, getConfig());
    ServiceApiUtil.createDirAndPersistApp(fs, serviceUpgradeDir, service);

    ApplicationReport appReport = yarnClient
        .getApplicationReport(getAppId(service.getName()));
    if (StringUtils.isEmpty(appReport.getHost())) {
      throw new YarnException(service.getName() + " AM hostname is empty");
    }
    return appReport;
  }

  @Override
  public int actionUpgradeExpress(String appName, File path)
      throws IOException, YarnException {
    Service service =
        loadAppJsonFromLocalFS(path.getAbsolutePath(), appName, null, null);
    service.setState(ServiceState.UPGRADING_AUTO_FINALIZE);
    actionUpgradeExpress(service);
    return EXIT_SUCCESS;
  }

  public int actionUpgradeExpress(Service service) throws YarnException,
      IOException {
    ApplicationReport appReport = upgradePrecheck(service);
    ClientAMProtocol proxy = createAMProxy(service.getName(), appReport);
    UpgradeServiceRequestProto.Builder requestBuilder =
        UpgradeServiceRequestProto.newBuilder();
    requestBuilder.setVersion(service.getVersion());
    if (service.getState().equals(ServiceState.UPGRADING_AUTO_FINALIZE)) {
      requestBuilder.setAutoFinalize(true);
    }
    if (service.getState().equals(ServiceState.EXPRESS_UPGRADING)) {
      requestBuilder.setExpressUpgrade(true);
      requestBuilder.setAutoFinalize(true);
    }
    UpgradeServiceResponseProto responseProto = proxy.upgrade(
        requestBuilder.build());
    if (responseProto.hasError()) {
      LOG.error("Service {} express upgrade to version {} failed because {}",
          service.getName(), service.getVersion(), responseProto.getError());
      throw new YarnException("Failed to express upgrade service " +
          service.getName() + " to version " + service.getVersion() +
          " because " + responseProto.getError());
    }
    return EXIT_SUCCESS;
  }

  @Override
  public int initiateUpgrade(String appName, String fileName,
      boolean autoFinalize)
      throws IOException, YarnException {
    Service upgradeService = loadAppJsonFromLocalFS(fileName, appName,
        null, null);
    if (autoFinalize) {
      upgradeService.setState(ServiceState.UPGRADING_AUTO_FINALIZE);
    } else {
      upgradeService.setState(ServiceState.UPGRADING);
    }
    return initiateUpgrade(upgradeService);
  }

  public int initiateUpgrade(Service service) throws YarnException,
      IOException {
    ApplicationReport appReport = upgradePrecheck(service);
    ClientAMProtocol proxy = createAMProxy(service.getName(), appReport);

    UpgradeServiceRequestProto.Builder requestBuilder =
        UpgradeServiceRequestProto.newBuilder();
    requestBuilder.setVersion(service.getVersion());
    if (service.getState().equals(ServiceState.UPGRADING_AUTO_FINALIZE)) {
      requestBuilder.setAutoFinalize(true);
    }
    UpgradeServiceResponseProto responseProto = proxy.upgrade(
        requestBuilder.build());
    if (responseProto.hasError()) {
      LOG.error("Service {} upgrade to version {} failed because {}",
          service.getName(), service.getVersion(), responseProto.getError());
      throw new YarnException("Failed to upgrade service " + service.getName()
          + " to version " + service.getVersion() + " because " +
          responseProto.getError());
    }
    return EXIT_SUCCESS;
  }

  @Override
  public int actionUpgradeInstances(String appName,
      List<String> componentInstances) throws IOException, YarnException {
    checkAppExistOnHdfs(appName);
    Service persistedService = ServiceApiUtil.loadService(fs, appName);
    List<Container> containersToUpgrade = ServiceApiUtil.
        getLiveContainers(persistedService, componentInstances);
    ServiceApiUtil.validateInstancesUpgrade(containersToUpgrade);
    return actionUpgrade(persistedService, containersToUpgrade);
  }

  @Override
  public int actionUpgradeComponents(String appName,
      List<String> components) throws IOException, YarnException {
    checkAppExistOnHdfs(appName);
    Service persistedService = ServiceApiUtil.loadService(fs, appName);
    List<Container> containersToUpgrade = ServiceApiUtil
        .validateAndResolveCompsUpgrade(persistedService, components);
    return actionUpgrade(persistedService, containersToUpgrade);
  }

  @Override
  public int actionCancelUpgrade(String appName) throws IOException,
      YarnException {
    Service liveService = getStatus(appName);
    if (liveService == null ||
        !ServiceState.isUpgrading(liveService.getState())) {
      throw new YarnException("Service " + appName + " is not upgrading, " +
          "so nothing to cancel.");
    }

    ApplicationReport appReport = yarnClient.getApplicationReport(
        getAppId(appName));
    if (StringUtils.isEmpty(appReport.getHost())) {
      throw new YarnException(appName + " AM hostname is empty");
    }
    ClientAMProtocol proxy = createAMProxy(appName, appReport);
    proxy.cancelUpgrade(CancelUpgradeRequestProto.newBuilder().build());
    return EXIT_SUCCESS;
  }

  @Override
  public int actionDecommissionInstances(String appName,
      List<String> componentInstances) throws IOException, YarnException {
    checkAppExistOnHdfs(appName);
    Service persistedService = ServiceApiUtil.loadService(fs, appName);
    if (StringUtils.isEmpty(persistedService.getId())) {
      throw new YarnException(
          persistedService.getName() + " appId is null, may be not submitted " +
              "to YARN yet");
    }
    cachedAppInfo.put(persistedService.getName(), new AppInfo(
        ApplicationId.fromString(persistedService.getId()), persistedService
        .getKerberosPrincipal().getPrincipalName()));

    for (String instance : componentInstances) {
      String componentName = ServiceApiUtil.parseComponentName(
          ServiceApiUtil.parseAndValidateComponentInstanceName(instance,
              appName, getConfig()));
      Component component = persistedService.getComponent(componentName);
      if (component == null) {
        throw new IllegalArgumentException(instance + " does not exist !");
      }
      if (!component.getDecommissionedInstances().contains(instance)) {
        component.addDecommissionedInstance(instance);
        component.setNumberOfContainers(Math.max(0, component
            .getNumberOfContainers() - 1));
      }
    }
    ServiceApiUtil.writeAppDefinition(fs, persistedService);

    ApplicationReport appReport =
        yarnClient.getApplicationReport(ApplicationId.fromString(
            persistedService.getId()));
    if (appReport.getYarnApplicationState() != RUNNING) {
      String message =
          persistedService.getName() + " is at " + appReport
              .getYarnApplicationState() + " state, decommission can only be " +
              "invoked when service is running";
      LOG.error(message);
      throw new YarnException(message);
    }

    if (StringUtils.isEmpty(appReport.getHost())) {
      throw new YarnException(persistedService.getName() + " AM hostname is " +
          "empty");
    }
    ClientAMProtocol proxy =
        createAMProxy(persistedService.getName(), appReport);
    DecommissionCompInstancesRequestProto.Builder requestBuilder =
        DecommissionCompInstancesRequestProto.newBuilder();
    requestBuilder.addAllCompInstances(componentInstances);
    proxy.decommissionCompInstances(requestBuilder.build());
    return EXIT_SUCCESS;
  }

  @Override
  public int actionCleanUp(String appName, String userName) throws
      IOException, YarnException {
    if (cleanUpRegistry(appName, userName)) {
      return EXIT_SUCCESS;
    } else {
      return EXIT_FALSE;
    }
  }

  @Override
  public String getInstances(String appName,
      List<String> components, String version, List<String> containerStates)
      throws IOException, YarnException {
    GetCompInstancesResponseProto result = filterContainers(appName, components,
        version, containerStates);
    return result.getCompInstances();
  }

  public ComponentContainers[] getContainers(String appName,
      List<String> components,
      String version, List<ContainerState> containerStates)
      throws IOException, YarnException {
    GetCompInstancesResponseProto result = filterContainers(appName, components,
        version, containerStates != null ? containerStates.stream()
            .map(Enum::toString).collect(Collectors.toList()) : null);

    return ServiceApiUtil.COMP_CONTAINERS_JSON_SERDE.fromJson(
        result.getCompInstances());
  }

  private GetCompInstancesResponseProto filterContainers(String appName,
      List<String> components, String version,
      List<String> containerStates) throws IOException, YarnException {
    ApplicationReport appReport = yarnClient.getApplicationReport(getAppId(
        appName));
    if (StringUtils.isEmpty(appReport.getHost())) {
      throw new YarnException(appName + " AM hostname is empty.");
    }
    ClientAMProtocol proxy = createAMProxy(appName, appReport);
    GetCompInstancesRequestProto.Builder req = GetCompInstancesRequestProto
        .newBuilder();
    if (components != null && !components.isEmpty()) {
      req.addAllComponentNames(components);
    }
    if (version != null) {
      req.setVersion(version);
    }
    if (containerStates != null && !containerStates.isEmpty()){
      req.addAllContainerStates(containerStates);
    }
    return proxy.getCompInstances(req.build());
  }

  public int actionUpgrade(Service service, List<Container> compInstances)
      throws IOException, YarnException {
    ApplicationReport appReport =
        yarnClient.getApplicationReport(getAppId(service.getName()));

    if (appReport.getYarnApplicationState() != RUNNING) {
      String message = service.getName() + " is at " +
          appReport.getYarnApplicationState()
          + " state, upgrade can only be invoked when service is running.";
      LOG.error(message);
      throw new YarnException(message);
    }
    if (StringUtils.isEmpty(appReport.getHost())) {
      throw new YarnException(service.getName() + " AM hostname is empty.");
    }
    ClientAMProtocol proxy = createAMProxy(service.getName(), appReport);

    List<String> containerIdsToUpgrade = new ArrayList<>();
    compInstances.forEach(compInst ->
        containerIdsToUpgrade.add(compInst.getId()));
    LOG.info("instances to upgrade {}", containerIdsToUpgrade);
    CompInstancesUpgradeRequestProto.Builder upgradeRequestBuilder =
        CompInstancesUpgradeRequestProto.newBuilder();
    upgradeRequestBuilder.addAllContainerIds(containerIdsToUpgrade);
    proxy.upgrade(upgradeRequestBuilder.build());
    return EXIT_SUCCESS;
  }

  public int actionLaunch(String fileName, String serviceName, Long lifetime,
      String queue) throws IOException, YarnException {
    actionCreate(loadAppJsonFromLocalFS(fileName, serviceName, lifetime,
        queue));
    return EXIT_SUCCESS;
  }

  public ApplicationId actionCreate(Service service)
      throws IOException, YarnException {
    String serviceName = service.getName();
    ServiceApiUtil.validateAndResolveService(service, fs, getConfig());
    verifyNoLiveAppInRM(serviceName, "create");
    Path appDir = checkAppNotExistOnHdfs(service, false);

    // Write the definition first and then submit - AM will read the definition
    ServiceApiUtil.createDirAndPersistApp(fs, appDir, service);
    ApplicationId appId;
    try {
      appId = submitApp(service);
    } catch(YarnException e){
      actionDestroy(serviceName);
      throw e;
    }
    cachedAppInfo.put(serviceName, new AppInfo(appId, service
        .getKerberosPrincipal().getPrincipalName()));
    service.setId(appId.toString());
    // update app definition with appId
    ServiceApiUtil.writeAppDefinition(fs, appDir, service);
    return appId;
  }

  public int actionFlex(String serviceName, Map<String, String>
      componentCountStrings) throws YarnException, IOException {
    Map<String, Long> componentCounts =
        new HashMap<>(componentCountStrings.size());
    Service persistedService =
        ServiceApiUtil.loadService(fs, serviceName);
    if (!StringUtils.isEmpty(persistedService.getId())) {
      cachedAppInfo.put(persistedService.getName(), new AppInfo(
          ApplicationId.fromString(persistedService.getId()),
          persistedService.getKerberosPrincipal().getPrincipalName()));
    } else {
      throw new YarnException(persistedService.getName()
          + " appId is null, may be not submitted to YARN yet");
    }

    for (Map.Entry<String, String> entry : componentCountStrings.entrySet()) {
      String compName = entry.getKey();
      ServiceApiUtil.validateNameFormat(compName, getConfig());
      Component component = persistedService.getComponent(compName);
      if (component == null) {
        throw new IllegalArgumentException(entry.getKey() + " does not exist !");
      }
      long numberOfContainers =
          parseNumberOfContainers(component, entry.getValue());
      componentCounts.put(compName, numberOfContainers);
    }
    flexComponents(serviceName, componentCounts, persistedService);
    return EXIT_SUCCESS;
  }

  // Parse the number of containers requested by user, e.g.
  // +5 means add 5 additional containers
  // -5 means reduce 5 containers, if it goes to negative, sets it to 0
  // 5 means sets it to 5 containers.
  private long parseNumberOfContainers(Component component, String newNumber) {

    long orig = component.getNumberOfContainers();
    if (newNumber.startsWith("+")) {
      return orig + Long.parseLong(newNumber.substring(1));
    } else if (newNumber.startsWith("-")) {
      long ret = orig - Long.parseLong(newNumber.substring(1));
      if (ret < 0) {
        LOG.warn(MessageFormat.format(
            "[COMPONENT {0}]: component count goes to negative ({1}{2} = {3}),"
                + " ignore and reset it to 0.",
            component.getName(), orig, newNumber, ret));
        ret = 0;
      }
      return ret;
    } else {
      return Long.parseLong(newNumber);
    }
  }

  // Called by Rest Service
  public Map<String, Long> flexByRestService(String serviceName,
      Map<String, Long> componentCounts) throws YarnException, IOException {
    // load app definition
    Service persistedService = ServiceApiUtil.loadService(fs, serviceName);
    if (StringUtils.isEmpty(persistedService.getId())) {
      throw new YarnException(
          serviceName + " appId is null, may be not submitted to YARN yet");
    }
    cachedAppInfo.put(persistedService.getName(), new AppInfo(
        ApplicationId.fromString(persistedService.getId()), persistedService
        .getKerberosPrincipal().getPrincipalName()));
    return flexComponents(serviceName, componentCounts, persistedService);
  }

  private Map<String, Long> flexComponents(String serviceName,
      Map<String, Long> componentCounts, Service persistedService)
      throws YarnException, IOException {
    ServiceApiUtil.validateNameFormat(serviceName, getConfig());

    Map<String, Long> original = new HashMap<>(componentCounts.size());

    ComponentCountProto.Builder countBuilder = ComponentCountProto.newBuilder();
    FlexComponentsRequestProto.Builder requestBuilder =
        FlexComponentsRequestProto.newBuilder();

    for (Component persistedComp : persistedService.getComponents()) {
      String name = persistedComp.getName();
      if (componentCounts.containsKey(persistedComp.getName())) {
        original.put(name, persistedComp.getNumberOfContainers());
        persistedComp.setNumberOfContainers(componentCounts.get(name));

        // build the request
        countBuilder.setName(persistedComp.getName())
            .setNumberOfContainers(persistedComp.getNumberOfContainers());
        requestBuilder.addComponents(countBuilder.build());
      }
    }
    if (original.size() < componentCounts.size()) {
      componentCounts.keySet().removeAll(original.keySet());
      throw new YarnException("Components " + componentCounts.keySet()
          + " do not exist in app definition.");
    }
    ServiceApiUtil.writeAppDefinition(fs, persistedService);

    ApplicationId appId = getAppId(serviceName);
    if (appId == null) {
      String message = "Application ID doesn't exist for " + serviceName;
      LOG.error(message);
      throw new YarnException(message);
    }
    ApplicationReport appReport =
        yarnClient.getApplicationReport(appId);
    if (appReport.getYarnApplicationState() != RUNNING) {
      String message =
          serviceName + " is at " + appReport.getYarnApplicationState()
              + " state, flex can only be invoked when service is running";
      LOG.error(message);
      throw new YarnException(message);
    }

    Service liveService = getStatus(serviceName);
    if (liveService.getState().equals(ServiceState.UPGRADING) ||
        liveService.getState().equals(ServiceState.UPGRADING_AUTO_FINALIZE)) {
      String message = serviceName + " is at " +
          liveService.getState()
          + " state, flex can not be invoked when service is upgrading. ";
      LOG.error(message);
      throw new YarnException(message);
    }

    if (StringUtils.isEmpty(appReport.getHost())) {
      throw new YarnException(serviceName + " AM hostname is empty");
    }
    ClientAMProtocol proxy =
        createAMProxy(serviceName, appReport);
    proxy.flexComponents(requestBuilder.build());
    for (Map.Entry<String, Long> entry : original.entrySet()) {
      LOG.info("[COMPONENT {}]: number of containers changed from {} to {}",
          entry.getKey(), entry.getValue(),
          componentCounts.get(entry.getKey()));
    }
    return original;
  }

  @Override
  public int actionStop(String serviceName)
      throws YarnException, IOException {
    return actionStop(serviceName, true);
  }

  public int actionStop(String serviceName, boolean waitForAppStopped)
      throws YarnException, IOException {
    ServiceApiUtil.validateNameFormat(serviceName, getConfig());
    ApplicationId currentAppId = getAppId(serviceName);
    if (currentAppId == null) {
      LOG.info("Application ID doesn't exist for service {}", serviceName);
      cleanUpRegistry(serviceName);
      return EXIT_COMMAND_ARGUMENT_ERROR;
    }
    ApplicationReport report = yarnClient.getApplicationReport(currentAppId);
    if (terminatedStates.contains(report.getYarnApplicationState())) {
      LOG.info("Service {} is already in a terminated state {}", serviceName,
          report.getYarnApplicationState());
      cleanUpRegistry(serviceName);
      return EXIT_COMMAND_ARGUMENT_ERROR;
    }
    if (preRunningStates.contains(report.getYarnApplicationState())) {
      String msg = serviceName + " is at " + report.getYarnApplicationState()
          + ", forcefully killed by user!";
      yarnClient.killApplication(currentAppId, msg);
      LOG.info(msg);
      cleanUpRegistry(serviceName);
      return EXIT_SUCCESS;
    }
    if (StringUtils.isEmpty(report.getHost())) {
      throw new YarnException(serviceName + " AM hostname is empty");
    }
    LOG.info("Stopping service {}, with appId = {}", serviceName, currentAppId);
    try {
      ClientAMProtocol proxy =
          createAMProxy(serviceName, report);
      cachedAppInfo.remove(serviceName);
      if (proxy != null) {
        // try to stop the app gracefully.
        StopRequestProto request = StopRequestProto.newBuilder().build();
        proxy.stop(request);
        LOG.info("Service " + serviceName + " is being gracefully stopped...");
      } else {
        yarnClient.killApplication(currentAppId,
            serviceName + " is forcefully killed by user!");
        LOG.info("Forcefully kill the service: " + serviceName);
        cleanUpRegistry(serviceName);
        return EXIT_SUCCESS;
      }

      if (!waitForAppStopped) {
        cleanUpRegistry(serviceName);
        return EXIT_SUCCESS;
      }
      // Wait until the app is killed.
      long startTime = System.currentTimeMillis();
      int pollCount = 0;
      while (true) {
        Thread.sleep(2000);
        report = yarnClient.getApplicationReport(currentAppId);
        if (terminatedStates.contains(report.getYarnApplicationState())) {
          LOG.info("Service " + serviceName + " is stopped.");
          break;
        }
        // Forcefully kill after 10 seconds.
        if ((System.currentTimeMillis() - startTime) > 10000) {
          LOG.info("Stop operation timeout stopping, forcefully kill the app "
              + serviceName);
          yarnClient.killApplication(currentAppId,
              "Forcefully kill the app by user");
          break;
        }
        if (++pollCount % 10 == 0) {
          LOG.info("Waiting for service " + serviceName + " to be stopped.");
        }
      }
    } catch (IOException | YarnException | InterruptedException e) {
      LOG.info("Failed to stop " + serviceName + " gracefully due to: "
          + e.getMessage() + ", forcefully kill the app.");
      yarnClient.killApplication(currentAppId, "Forcefully kill the app");
    }
    cleanUpRegistry(serviceName);
    return EXIT_SUCCESS;
  }

  @Override
  public int actionDestroy(String serviceName) throws YarnException,
      IOException {
    ServiceApiUtil.validateNameFormat(serviceName, getConfig());
    verifyNoLiveAppInRM(serviceName, "destroy");

    Path appDir = fs.buildClusterDirPath(serviceName);
    FileSystem fileSystem = fs.getFileSystem();
    // remove from the appId cache
    cachedAppInfo.remove(serviceName);
    int ret = EXIT_SUCCESS;
    if (fileSystem.exists(appDir)) {
      if (fileSystem.delete(appDir, true)) {
        LOG.info("Successfully deleted service dir for " + serviceName + ": "
            + appDir);
      } else {
        String message =
            "Failed to delete service + " + serviceName + " at:  " + appDir;
        LOG.info(message);
        throw new YarnException(message);
      }
    } else {
      LOG.info("Service '" + serviceName + "' doesn't exist at hdfs path: "
          + appDir);
      ret = EXIT_NOT_FOUND;
    }

    // Delete Public Resource Dir
    Path publicResourceDir = new Path(fs.getBasePath(), serviceName);
    if (fileSystem.exists(publicResourceDir)) {
      if (fileSystem.delete(publicResourceDir, true)) {
        LOG.info("Successfully deleted public resource dir for "
            + serviceName + ": " + publicResourceDir);
      } else {
        String message = "Failed to delete public resource dir for service "
            + serviceName + " at:  " + publicResourceDir;
        LOG.info(message);
        throw new YarnException(message);
      }
    }

    try {
      deleteZKNode(serviceName);
      // don't set destroySucceed to false if no ZK node exists because not
      // all services use a ZK node
    } catch (Exception e) {
      throw new IOException("Could not delete zk node for " + serviceName, e);
    }
    if (!cleanUpRegistry(serviceName)) {
      if (ret == EXIT_SUCCESS) {
        ret = EXIT_OTHER_FAILURE;
      }
    }
    if (ret == EXIT_SUCCESS) {
      LOG.info("Successfully destroyed service {}", serviceName);
      return ret;
    } else if (ret == EXIT_NOT_FOUND) {
      LOG.error("Error on destroy '" + serviceName + "': not found.");
      return ret;
    } else {
      LOG.error("Error on destroy '" + serviceName + "': error cleaning up " +
          "registry.");
      return ret;
    }
  }

  private boolean cleanUpRegistry(String serviceName, String user) throws
      SliderException {
    String encodedName = RegistryUtils.registryUser(user);

    String registryPath = RegistryUtils.servicePath(encodedName,
        YarnServiceConstants.APP_TYPE, serviceName);
    return cleanUpRegistryPath(registryPath, serviceName);
  }

  private boolean cleanUpRegistry(String serviceName) throws SliderException {
    String registryPath =
        ServiceRegistryUtils.registryPathForInstance(serviceName);
    return cleanUpRegistryPath(registryPath, serviceName);
  }

  private boolean cleanUpRegistryPath(String registryPath, String
      serviceName) throws SliderException {
    try {
      if (getRegistryClient().exists(registryPath)) {
        getRegistryClient().delete(registryPath, true);
      } else {
        LOG.info(
            "Service '" + serviceName + "' doesn't exist at ZK registry path: "
                + registryPath);
        // not counted as a failure if the registry entries don't exist
      }
    } catch (IOException e) {
      LOG.warn("Error deleting registry entry {}", registryPath, e);
      return false;
    }
    return true;
  }

  private synchronized RegistryOperations getRegistryClient()
      throws SliderException, IOException {

    if (registryClient == null) {
      registryClient =
          RegistryOperationsFactory.createInstance("ServiceClient", getConfig());
      registryClient.init(getConfig());
      registryClient.start();
    }
    return registryClient;
  }

  /**
   * Delete service's ZK node. This is a different node from the service's
   * registry entry and is set aside for the service to use for its own ZK data.
   *
   * @param serviceName service name
   * @return true if the node was deleted, false if the node doesn't exist
   * @throws Exception if the node couldn't be deleted
   */
  private boolean deleteZKNode(String serviceName) throws Exception {
    CuratorFramework curatorFramework = getCuratorClient();
    String user = RegistryUtils.currentUser();
    String zkPath = ServiceRegistryUtils.mkServiceHomePath(user, serviceName);
    if (curatorFramework.checkExists().forPath(zkPath) != null) {
      curatorFramework.delete().deletingChildrenIfNeeded().forPath(zkPath);
      LOG.info("Deleted zookeeper path: " + zkPath);
      return true;
    } else {
      LOG.info(
          "Service '" + serviceName + "' doesn't exist at ZK path: " + zkPath);
      return false;
    }
  }

  private synchronized CuratorFramework getCuratorClient()
      throws BadConfigException {
    String registryQuorum =
        getConfig().get(RegistryConstants.KEY_REGISTRY_ZK_QUORUM);

    // though if neither is set: trouble
    if (ServiceUtils.isUnset(registryQuorum)) {
      throw new BadConfigException(
          "No Zookeeper quorum provided in the" + " configuration property "
              + RegistryConstants.KEY_REGISTRY_ZK_QUORUM);
    }
    ZookeeperUtils.splitToHostsAndPortsStrictly(registryQuorum);

    if (curatorClient == null) {
      curatorClient =
          CuratorFrameworkFactory.builder().connectString(registryQuorum)
              .sessionTimeoutMs(10000).retryPolicy(new RetryNTimes(5, 2000))
              .build();
      curatorClient.start();
    }
    return curatorClient;
  }

  private void verifyNoLiveAppInRM(String serviceName, String action)
      throws IOException, YarnException {
    Set<String> types = new HashSet<>(1);
    types.add(YarnServiceConstants.APP_TYPE);
    Set<String> tags = null;
    if (serviceName != null) {
      tags = Collections.singleton(ServiceUtils.createNameTag(serviceName));
    }
    GetApplicationsRequest request = GetApplicationsRequest.newInstance();
    request.setApplicationTypes(types);
    request.setApplicationTags(tags);
    request.setApplicationStates(liveStates);
    String user = UserGroupInformation.getCurrentUser().getUserName();
    if (user != null) {
      request.setUsers(Collections.singleton(user));
    }
    List<ApplicationReport> reports = yarnClient.getApplications(request);
    if (!reports.isEmpty()) {
      String message = "";
      if (action.equals("destroy")) {
        message = "Failed to destroy service " + serviceName
            + ", because it is still running.";
      } else {
        message = "Failed to " + action + " service " + serviceName
            + ", because it already exists.";
      }
      throw new YarnException(message);
    }
  }

  @VisibleForTesting
  ApplicationId submitApp(Service app) throws IOException, YarnException {
    String serviceName = app.getName();
    Configuration conf = getConfig();
    Path appRootDir = fs.buildClusterDirPath(app.getName());

    YarnClientApplication yarnApp = yarnClient.createApplication();
    ApplicationSubmissionContext submissionContext =
        yarnApp.getApplicationSubmissionContext();
    ServiceApiUtil.validateCompResourceSize(
        yarnApp.getNewApplicationResponse().getMaximumResourceCapability(),
        app);

    submissionContext.setKeepContainersAcrossApplicationAttempts(true);
    if (app.getLifetime() > 0) {
      Map<ApplicationTimeoutType, Long> appTimeout = new HashMap<>();
      appTimeout.put(ApplicationTimeoutType.LIFETIME, app.getLifetime());
      submissionContext.setApplicationTimeouts(appTimeout);
    }
    submissionContext.setMaxAppAttempts(YarnServiceConf
        .getInt(YarnServiceConf.AM_RESTART_MAX, DEFAULT_AM_RESTART_MAX, app
            .getConfiguration(), conf));
    submissionContext.setAttemptFailuresValidityInterval(YarnServiceConf
        .getLong(YarnServiceConf.AM_FAILURES_VALIDITY_INTERVAL,
            DEFAULT_AM_FAILURES_VALIDITY_INTERVAL, app.getConfiguration(),
            conf));

    setLogAggregationContext(app, conf, submissionContext);

    Map<String, LocalResource> localResources = new HashMap<>();

    // copy local slideram-log4j.properties to hdfs and add to localResources
    boolean hasAMLog4j =
        addAMLog4jResource(serviceName, conf, localResources);
    // copy jars to hdfs and add to localResources
    addJarResource(serviceName, localResources);
    // add keytab if in secure env
    addKeytabResourceIfSecure(fs, localResources, app);
    // add yarn sysfs to localResources
    addYarnSysFs(appRootDir, localResources, app);
    if (LOG.isDebugEnabled()) {
      printLocalResources(localResources);
    }
    Map<String, String> env = addAMEnv();

    // create AM CLI
    String cmdStr = buildCommandLine(app, conf, appRootDir, hasAMLog4j);
    submissionContext.setResource(Resource.newInstance(YarnServiceConf
        .getLong(YarnServiceConf.AM_RESOURCE_MEM,
            YarnServiceConf.DEFAULT_KEY_AM_RESOURCE_MEM,
            app.getConfiguration(), conf), 1));
    String queue = app.getQueue();
    if (StringUtils.isEmpty(queue)) {
      queue = conf.get(YARN_QUEUE, DEFAULT_YARN_QUEUE);
    }
    submissionContext.setQueue(queue);
    submissionContext.setApplicationName(serviceName);
    submissionContext.setApplicationType(YarnServiceConstants.APP_TYPE);
    Set<String> appTags =
        AbstractClientProvider.createApplicationTags(serviceName, null, null);
    if (!appTags.isEmpty()) {
      submissionContext.setApplicationTags(appTags);
    }
    ContainerLaunchContext amLaunchContext =
        Records.newRecord(ContainerLaunchContext.class);
    amLaunchContext.setCommands(Collections.singletonList(cmdStr));
    amLaunchContext.setEnvironment(env);
    amLaunchContext.setLocalResources(localResources);
    addCredentials(amLaunchContext, app);
    submissionContext.setAMContainerSpec(amLaunchContext);
    yarnClient.submitApplication(submissionContext);
    return submissionContext.getApplicationId();
  }

  /**
   * Compress (tar) the input files to the output file.
   *
   * @param files The files to compress
   * @param output The resulting output file (should end in .tar.gz)
   * @param bundleRoot
   * @throws IOException
   */
  public static File compressFiles(Collection<File> files, File output,
      String bundleRoot) throws IOException {
    try (FileOutputStream fos = new FileOutputStream(output);
        TarArchiveOutputStream taos = new TarArchiveOutputStream(
            new BufferedOutputStream(fos))) {
      taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
      for (File f : files) {
        addFilesToCompression(taos, f, "sysfs", bundleRoot);
      }
    }
    return output;
  }

  /**
   * Compile file list for compression and going recursive for
   * nested directories.
   *
   * @param taos The archive
   * @param file The file to add to the archive
   * @param dir The directory that should serve as
   *            the parent directory in the archive
   * @throws IOException
   */
  private static void addFilesToCompression(TarArchiveOutputStream taos,
      File file, String dir, String bundleRoot) throws IOException {
    if (!file.isHidden()) {
      // Create an entry for the file
      if (!dir.equals(".")) {
        if (File.separator.equals("\\")) {
          dir = dir.replaceAll("\\\\", "/");
        }
      }
      taos.putArchiveEntry(
          new TarArchiveEntry(file, dir + "/" + file.getName()));
      if (file.isFile()) {
        // Add the file to the archive
        try (FileInputStream input = new FileInputStream(file)) {
          IOUtils.copy(input, taos);
          taos.closeArchiveEntry();
        }
      } else if (file.isDirectory()) {
        // close the archive entry
        if (!dir.equals(".")) {
          taos.closeArchiveEntry();
        }
        // go through all the files in the directory and using recursion, add
        // them to the archive
        File[] allFiles = file.listFiles();
        if (allFiles != null) {
          for (File childFile : allFiles) {
            addFilesToCompression(taos, childFile,
                file.getPath().substring(bundleRoot.length()), bundleRoot);
          }
        }
      }
    }
  }

  private void addYarnSysFs(Path path,
      Map<String, LocalResource> localResources, Service app)
          throws IOException {
    List<Component> componentsWithYarnSysFS = new ArrayList<Component>();
    for(Component c : app.getComponents()) {
      boolean enabled = Boolean.parseBoolean(c.getConfiguration()
          .getEnv(ApplicationConstants.Environment
              .YARN_CONTAINER_RUNTIME_YARN_SYSFS_ENABLE.name()));
      if (enabled) {
        componentsWithYarnSysFS.add(c);
      }
    }
    if(componentsWithYarnSysFS.size() == 0) {
      return;
    }
    String buffer = ServiceApiUtil.jsonSerDeser.toJson(app);
    File testDir =
        new File(System.getProperty("java.io.tmpdir"));
    File tmpDir = Files.createTempDirectory(
        testDir.toPath(), System.currentTimeMillis() + "-").toFile();
    if (tmpDir.exists()) {
      String serviceJsonPath = tmpDir.getAbsolutePath() + "/app.json";
      File localFile = new File(serviceJsonPath);
      if (localFile.createNewFile()) {
        try (Writer writer = new OutputStreamWriter(
            new FileOutputStream(localFile), StandardCharsets.UTF_8)) {
          writer.write(buffer);
        }
      } else {
        throw new IOException("Fail to write app.json to temp directory");
      }
      File destinationFile = new File(tmpDir.getAbsolutePath() + "/sysfs.tar");
      if (!destinationFile.createNewFile()) {
        throw new IOException("Fail to localize sysfs.tar.");
      }
      List<File> files = new ArrayList<File>();
      files.add(localFile);
      compressFiles(files, destinationFile, "sysfs");
      LocalResource localResource =
          fs.submitFile(destinationFile, path, ".", "sysfs.tar");
      Path serviceJson = new Path(path, "sysfs.tar");
      for (Component c  : componentsWithYarnSysFS) {
        ConfigFile e = new ConfigFile();
        e.type(TypeEnum.ARCHIVE);
        e.srcFile(serviceJson.toString());
        e.destFile("/hadoop/yarn");
        if (!c.getConfiguration().getFiles().contains(e)) {
          c.getConfiguration().getFiles().add(e);
        }
      }
      localResources.put("sysfs", localResource);
      if (!tmpDir.delete()) {
        LOG.warn("Failed to delete temp file: " + tmpDir.getAbsolutePath());
      }
    } else {
      throw new IOException("Fail to localize sysfs resource.");
    }
  }

  private void setLogAggregationContext(Service app, Configuration conf,
      ApplicationSubmissionContext submissionContext) {
    LogAggregationContext context = Records.newRecord(LogAggregationContext
        .class);
    String finalLogInclude = YarnServiceConf.get
        (FINAL_LOG_INCLUSION_PATTERN, null, app.getConfiguration(), conf);
    if (!StringUtils.isEmpty(finalLogInclude)) {
      context.setIncludePattern(finalLogInclude);
    }
    String finalLogExclude = YarnServiceConf.get
        (FINAL_LOG_EXCLUSION_PATTERN, null, app.getConfiguration(), conf);
    if (!StringUtils.isEmpty(finalLogExclude)) {
      context.setExcludePattern(finalLogExclude);
    }
    String rollingLogInclude = YarnServiceConf.get
        (ROLLING_LOG_INCLUSION_PATTERN, null, app.getConfiguration(), conf);
    if (!StringUtils.isEmpty(rollingLogInclude)) {
      context.setRolledLogsIncludePattern(rollingLogInclude);
    }
    String rollingLogExclude = YarnServiceConf.get
        (ROLLING_LOG_EXCLUSION_PATTERN, null, app.getConfiguration(), conf);
    if (!StringUtils.isEmpty(rollingLogExclude)) {
      context.setRolledLogsExcludePattern(rollingLogExclude);
    }
    submissionContext.setLogAggregationContext(context);
  }

  private void printLocalResources(Map<String, LocalResource> map) {
    LOG.debug("Added LocalResource for localization: ");
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String, LocalResource> entry : map.entrySet()) {
      builder.append(entry.getKey()).append(" -> ")
          .append(entry.getValue().getResource().getFile())
          .append(System.lineSeparator());
    }
    LOG.debug("{}", builder);
  }

  private String buildCommandLine(Service app, Configuration conf,
      Path appRootDir, boolean hasSliderAMLog4j) throws BadConfigException {
    JavaCommandLineBuilder CLI = new JavaCommandLineBuilder();
    CLI.forceIPv4().headless();
    String jvmOpts = YarnServiceConf
        .get(YarnServiceConf.JVM_OPTS, "", app.getConfiguration(), conf);
    if (!jvmOpts.contains("-Xmx")) {
      jvmOpts += DEFAULT_AM_JVM_XMX;
    }

    // validate possible command injection.
    ServiceApiUtil.validateJvmOpts(jvmOpts);

    CLI.setJVMOpts(jvmOpts);
    if (hasSliderAMLog4j) {
      CLI.sysprop(SYSPROP_LOG4J_CONFIGURATION, YARN_SERVICE_LOG4J_FILENAME);
      CLI.sysprop(SYSPROP_LOG_DIR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    }
    CLI.add(ServiceMaster.class.getCanonicalName());
    //TODO debugAM CLI.add(Arguments.ARG_DEBUG)
    CLI.add("-" + ServiceMaster.YARNFILE_OPTION, new Path(appRootDir,
        app.getName() + ".json"));
    CLI.add("-" + ServiceMaster.SERVICE_NAME_OPTION, app.getName());
    if (app.getKerberosPrincipal() != null) {
      if (!StringUtils.isEmpty(app.getKerberosPrincipal().getKeytab())) {
        CLI.add("-" + ServiceMaster.KEYTAB_OPTION,
            app.getKerberosPrincipal().getKeytab());
      }
      if (!StringUtils.isEmpty(app.getKerberosPrincipal().getPrincipalName())) {
        CLI.add("-" + ServiceMaster.PRINCIPAL_NAME_OPTION,
            app.getKerberosPrincipal().getPrincipalName());
      }
    }
    // pass the registry binding
    CLI.addConfOptionToCLI(conf, RegistryConstants.KEY_REGISTRY_ZK_ROOT,
        RegistryConstants.DEFAULT_ZK_REGISTRY_ROOT);
    CLI.addMandatoryConfOption(conf, RegistryConstants.KEY_REGISTRY_ZK_QUORUM);

    // write out the path output
    CLI.addOutAndErrFiles(STDOUT_AM, STDERR_AM);
    String cmdStr = CLI.build();
    LOG.debug("AM launch command: {}", cmdStr);
    return cmdStr;
  }

  @VisibleForTesting
  protected Map<String, String> addAMEnv() throws IOException {
    Map<String, String> env = new HashMap<>();
    ClasspathConstructor classpath = buildClasspath(
        YarnServiceConstants.SUBMITTED_CONF_DIR,
        "lib",
        fs,
        getConfig().get(YarnServiceConf.YARN_SERVICE_CLASSPATH, ""),
        getConfig().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false));
    env.put("CLASSPATH", classpath.buildClasspath());
    env.put("LANG", "en_US.UTF-8");
    env.put("LC_ALL", "en_US.UTF-8");
    env.put("LANGUAGE", "en_US.UTF-8");
    String jaas = System.getenv("HADOOP_JAAS_DEBUG");
    if (jaas != null) {
      env.put("HADOOP_JAAS_DEBUG", jaas);
    }
    if (!UserGroupInformation.isSecurityEnabled()) {
      String userName = UserGroupInformation.getCurrentUser().getUserName();
      LOG.debug("Run as user {}", userName);
      // HADOOP_USER_NAME env is used by UserGroupInformation when log in
      // This env makes AM run as this user
      env.put("HADOOP_USER_NAME", userName);
    }
    LOG.debug("AM env: \n{}", stringifyMap(env));
    return env;
  }

  protected Path addJarResource(String serviceName,
      Map<String, LocalResource> localResources)
      throws IOException, YarnException {
    Path libPath = fs.buildClusterDirPath(serviceName);
    ProviderUtils
        .addProviderJar(localResources, ServiceMaster.class, SERVICE_CORE_JAR, fs,
            libPath, "lib", false);
    Path dependencyLibTarGzip = fs.getDependencyTarGzip();
    if (actionDependency(null, false) == EXIT_SUCCESS) {
      LOG.info("Loading lib tar from " + dependencyLibTarGzip);
      fs.submitTarGzipAndUpdate(localResources);
    } else {
      if (dependencyLibTarGzip != null) {
        LOG.warn("Property {} has a value {}, but is not a valid file",
            YarnServiceConf.DEPENDENCY_TARBALL_PATH, dependencyLibTarGzip);
      }
      String[] libs = ServiceUtils.getLibDirs();
      LOG.info("Uploading all dependency jars to HDFS. For faster submission of"
          + " apps, set config property {} to the dependency tarball location."
          + " Dependency tarball can be uploaded to any HDFS path directly"
          + " or by using command: yarn app -{} [<Destination Folder>]",
          YarnServiceConf.DEPENDENCY_TARBALL_PATH,
          ApplicationCLI.ENABLE_FAST_LAUNCH);
      for (String libDirProp : libs) {
        ProviderUtils.addAllDependencyJars(localResources, fs, libPath, "lib",
            libDirProp);
      }
    }
    return libPath;
  }

  private boolean addAMLog4jResource(String serviceName, Configuration conf,
      Map<String, LocalResource> localResources)
      throws IOException, BadClusterStateException {
    boolean hasAMLog4j = false;
    String hadoopConfDir =
        System.getenv(ApplicationConstants.Environment.HADOOP_CONF_DIR.name());
    if (hadoopConfDir != null) {
      File localFile =
          new File(hadoopConfDir, YarnServiceConstants.YARN_SERVICE_LOG4J_FILENAME);
      if (localFile.exists()) {
        Path localFilePath = createLocalPath(localFile);
        Path appDirPath = fs.buildClusterDirPath(serviceName);
        Path remoteConfPath =
            new Path(appDirPath, YarnServiceConstants.SUBMITTED_CONF_DIR);
        Path remoteFilePath =
            new Path(remoteConfPath, YarnServiceConstants.YARN_SERVICE_LOG4J_FILENAME);
        copy(conf, localFilePath, remoteFilePath);
        LocalResource localResource =
            fs.createAmResource(remoteConfPath, LocalResourceType.FILE,
            LocalResourceVisibility.APPLICATION);
        localResources.put(localFilePath.getName(), localResource);
        hasAMLog4j = true;
      } else {
        LOG.warn("AM log4j property file doesn't exist: " + localFile);
      }
    }
    return hasAMLog4j;
  }

  @Override
  public int actionStart(String serviceName) throws YarnException, IOException {
    actionStartAndGetId(serviceName);
    return EXIT_SUCCESS;
  }

  public ApplicationId actionStartAndGetId(String serviceName) throws
      YarnException, IOException {
    ServiceApiUtil.validateNameFormat(serviceName, getConfig());
    Service liveService = getStatus(serviceName);
    if (liveService == null ||
        !liveService.getState().equals(ServiceState.UPGRADING)) {
      Path appDir = checkAppExistOnHdfs(serviceName);
      Service service = ServiceApiUtil.loadService(fs, serviceName);
      ServiceApiUtil.validateAndResolveService(service, fs, getConfig());
      // see if it is actually running and bail out;
      verifyNoLiveAppInRM(serviceName, "start");
      ApplicationId appId;
      try {
        appId = submitApp(service);
      } catch (YarnException e) {
        actionDestroy(serviceName);
        throw e;
      }
      cachedAppInfo.put(serviceName, new AppInfo(appId, service
          .getKerberosPrincipal().getPrincipalName()));
      service.setId(appId.toString());
      // write app definition on to hdfs
      Path appJson = ServiceApiUtil.writeAppDefinition(fs, appDir, service);
      LOG.info("Persisted service " + service.getName() + " at " + appJson);
      return appId;
    } else {
      LOG.info("Finalize service {} upgrade", serviceName);
      ApplicationId appId = getAppId(serviceName);
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (StringUtils.isEmpty(appReport.getHost())) {
        throw new YarnException(serviceName + " AM hostname is empty");
      }
      ClientAMProtocol proxy = createAMProxy(serviceName, appReport);

      RestartServiceRequestProto.Builder requestBuilder =
          RestartServiceRequestProto.newBuilder();
      proxy.restart(requestBuilder.build());
      return appId;
    }
  }

  /**
   * Verifies that the service definition does not exist on hdfs.
   *
   * @param service   service
   * @param isUpgrade true for upgrades; false otherwise
   * @return path to the service definition..
   * @throws IOException
   * @throws SliderException
   */
  private Path checkAppNotExistOnHdfs(Service service, boolean isUpgrade)
      throws IOException, SliderException {
    Path appDir = !isUpgrade ? fs.buildClusterDirPath(service.getName()) :
        fs.buildClusterUpgradeDirPath(service.getName(), service.getVersion());
    fs.verifyDirectoryNonexistent(
        new Path(appDir, service.getName() + ".json"));
    return appDir;
  }

  /**
   * Verifies that the service exists on hdfs.
   * @param serviceName service name
   * @return path to the service definition.
   * @throws IOException
   * @throws SliderException
   */
  private Path checkAppExistOnHdfs(String serviceName)
      throws IOException, SliderException {
    Path appDir = fs.buildClusterDirPath(serviceName);
    fs.verifyPathExists(new Path(appDir, serviceName + ".json"));
    return appDir;
  }

  private void addCredentials(ContainerLaunchContext amContext, Service app)
      throws IOException {
    Credentials allCreds = new Credentials();
    // HDFS DT
    if (UserGroupInformation.isSecurityEnabled()) {
      String tokenRenewer = YarnClientUtils.getRmPrincipal(getConfig());
      if (StringUtils.isEmpty(tokenRenewer)) {
        throw new IOException(
            "Can't get Master Kerberos principal for the RM to use as renewer");
      }
      final org.apache.hadoop.security.token.Token<?>[] tokens =
          fs.getFileSystem().addDelegationTokens(tokenRenewer, allCreds);
      if (LOG.isDebugEnabled()) {
        if (tokens != null && tokens.length != 0) {
          for (Token<?> token : tokens) {
            LOG.debug("Got DT: {}", token);
          }
        }
      }
    }

    if (!StringUtils.isEmpty(app.getDockerClientConfig())) {
      allCreds.addAll(DockerClientConfigHandler.readCredentialsFromConfigFile(
          new Path(app.getDockerClientConfig()), getConfig(), app.getName()));
    }

    if (allCreds.numberOfTokens() > 0) {
      DataOutputBuffer dob = new DataOutputBuffer();
      allCreds.writeTokenStorageToStream(dob);
      ByteBuffer tokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      amContext.setTokens(tokens);
    }
  }

  private void addKeytabResourceIfSecure(SliderFileSystem fileSystem,
      Map<String, LocalResource> localResource, Service service)
      throws IOException, YarnException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    String principalName = service.getKerberosPrincipal().getPrincipalName();
    if (StringUtils.isEmpty(principalName)) {
      LOG.warn("No Kerberos principal name specified for " + service.getName());
      return;
    }
    if (StringUtils.isEmpty(service.getKerberosPrincipal().getKeytab())) {
      LOG.warn("No Kerberos keytab specified for " + service.getName());
      return;
    }

    URI keytabURI;
    try {
      keytabURI = new URI(service.getKerberosPrincipal().getKeytab());
    } catch (URISyntaxException e) {
      throw new YarnException(e);
    }

    if ("file".equals(keytabURI.getScheme())) {
      LOG.info("Using a keytab from localhost: " + keytabURI);
    } else {
      Path keytabOnhdfs = new Path(keytabURI);
      if (!fileSystem.getFileSystem().exists(keytabOnhdfs)) {
        LOG.warn(service.getName() + "'s keytab (principalName = "
            + principalName + ") doesn't exist at: " + keytabOnhdfs);
        return;
      }
      LocalResource keytabRes = fileSystem.createAmResource(keytabOnhdfs,
          LocalResourceType.FILE, LocalResourceVisibility.PRIVATE);
      localResource.put(String.format(YarnServiceConstants.KEYTAB_LOCATION,
          service.getName()), keytabRes);
      LOG.info("Adding " + service.getName() + "'s keytab for "
          + "localization, uri = " + keytabOnhdfs);
    }
  }

  public String updateLifetime(String serviceName, long lifetime)
      throws YarnException, IOException {
    ApplicationId currentAppId = getAppId(serviceName);
    if (currentAppId == null) {
      throw new YarnException("Application ID not found for " + serviceName);
    }
    ApplicationReport report = yarnClient.getApplicationReport(currentAppId);
    if (report == null) {
      throw new YarnException("Service not found for " + serviceName);
    }
    ApplicationId appId = report.getApplicationId();
    LOG.info("Updating lifetime of an service: serviceName = " + serviceName
        + ", appId = " + appId + ", lifetime = " + lifetime);
    Map<ApplicationTimeoutType, String> map = new HashMap<>();
    String newTimeout =
        Times.formatISO8601(System.currentTimeMillis() + lifetime * 1000);
    map.put(ApplicationTimeoutType.LIFETIME, newTimeout);
    UpdateApplicationTimeoutsRequest request =
        UpdateApplicationTimeoutsRequest.newInstance(appId, map);
    yarnClient.updateApplicationTimeouts(request);
    LOG.info(
        "Successfully updated lifetime for an service: serviceName = " + serviceName
            + ", appId = " + appId + ". New expiry time in ISO8601 format is "
            + newTimeout);
    return newTimeout;
  }

  public ServiceState convertState(YarnApplicationState state) {
    switch (state) {
    case NEW:
    case NEW_SAVING:
    case SUBMITTED:
    case ACCEPTED:
      return ServiceState.ACCEPTED;
    case RUNNING:
      return ServiceState.STARTED;
    case FINISHED:
    case KILLED:
      return ServiceState.STOPPED;
    case FAILED:
      return ServiceState.FAILED;
    default:
      return ServiceState.ACCEPTED;
    }
  }

  @Override
  public String getStatusString(String appIdOrName)
      throws IOException, YarnException {
    try {
      // try parsing appIdOrName, if it succeeds, it means it's appId
      ApplicationId appId = ApplicationId.fromString(appIdOrName);
      return getStatusByAppId(appId);
    } catch (IllegalArgumentException e) {
      // not appId format, it could be appName.
      Service status = getStatus(appIdOrName);
      return ServiceApiUtil.jsonSerDeser.toJson(status);
    }
  }

  private String getStatusByAppId(ApplicationId appId)
      throws IOException, YarnException {
    ApplicationReport appReport =
        yarnClient.getApplicationReport(appId);

    if (appReport.getYarnApplicationState() != RUNNING) {
      return "";
    }
    if (StringUtils.isEmpty(appReport.getHost())) {
      return "";
    }
    ClientAMProtocol amProxy = createAMProxy(appReport.getName(), appReport);
    GetStatusResponseProto response =
        amProxy.getStatus(GetStatusRequestProto.newBuilder().build());
    return response.getStatus();
  }

  public Service getStatus(String serviceName)
      throws IOException, YarnException {
    ServiceApiUtil.validateNameFormat(serviceName, getConfig());
    Service appSpec = new Service();
    appSpec.setName(serviceName);
    appSpec.setState(ServiceState.STOPPED);
    ApplicationId currentAppId = getAppId(serviceName);
    if (currentAppId == null) {
      LOG.info("Service {} does not have an application ID", serviceName);
      return appSpec;
    }
    appSpec.setId(currentAppId.toString());
    ApplicationReport appReport = null;
    try {
      appReport = yarnClient.getApplicationReport(currentAppId);
    } catch (ApplicationNotFoundException e) {
      LOG.info("application ID {} doesn't exist", currentAppId);
      return appSpec;
    }
    if (appReport == null) {
      LOG.warn("application ID {} is reported as null", currentAppId);
      return appSpec;
    }
    appSpec.setState(convertState(appReport.getYarnApplicationState()));
    ApplicationTimeout lifetime =
        appReport.getApplicationTimeouts().get(ApplicationTimeoutType.LIFETIME);
    if (lifetime != null) {
      appSpec.setLifetime(lifetime.getRemainingTime());
    }

    if (appReport.getYarnApplicationState() != RUNNING) {
      LOG.info("Service {} is at {} state", serviceName,
          appReport.getYarnApplicationState());
      return appSpec;
    }
    if (StringUtils.isEmpty(appReport.getHost())) {
      LOG.warn(serviceName + " AM hostname is empty");
      return appSpec;
    }
    ClientAMProtocol amProxy =
        createAMProxy(serviceName, appReport);
    GetStatusResponseProto response =
        amProxy.getStatus(GetStatusRequestProto.newBuilder().build());
    appSpec = jsonSerDeser.fromJson(response.getStatus());
    if (lifetime != null) {
      appSpec.setLifetime(lifetime.getRemainingTime());
    }
    return appSpec;
  }

  public YarnClient getYarnClient() {
    return this.yarnClient;
  }

  public int enableFastLaunch(String destinationFolder)
      throws IOException, YarnException {
    return actionDependency(destinationFolder, true);
  }

  public int actionDependency(String destinationFolder, boolean overwrite) {
    String currentUser = RegistryUtils.currentUser();
    LOG.info("Running command as user {}", currentUser);

    Path dependencyLibTarGzip;
    if (destinationFolder == null) {
      dependencyLibTarGzip = fs.getDependencyTarGzip();
    } else {
      dependencyLibTarGzip = new Path(destinationFolder,
          YarnServiceConstants.DEPENDENCY_TAR_GZ_FILE_NAME
              + YarnServiceConstants.DEPENDENCY_TAR_GZ_FILE_EXT);
    }

    // Check if dependency has already been uploaded, in which case log
    // appropriately and exit success (unless overwrite has been requested)
    if (fs.isFile(dependencyLibTarGzip) && !overwrite) {
      System.out.println(String.format(
          "Dependency libs are already uploaded to %s.", dependencyLibTarGzip
              .toUri()));
      return EXIT_SUCCESS;
    }

    String[] libDirs = ServiceUtils.getLibDirs();
    if (libDirs.length > 0) {
      File tempLibTarGzipFile = null;
      try {
        if (!checkPermissions(dependencyLibTarGzip)) {
          return EXIT_UNAUTHORIZED;
        }

        tempLibTarGzipFile = File.createTempFile(
            YarnServiceConstants.DEPENDENCY_TAR_GZ_FILE_NAME + "_",
            YarnServiceConstants.DEPENDENCY_TAR_GZ_FILE_EXT);
        // copy all jars
        tarGzipFolder(libDirs, tempLibTarGzipFile, createJarFilter());

        fs.copyLocalFileToHdfs(tempLibTarGzipFile, dependencyLibTarGzip,
            new FsPermission(YarnServiceConstants.DEPENDENCY_DIR_PERMISSIONS));
        LOG.info("To let apps use this tarball, in yarn-site set config " +
                "property {} to {}", YarnServiceConf.DEPENDENCY_TARBALL_PATH,
            dependencyLibTarGzip);
        return EXIT_SUCCESS;
      } catch (IOException e) {
        LOG.error("Got exception creating tarball and uploading to HDFS", e);
        return EXIT_EXCEPTION_THROWN;
      } finally {
        if (tempLibTarGzipFile != null) {
          if (!tempLibTarGzipFile.delete()) {
            LOG.warn("Failed to delete tmp file {}", tempLibTarGzipFile);
          }
        }
      }
    } else {
      return EXIT_FALSE;
    }
  }

  private boolean checkPermissions(Path dependencyLibTarGzip) throws
      IOException {
    AccessControlList yarnAdminAcl = new AccessControlList(getConfig().get(
        YarnConfiguration.YARN_ADMIN_ACL,
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    AccessControlList dfsAdminAcl = new AccessControlList(
        getConfig().get(DFSConfigKeys.DFS_ADMIN, " "));
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    if (!yarnAdminAcl.isUserAllowed(ugi) && !dfsAdminAcl.isUserAllowed(ugi)) {
      LOG.error("User must be on the {} or {} list to have permission to " +
          "upload AM dependency tarball", YarnConfiguration.YARN_ADMIN_ACL,
          DFSConfigKeys.DFS_ADMIN);
      return false;
    }

    Path parent = dependencyLibTarGzip.getParent();
    while (parent != null) {
      if (fs.getFileSystem().exists(parent)) {
        FsPermission perm = fs.getFileSystem().getFileStatus(parent)
            .getPermission();
        if (!perm.getOtherAction().implies(FsAction.READ_EXECUTE)) {
          LOG.error("Parent directory {} of {} tarball location {} does not " +
              "have world read/execute permission", parent, YarnServiceConf
              .DEPENDENCY_TARBALL_PATH, dependencyLibTarGzip);
          return false;
        }
      }
      parent = parent.getParent();
    }
    return true;
  }

  protected ClientAMProtocol createAMProxy(String serviceName,
      ApplicationReport appReport) throws IOException, YarnException {

    if (UserGroupInformation.isSecurityEnabled()) {
      if (!cachedAppInfo.containsKey(serviceName)) {
        Service persistedService  = ServiceApiUtil.loadService(fs, serviceName);
        cachedAppInfo.put(serviceName, new AppInfo(appReport.getApplicationId(),
            persistedService.getKerberosPrincipal().getPrincipalName()));
      }
      String principalName = cachedAppInfo.get(serviceName).principalName;
      // Inject the principal into hadoop conf, because Hadoop
      // SaslRpcClient#getServerPrincipal requires a config for the
      // principal
      if (!StringUtils.isEmpty(principalName)) {
        getConfig().set(PRINCIPAL, principalName);
      } else {
        throw new YarnException("No principal specified in the persisted " +
            "service definition, fail to connect to AM.");
      }
    }
    InetSocketAddress address =
        NetUtils.createSocketAddrForHost(appReport.getHost(), appReport
            .getRpcPort());
    return ClientAMProxy.createProxy(getConfig(), ClientAMProtocol.class,
        UserGroupInformation.getCurrentUser(), rpc, address);
  }

  @VisibleForTesting
  void setFileSystem(SliderFileSystem fileSystem)
      throws IOException {
    this.fs = fileSystem;
  }

  @VisibleForTesting
  void setYarnClient(YarnClient yarnClient) {
    this.yarnClient = yarnClient;
  }

  public synchronized ApplicationId getAppId(String serviceName)
      throws IOException, YarnException {
    if (cachedAppInfo.containsKey(serviceName)) {
      return cachedAppInfo.get(serviceName).appId;
    }
    Service persistedService = ServiceApiUtil.loadService(fs, serviceName);
    if (persistedService == null) {
      throw new YarnException("Service " + serviceName
          + " doesn't exist on hdfs. Please check if the app exists in RM");
    }
    if (persistedService.getId() == null) {
      return null;
    }
    ApplicationId currentAppId = ApplicationId.fromString(persistedService
        .getId());
    cachedAppInfo.put(serviceName, new AppInfo(currentAppId, persistedService
        .getKerberosPrincipal().getPrincipalName()));
    return currentAppId;
  }

  private static class AppInfo {
    ApplicationId appId;
    String principalName;

    AppInfo(ApplicationId appId, String principalName) {
      this.appId = appId;
      this.principalName = principalName;
    }
  }

}
