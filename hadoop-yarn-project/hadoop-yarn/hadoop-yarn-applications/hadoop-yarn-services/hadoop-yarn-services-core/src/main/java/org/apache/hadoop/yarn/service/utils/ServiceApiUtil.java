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

package org.apache.hadoop.yarn.service.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.ComponentState;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Configuration;
import org.apache.hadoop.yarn.service.api.records.KerberosPrincipal;
import org.apache.hadoop.yarn.service.api.records.PlacementConstraint;
import org.apache.hadoop.yarn.service.api.records.PlacementPolicy;
import org.apache.hadoop.yarn.service.api.records.Resource;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.apache.hadoop.yarn.service.conf.RestApiConstants;
import org.apache.hadoop.yarn.service.exceptions.RestApiErrorMessages;
import org.apache.hadoop.yarn.service.monitor.probe.MonitorUtils;
import org.apache.hadoop.yarn.service.provider.AbstractClientProvider;
import org.apache.hadoop.yarn.service.provider.ProviderFactory;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.service.exceptions.RestApiErrorMessages.ERROR_COMP_DOES_NOT_NEED_UPGRADE;
import static org.apache.hadoop.yarn.service.exceptions.RestApiErrorMessages.ERROR_COMP_INSTANCE_DOES_NOT_NEED_UPGRADE;

public class ServiceApiUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceApiUtil.class);
  public static JsonSerDeser<Service> jsonSerDeser =
      new JsonSerDeser<>(Service.class,
          PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

  public static final JsonSerDeser<Container[]> CONTAINER_JSON_SERDE =
      new JsonSerDeser<>(Container[].class,
          PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

  public static final JsonSerDeser<Component[]> COMP_JSON_SERDE =
      new JsonSerDeser<>(Component[].class,
          PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

  private static final PatternValidator namePattern
      = new PatternValidator("[a-z][a-z0-9-]*");

  private static final PatternValidator userNamePattern
      = new PatternValidator("[a-z][a-z0-9-.]*");



  @VisibleForTesting
  public static void setJsonSerDeser(JsonSerDeser jsd) {
    jsonSerDeser = jsd;
  }

  @VisibleForTesting
  public static void validateAndResolveService(Service service,
      SliderFileSystem fs, org.apache.hadoop.conf.Configuration conf) throws
      IOException {
    boolean dnsEnabled = conf.getBoolean(RegistryConstants.KEY_DNS_ENABLED,
        RegistryConstants.DEFAULT_DNS_ENABLED);
    if (dnsEnabled) {
      if (RegistryUtils.currentUser().length()
          > RegistryConstants.MAX_FQDN_LABEL_LENGTH) {
        throw new IllegalArgumentException(
            RestApiErrorMessages.ERROR_USER_NAME_INVALID);
      }
      userNamePattern.validate(RegistryUtils.currentUser());
    }

    if (StringUtils.isEmpty(service.getName())) {
      throw new IllegalArgumentException(
          RestApiErrorMessages.ERROR_APPLICATION_NAME_INVALID);
    }

    if (StringUtils.isEmpty(service.getVersion())) {
      throw new IllegalArgumentException(String.format(
          RestApiErrorMessages.ERROR_APPLICATION_VERSION_INVALID,
          service.getName()));
    }

    validateNameFormat(service.getName(), conf);

    // If the service has no components, throw error
    if (!hasComponent(service)) {
      throw new IllegalArgumentException(
          "No component specified for " + service.getName());
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      validateKerberosPrincipal(service.getKerberosPrincipal());
    }

    // Validate the Docker client config.
    try {
      validateDockerClientConfiguration(service, conf);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }

    // Validate there are no component name collisions (collisions are not
    // currently supported) and add any components from external services
    Configuration globalConf = service.getConfiguration();
    Set<String> componentNames = new HashSet<>();
    List<Component> componentsToRemove = new ArrayList<>();
    List<Component> componentsToAdd = new ArrayList<>();
    for (Component comp : service.getComponents()) {
      int maxCompLength = RegistryConstants.MAX_FQDN_LABEL_LENGTH;
      maxCompLength = maxCompLength - Long.toString(Long.MAX_VALUE).length();
      if (dnsEnabled && comp.getName().length() > maxCompLength) {
        throw new IllegalArgumentException(String.format(RestApiErrorMessages
            .ERROR_COMPONENT_NAME_INVALID, maxCompLength, comp.getName()));
      }
      if (service.getName().equals(comp.getName())) {
        throw new IllegalArgumentException(String.format(RestApiErrorMessages
                .ERROR_COMPONENT_NAME_CONFLICTS_WITH_SERVICE_NAME,
            comp.getName(), service.getName()));
      }
      if (componentNames.contains(comp.getName())) {
        throw new IllegalArgumentException("Component name collision: " +
            comp.getName());
      }
      // If artifact is of type SERVICE (which cannot be filled from global),
      // read external service and add its components to this service
      if (comp.getArtifact() != null && comp.getArtifact().getType() ==
          Artifact.TypeEnum.SERVICE) {
        if (StringUtils.isEmpty(comp.getArtifact().getId())) {
          throw new IllegalArgumentException(
              RestApiErrorMessages.ERROR_ARTIFACT_ID_INVALID);
        }
        LOG.info("Marking {} for removal", comp.getName());
        componentsToRemove.add(comp);
        List<Component> externalComponents = getComponents(fs,
            comp.getArtifact().getId());
        for (Component c : externalComponents) {
          Component override = service.getComponent(c.getName());
          if (override != null && override.getArtifact() == null) {
            // allow properties from external components to be overridden /
            // augmented by properties in this component, except for artifact
            // which must be read from external component
            override.mergeFrom(c);
            LOG.info("Merging external component {} from external {}", c
                .getName(), comp.getName());
          } else {
            if (componentNames.contains(c.getName())) {
              throw new IllegalArgumentException("Component name collision: " +
                  c.getName());
            }
            componentNames.add(c.getName());
            componentsToAdd.add(c);
            LOG.info("Adding component {} from external {}", c.getName(),
                comp.getName());
          }
        }
      } else {
        // otherwise handle as a normal component
        componentNames.add(comp.getName());
        // configuration
        comp.getConfiguration().mergeFrom(globalConf);
      }
    }
    service.getComponents().removeAll(componentsToRemove);
    service.getComponents().addAll(componentsToAdd);

    // Validate components and let global values take effect if component level
    // values are not provided
    Artifact globalArtifact = service.getArtifact();
    Resource globalResource = service.getResource();
    for (Component comp : service.getComponents()) {
      // fill in global artifact unless it is type SERVICE
      if (comp.getArtifact() == null && service.getArtifact() != null
          && service.getArtifact().getType() != Artifact.TypeEnum
          .SERVICE) {
        comp.setArtifact(globalArtifact);
      }
      // fill in global resource
      if (comp.getResource() == null) {
        comp.setResource(globalResource);
      }
      // validate dependency existence
      if (comp.getDependencies() != null) {
        for (String dependency : comp.getDependencies()) {
          if (!componentNames.contains(dependency)) {
            throw new IllegalArgumentException(String.format(
                RestApiErrorMessages.ERROR_DEPENDENCY_INVALID, dependency,
                comp.getName()));
          }
        }
      }
      validateComponent(comp, fs.getFileSystem(), conf);
    }
    validatePlacementPolicy(service.getComponents(), componentNames);

    // validate dependency tree
    sortByDependencies(service.getComponents());

    // Service lifetime if not specified, is set to unlimited lifetime
    if (service.getLifetime() == null) {
      service.setLifetime(RestApiConstants.DEFAULT_UNLIMITED_LIFETIME);
    }
  }

  public static void validateKerberosPrincipal(
      KerberosPrincipal kerberosPrincipal) throws IOException {
    try {
      if (!kerberosPrincipal.getPrincipalName().contains("/")) {
        throw new IllegalArgumentException(String.format(
            RestApiErrorMessages.ERROR_KERBEROS_PRINCIPAL_NAME_FORMAT,
            kerberosPrincipal.getPrincipalName()));
      }
    } catch (NullPointerException e) {
      throw new IllegalArgumentException(
          RestApiErrorMessages.ERROR_KERBEROS_PRINCIPAL_MISSING);
    }
  }

  private static void validateDockerClientConfiguration(Service service,
      org.apache.hadoop.conf.Configuration conf) throws IOException {
    String dockerClientConfig = service.getDockerClientConfig();
    if (!StringUtils.isEmpty(dockerClientConfig)) {
      Path dockerClientConfigPath = new Path(dockerClientConfig);
      FileSystem fs = dockerClientConfigPath.getFileSystem(conf);
      LOG.info("The supplied Docker client config is " + dockerClientConfig);
      if (!fs.exists(dockerClientConfigPath)) {
        throw new IOException(
            "The supplied Docker client config does not exist: "
                + dockerClientConfig);
      }
    }
  }

  private static void validateComponent(Component comp, FileSystem fs,
      org.apache.hadoop.conf.Configuration conf)
      throws IOException {
    validateNameFormat(comp.getName(), conf);

    AbstractClientProvider compClientProvider = ProviderFactory
        .getClientProvider(comp.getArtifact());
    compClientProvider.validateArtifact(comp.getArtifact(), comp.getName(), fs);

    if (comp.getLaunchCommand() == null && (comp.getArtifact() == null || comp
        .getArtifact().getType() != Artifact.TypeEnum.DOCKER)) {
      throw new IllegalArgumentException(RestApiErrorMessages
          .ERROR_ABSENT_LAUNCH_COMMAND);
    }

    validateServiceResource(comp.getResource(), comp);

    if (comp.getNumberOfContainers() == null
        || comp.getNumberOfContainers() < 0) {
      throw new IllegalArgumentException(String.format(
          RestApiErrorMessages.ERROR_CONTAINERS_COUNT_FOR_COMP_INVALID
              + ": " + comp.getNumberOfContainers(), comp.getName()));
    }
    compClientProvider.validateConfigFiles(comp.getConfiguration()
        .getFiles(), comp.getName(), fs);

    MonitorUtils.getProbe(comp.getReadinessCheck());
  }

  // Check component or service name format and transform to lower case.
  public static void validateNameFormat(String name,
      org.apache.hadoop.conf.Configuration conf) {
    if (StringUtils.isEmpty(name)) {
      throw new IllegalArgumentException("Name can not be empty!");
    }
    // validate component name
    if (name.contains("_")) {
      throw new IllegalArgumentException(
          "Invalid format: " + name
              + ", can not use '_', as DNS hostname does not allow '_'. Use '-' Instead. ");
    }
    boolean dnsEnabled = conf.getBoolean(RegistryConstants.KEY_DNS_ENABLED,
        RegistryConstants.DEFAULT_DNS_ENABLED);
    if (dnsEnabled && name.length() > RegistryConstants.MAX_FQDN_LABEL_LENGTH) {
      throw new IllegalArgumentException(String
          .format("Invalid format %s, must be no more than 63 characters ",
              name));
    }
    namePattern.validate(name);
  }

  private static void validatePlacementPolicy(List<Component> components,
      Set<String> componentNames) {
    for (Component comp : components) {
      PlacementPolicy placementPolicy = comp.getPlacementPolicy();
      if (placementPolicy != null) {
        for (PlacementConstraint constraint : placementPolicy
            .getConstraints()) {
          if (constraint.getType() == null) {
            throw new IllegalArgumentException(String.format(
              RestApiErrorMessages.ERROR_PLACEMENT_POLICY_CONSTRAINT_TYPE_NULL,
              constraint.getName() == null ? "" : constraint.getName() + " ",
              comp.getName()));
          }
          if (constraint.getScope() == null) {
            throw new IllegalArgumentException(String.format(
              RestApiErrorMessages.ERROR_PLACEMENT_POLICY_CONSTRAINT_SCOPE_NULL,
              constraint.getName() == null ? "" : constraint.getName() + " ",
              comp.getName()));
          }
          if (constraint.getTargetTags().isEmpty()) {
            throw new IllegalArgumentException(String.format(
              RestApiErrorMessages.ERROR_PLACEMENT_POLICY_CONSTRAINT_TAGS_NULL,
              constraint.getName() == null ? "" : constraint.getName() + " ",
              comp.getName()));
          }
          for (String targetTag : constraint.getTargetTags()) {
            if (!comp.getName().equals(targetTag)) {
              throw new IllegalArgumentException(String.format(
                  RestApiErrorMessages.ERROR_PLACEMENT_POLICY_TAG_NAME_NOT_SAME,
                  targetTag, comp.getName(), comp.getName(), comp.getName()));
            }
          }
        }
      }
    }
  }

  @VisibleForTesting
  public static List<Component> getComponents(SliderFileSystem
      fs, String serviceName) throws IOException {
    return loadService(fs, serviceName).getComponents();
  }

  public static Service loadService(SliderFileSystem fs, String
      serviceName) throws IOException {
    Path serviceJson = getServiceJsonPath(fs, serviceName);
    LOG.info("Loading service definition from " + serviceJson);
    return jsonSerDeser.load(fs.getFileSystem(), serviceJson);
  }

  public static Service loadServiceUpgrade(SliderFileSystem fs,
      String serviceName, String version) throws IOException {
    Path versionPath = fs.buildClusterUpgradeDirPath(serviceName, version);
    Path versionedDef = new Path(versionPath, serviceName + ".json");
    LOG.info("Loading service definition from {}", versionedDef);
    return jsonSerDeser.load(fs.getFileSystem(), versionedDef);
  }

  public static Service loadServiceFrom(SliderFileSystem fs,
      Path appDefPath) throws IOException {
    LOG.info("Loading service definition from " + appDefPath);
    return jsonSerDeser.load(fs.getFileSystem(), appDefPath);
  }

  public static Path getServiceJsonPath(SliderFileSystem fs, String serviceName) {
    Path serviceDir = fs.buildClusterDirPath(serviceName);
    return new Path(serviceDir, serviceName + ".json");
  }

  private static void validateServiceResource(Resource resource,
      Component comp) {
    // Only services/components of type SERVICE can skip resource requirement
    if (resource == null) {
      throw new IllegalArgumentException(
          comp == null ? RestApiErrorMessages.ERROR_RESOURCE_INVALID : String
              .format(RestApiErrorMessages.ERROR_RESOURCE_FOR_COMP_INVALID,
                  comp.getName()));
    }
    // One and only one of profile OR cpus & memory can be specified. Specifying
    // both raises validation error.
    if (StringUtils.isNotEmpty(resource.getProfile()) && (
        resource.getCpus() != null || StringUtils
            .isNotEmpty(resource.getMemory()))) {
      throw new IllegalArgumentException(comp == null ?
          RestApiErrorMessages.ERROR_RESOURCE_PROFILE_MULTIPLE_VALUES_NOT_SUPPORTED :
          String.format(
              RestApiErrorMessages.ERROR_RESOURCE_PROFILE_MULTIPLE_VALUES_FOR_COMP_NOT_SUPPORTED,
              comp.getName()));
    }
    // Currently resource profile is not supported yet, so we will raise
    // validation error if only resource profile is specified
    if (StringUtils.isNotEmpty(resource.getProfile())) {
      throw new IllegalArgumentException(
          RestApiErrorMessages.ERROR_RESOURCE_PROFILE_NOT_SUPPORTED_YET);
    }

    String memory = resource.getMemory();
    Integer cpus = resource.getCpus();
    if (StringUtils.isEmpty(memory)) {
      throw new IllegalArgumentException(
          comp == null ? RestApiErrorMessages.ERROR_RESOURCE_MEMORY_INVALID :
              String.format(
                  RestApiErrorMessages.ERROR_RESOURCE_MEMORY_FOR_COMP_INVALID,
                  comp.getName()));
    }
    if (cpus == null) {
      throw new IllegalArgumentException(
          comp == null ? RestApiErrorMessages.ERROR_RESOURCE_CPUS_INVALID :
              String.format(
                  RestApiErrorMessages.ERROR_RESOURCE_CPUS_FOR_COMP_INVALID,
                  comp.getName()));
    }
    if (cpus <= 0) {
      throw new IllegalArgumentException(comp == null ?
          RestApiErrorMessages.ERROR_RESOURCE_CPUS_INVALID_RANGE : String
          .format(
              RestApiErrorMessages.ERROR_RESOURCE_CPUS_FOR_COMP_INVALID_RANGE,
              comp.getName()));
    }
  }

  // check if comp mem size exceeds cluster limit
  public static void validateCompResourceSize(
      org.apache.hadoop.yarn.api.records.Resource maxResource,
      Service service) throws YarnException {
    for (Component component : service.getComponents()) {
      long mem = Long.parseLong(component.getResource().getMemory());
      if (mem > maxResource.getMemorySize()) {
        throw new YarnException(
            "Component " + component.getName() + ": specified memory size ("
                + mem + ") is larger than configured max container memory " +
                "size (" + maxResource.getMemorySize() + ")");
      }
      int cpu = component.getResource().getCpus();
      if (cpu > maxResource.getVirtualCores()) {
        throw new YarnException(
            "Component " + component.getName() + ": specified number of " +
                "virtual core (" + cpu + ") is larger than configured max " +
                "virtual core size (" + maxResource.getVirtualCores() + ")");
      }
    }
  }

  private static boolean hasComponent(Service service) {
    if (service.getComponents() == null || service.getComponents()
        .isEmpty()) {
      return false;
    }
    return true;
  }

  public static Collection<Component> sortByDependencies(List<Component>
      components) {
    Map<String, Component> sortedComponents =
        sortByDependencies(components, null);
    return sortedComponents.values();
  }

  /**
   * Each internal call of sortByDependencies will identify all of the
   * components with the same dependency depth (the lowest depth that has not
   * been processed yet) and add them to the sortedComponents list, preserving
   * their original ordering in the components list.
   *
   * So the first time it is called, all components with no dependencies
   * (depth 0) will be identified. The next time it is called, all components
   * that have dependencies only on the the depth 0 components will be
   * identified (depth 1). This will be repeated until all components have
   * been added to the sortedComponents list. If no new components are
   * identified but the sortedComponents list is not complete, an error is
   * thrown.
   */
  private static Map<String, Component> sortByDependencies(List<Component>
      components, Map<String, Component> sortedComponents) {
    if (sortedComponents == null) {
      sortedComponents = new LinkedHashMap<>();
    }

    Map<String, Component> componentsToAdd = new LinkedHashMap<>();
    List<Component> componentsSkipped = new ArrayList<>();
    for (Component component : components) {
      String name = component.getName();
      if (sortedComponents.containsKey(name)) {
        continue;
      }
      boolean dependenciesAlreadySorted = true;
      if (!ServiceUtils.isEmpty(component.getDependencies())) {
        for (String dependency : component.getDependencies()) {
          if (!sortedComponents.containsKey(dependency)) {
            dependenciesAlreadySorted = false;
            break;
          }
        }
      }
      if (dependenciesAlreadySorted) {
        componentsToAdd.put(name, component);
      } else {
        componentsSkipped.add(component);
      }
    }

    if (componentsToAdd.size() == 0) {
      throw new IllegalArgumentException(String.format(RestApiErrorMessages
          .ERROR_DEPENDENCY_CYCLE, componentsSkipped));
    }
    sortedComponents.putAll(componentsToAdd);
    if (sortedComponents.size() == components.size()) {
      return sortedComponents;
    }
    return sortByDependencies(components, sortedComponents);
  }

  public static void createDirAndPersistApp(SliderFileSystem fs, Path appDir,
      Service service)
      throws IOException, SliderException {
    FsPermission appDirPermission = new FsPermission("750");
    fs.createWithPermissions(appDir, appDirPermission);
    Path appJson = writeAppDefinition(fs, appDir, service);
    LOG.info("Persisted service {} version {} at {}", service.getName(),
        service.getVersion(), appJson);
  }

  public static Path writeAppDefinition(SliderFileSystem fs, Path appDir,
      Service service) throws IOException {
    Path appJson = new Path(appDir, service.getName() + ".json");
    jsonSerDeser.save(fs.getFileSystem(), appJson, service, true);
    return appJson;
  }

  public static Path writeAppDefinition(SliderFileSystem fs, Service service)
      throws IOException {
    Path appJson = getServiceJsonPath(fs, service.getName());
    jsonSerDeser.save(fs.getFileSystem(), appJson, service, true);
    return appJson;
  }

  public static List<Container> getLiveContainers(Service service,
      List<String> componentInstances)
      throws YarnException {
    List<Container> result = new ArrayList<>();

    // In order to avoid iterating over all the containers of all components,
    // first find the affected components by parsing the instance name.
    Multimap<String, String> affectedComps = ArrayListMultimap.create();
    for (String instanceName : componentInstances) {
      affectedComps.put(
          ServiceApiUtil.parseComponentName(instanceName), instanceName);
    }

    service.getComponents().forEach(comp -> {
      // Iterating once over the containers of the affected component to
      // find all the containers. Avoiding multiple calls to
      // service.getComponent(...) and component.getContainer(...) because they
      // iterate over all the components of the service and all the containers
      // of the components respectively.
      if (affectedComps.get(comp.getName()) != null) {
        Collection<String> instanceNames = affectedComps.get(comp.getName());
        comp.getContainers().forEach(container -> {
          if (instanceNames.contains(container.getComponentInstanceName())) {
            result.add(container);
          }
        });
      }
    });
    return result;
  }

  /**
   * Validates that the component instances that are requested to upgrade
   * require an upgrade.
   */
  public static void validateInstancesUpgrade(List<Container>
      liveContainers) throws YarnException {
    for (Container liveContainer : liveContainers) {
      if (!isUpgradable(liveContainer)) {
        // Nothing to upgrade
        throw new YarnException(String.format(
            ERROR_COMP_INSTANCE_DOES_NOT_NEED_UPGRADE,
            liveContainer.getComponentInstanceName()));
      }
    }
  }

  /**
   * Returns whether the container can be upgraded in the current state.
   */
  public static boolean isUpgradable(Container container) {

    return container.getState() != null &&
        (container.getState().equals(ContainerState.NEEDS_UPGRADE) ||
            container.getState().equals(ContainerState.FAILED_UPGRADE));
  }

  /**
   * Validates the components that are requested to upgrade require an upgrade.
   * It returns the instances of the components which need upgrade.
   */
  public static List<Container> validateAndResolveCompsUpgrade(
      Service liveService, Collection<String> compNames) throws YarnException {
    Preconditions.checkNotNull(compNames);
    HashSet<String> requestedComps = Sets.newHashSet(compNames);
    List<Container> containerNeedUpgrade = new ArrayList<>();
    for (Component liveComp : liveService.getComponents()) {
      if (requestedComps.contains(liveComp.getName())) {
        if (!liveComp.getState().equals(ComponentState.NEEDS_UPGRADE)) {
          // Nothing to upgrade
          throw new YarnException(String.format(
              ERROR_COMP_DOES_NOT_NEED_UPGRADE, liveComp.getName()));
        }
        liveComp.getContainers().forEach(liveContainer -> {
          if (isUpgradable(liveContainer)) {
            containerNeedUpgrade.add(liveContainer);
          }
        });
      }
    }
    return containerNeedUpgrade;
  }

  /**
   * Validates the components that are requested are stable for upgrade.
   * It returns the instances of the components which are in ready state.
   */
  public static List<Container> validateAndResolveCompsStable(
      Service liveService, Collection<String> compNames) throws YarnException {
    Preconditions.checkNotNull(compNames);
    HashSet<String> requestedComps = Sets.newHashSet(compNames);
    List<Container> containerNeedUpgrade = new ArrayList<>();
    for (Component liveComp : liveService.getComponents()) {
      if (requestedComps.contains(liveComp.getName())) {
        if (!liveComp.getState().equals(ComponentState.STABLE)) {
          // Nothing to upgrade
          throw new YarnException(String.format(
              ERROR_COMP_DOES_NOT_NEED_UPGRADE, liveComp.getName()));
        }
        liveComp.getContainers().forEach(liveContainer -> {
          if (liveContainer.getState().equals(ContainerState.READY)) {
            containerNeedUpgrade.add(liveContainer);
          }
        });
      }
    }
    return containerNeedUpgrade;
  }

  public static String getHostnameSuffix(String serviceName, org.apache
      .hadoop.conf.Configuration conf) {
    String domain = conf.get(RegistryConstants.KEY_DNS_DOMAIN);
    String hostnameSuffix;
    if (domain == null || domain.isEmpty()) {
      hostnameSuffix = MessageFormat
          .format(".{0}.{1}", serviceName, RegistryUtils.currentUser());
    } else {
      hostnameSuffix = MessageFormat
          .format(".{0}.{1}.{2}", serviceName,
              RegistryUtils.currentUser(), domain);
    }
    return hostnameSuffix;
  }

  public static String parseAndValidateComponentInstanceName(String
      instanceOrHostname, String serviceName, org.apache.hadoop.conf
      .Configuration conf) throws IllegalArgumentException {
    int idx = instanceOrHostname.indexOf('.');
    String hostnameSuffix = getHostnameSuffix(serviceName, conf);
    if (idx != -1) {
      if (!instanceOrHostname.endsWith(hostnameSuffix)) {
        throw new IllegalArgumentException("Specified hostname " +
            instanceOrHostname + " does not have the expected format " +
            "componentInstanceName" +
            hostnameSuffix);
      }
      instanceOrHostname = instanceOrHostname.substring(0, instanceOrHostname
          .length() - hostnameSuffix.length());
    }
    idx = instanceOrHostname.indexOf('.');
    if (idx != -1) {
      throw new IllegalArgumentException("Specified hostname " +
          instanceOrHostname + " does not have the expected format " +
          "componentInstanceName" +
          hostnameSuffix);
    }
    return instanceOrHostname;
  }

  public static String parseComponentName(String componentInstanceName)
      throws YarnException {
    int idx = componentInstanceName.indexOf('.');
    if (idx != -1) {
      componentInstanceName = componentInstanceName.substring(0, idx);
    }
    idx = componentInstanceName.lastIndexOf('-');
    if (idx == -1) {
      throw new YarnException("Invalid component instance (" +
          componentInstanceName + ") name.");
    }
    return componentInstanceName.substring(0, idx);
  }

  public static String $(String s) {
    return "${" + s +"}";
  }

  public static List<String> resolveCompsDependency(Service service) {
    List<String> components = new ArrayList<String>();
    for (Component component : service.getComponents()) {
      int depSize = component.getDependencies().size();
      if (!components.contains(component.getName())) {
        components.add(component.getName());
      }
      if (depSize != 0) {
        for (String depComp : component.getDependencies()) {
          if (!components.contains(depComp)) {
            components.add(0, depComp);
          }
        }
      }
    }
    return components;
  }
}
