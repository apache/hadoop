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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Application;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Configuration;
import org.apache.hadoop.yarn.service.api.records.Resource;
import org.apache.hadoop.yarn.service.provider.AbstractClientProvider;
import org.apache.hadoop.yarn.service.provider.ProviderFactory;
import org.apache.hadoop.yarn.service.servicemonitor.probe.MonitorUtils;
import org.apache.hadoop.yarn.service.conf.RestApiConstants;
import org.apache.hadoop.yarn.service.exceptions.RestApiErrorMessages;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ServiceApiUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceApiUtil.class);
  public static JsonSerDeser<Application> jsonSerDeser =
      new JsonSerDeser<>(Application.class,
          PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
  private static final PatternValidator compNamePattern
      = new PatternValidator("[a-z][a-z0-9-]*");

  @VisibleForTesting
  public static void setJsonSerDeser(JsonSerDeser jsd) {
    jsonSerDeser = jsd;
  }

  @VisibleForTesting
  public static void validateAndResolveApplication(Application application,
      SliderFileSystem fs, org.apache.hadoop.conf.Configuration conf) throws
      IOException {
    boolean dnsEnabled = conf.getBoolean(RegistryConstants.KEY_DNS_ENABLED,
        RegistryConstants.DEFAULT_DNS_ENABLED);
    if (dnsEnabled && RegistryUtils.currentUser().length() > RegistryConstants
        .MAX_FQDN_LABEL_LENGTH) {
      throw new IllegalArgumentException(RestApiErrorMessages
          .ERROR_USER_NAME_INVALID);
    }
    if (StringUtils.isEmpty(application.getName())) {
      throw new IllegalArgumentException(
          RestApiErrorMessages.ERROR_APPLICATION_NAME_INVALID);
    }
    if (!SliderUtils.isClusternameValid(application.getName()) || (dnsEnabled
        && application.getName().length() > RegistryConstants
        .MAX_FQDN_LABEL_LENGTH)) {
      throw new IllegalArgumentException(String.format(
          RestApiErrorMessages.ERROR_APPLICATION_NAME_INVALID_FORMAT,
          application.getName()));
    }

    // If the application has no components do top-level checks
    if (!hasComponent(application)) {
      // If artifact is of type APPLICATION, read other application components
      if (application.getArtifact() != null && application.getArtifact()
          .getType() == Artifact.TypeEnum.APPLICATION) {
        if (StringUtils.isEmpty(application.getArtifact().getId())) {
          throw new IllegalArgumentException(
              RestApiErrorMessages.ERROR_ARTIFACT_ID_INVALID);
        }
        Application otherApplication = loadApplication(fs,
            application.getArtifact().getId());
        application.setComponents(otherApplication.getComponents());
        application.setArtifact(null);
        SliderUtils.mergeMapsIgnoreDuplicateKeys(application.getQuicklinks(),
            otherApplication.getQuicklinks());
      } else {
        // Since it is a simple app with no components, create a default
        // component
        Component comp = createDefaultComponent(application);
        validateComponent(comp, fs.getFileSystem());
        application.getComponents().add(comp);
        if (application.getLifetime() == null) {
          application.setLifetime(RestApiConstants.DEFAULT_UNLIMITED_LIFETIME);
        }
        return;
      }
    }

    // Validate there are no component name collisions (collisions are not
    // currently supported) and add any components from external applications
    // TODO allow name collisions? see AppState#roles
    // TODO or add prefix to external component names?
    Configuration globalConf = application.getConfiguration();
    Set<String> componentNames = new HashSet<>();
    List<Component> componentsToRemove = new ArrayList<>();
    List<Component> componentsToAdd = new ArrayList<>();
    for (Component comp : application.getComponents()) {
      int maxCompLength = RegistryConstants.MAX_FQDN_LABEL_LENGTH;
      maxCompLength = maxCompLength - Long.toString(Long.MAX_VALUE).length();
      if (dnsEnabled && comp.getName().length() > maxCompLength) {
        throw new IllegalArgumentException(String.format(RestApiErrorMessages
            .ERROR_COMPONENT_NAME_INVALID, maxCompLength, comp.getName()));
      }
      if (componentNames.contains(comp.getName())) {
        throw new IllegalArgumentException("Component name collision: " +
            comp.getName());
      }
      // If artifact is of type APPLICATION (which cannot be filled from
      // global), read external application and add its components to this
      // application
      if (comp.getArtifact() != null && comp.getArtifact().getType() ==
          Artifact.TypeEnum.APPLICATION) {
        if (StringUtils.isEmpty(comp.getArtifact().getId())) {
          throw new IllegalArgumentException(
              RestApiErrorMessages.ERROR_ARTIFACT_ID_INVALID);
        }
        LOG.info("Marking {} for removal", comp.getName());
        componentsToRemove.add(comp);
        List<Component> externalComponents = getApplicationComponents(fs,
            comp.getArtifact().getId());
        for (Component c : externalComponents) {
          Component override = application.getComponent(c.getName());
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
    application.getComponents().removeAll(componentsToRemove);
    application.getComponents().addAll(componentsToAdd);

    // Validate components and let global values take effect if component level
    // values are not provided
    Artifact globalArtifact = application.getArtifact();
    Resource globalResource = application.getResource();
    Long globalNumberOfContainers = application.getNumberOfContainers();
    String globalLaunchCommand = application.getLaunchCommand();
    for (Component comp : application.getComponents()) {
      // fill in global artifact unless it is type APPLICATION
      if (comp.getArtifact() == null && application.getArtifact() != null
          && application.getArtifact().getType() != Artifact.TypeEnum
          .APPLICATION) {
        comp.setArtifact(globalArtifact);
      }
      // fill in global resource
      if (comp.getResource() == null) {
        comp.setResource(globalResource);
      }
      // fill in global container count
      if (comp.getNumberOfContainers() == null) {
        comp.setNumberOfContainers(globalNumberOfContainers);
      }
      // fill in global launch command
      if (comp.getLaunchCommand() == null) {
        comp.setLaunchCommand(globalLaunchCommand);
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
      validateComponent(comp, fs.getFileSystem());
    }

    // validate dependency tree
    sortByDependencies(application.getComponents());

    // Application lifetime if not specified, is set to unlimited lifetime
    if (application.getLifetime() == null) {
      application.setLifetime(RestApiConstants.DEFAULT_UNLIMITED_LIFETIME);
    }
  }

  public static void validateComponent(Component comp, FileSystem fs)
      throws IOException {
    validateCompName(comp.getName());

    AbstractClientProvider compClientProvider = ProviderFactory
        .getClientProvider(comp.getArtifact());
    compClientProvider.validateArtifact(comp.getArtifact(), fs);

    if (comp.getLaunchCommand() == null && (comp.getArtifact() == null || comp
        .getArtifact().getType() != Artifact.TypeEnum.DOCKER)) {
      throw new IllegalArgumentException(RestApiErrorMessages
          .ERROR_ABSENT_LAUNCH_COMMAND);
    }

    validateApplicationResource(comp.getResource(), comp);

    if (comp.getNumberOfContainers() == null
        || comp.getNumberOfContainers() < 0) {
      throw new IllegalArgumentException(String.format(
          RestApiErrorMessages.ERROR_CONTAINERS_COUNT_FOR_COMP_INVALID
              + ": " + comp.getNumberOfContainers(), comp.getName()));
    }
    compClientProvider.validateConfigFiles(comp.getConfiguration()
        .getFiles(), fs);

    MonitorUtils.getProbe(comp.getReadinessCheck());
  }

  // Check component name format and transform to lower case.
  public static void validateCompName(String compName) {
    if (StringUtils.isEmpty(compName)) {
      throw new IllegalArgumentException("Component name can not be empty");
    }
    // validate component name
    if (compName.contains("_")) {
      throw new IllegalArgumentException(
          "Invalid format for component name: " + compName
              + ", can not use '_' as DNS hostname does not allow underscore. Use '-' Instead. ");
    }
    compNamePattern.validate(compName);
  }

  @VisibleForTesting
  public static List<Component> getApplicationComponents(SliderFileSystem
      fs, String appName) throws IOException {
    return loadApplication(fs, appName).getComponents();
  }

  public static Application loadApplication(SliderFileSystem fs, String
      appName) throws IOException {
    Path appJson = getAppJsonPath(fs, appName);
    LOG.info("Loading application definition from " + appJson);
    return jsonSerDeser.load(fs.getFileSystem(), appJson);
  }

  public static Application loadApplicationFrom(SliderFileSystem fs,
      Path appDefPath) throws IOException {
    LOG.info("Loading application definition from " + appDefPath);
    return jsonSerDeser.load(fs.getFileSystem(), appDefPath);
  }

  public static Path getAppJsonPath(SliderFileSystem fs, String appName) {
    Path appDir = fs.buildClusterDirPath(appName);
    Path appJson = new Path(appDir, appName + ".json");
    return appJson;
  }

  private static void validateApplicationResource(Resource resource,
      Component comp) {
    // Only apps/components of type APPLICATION can skip resource requirement
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
      Application application) throws YarnException {
    for (Component component : application.getComponents()) {
      // only handle mem now.
      long mem = Long.parseLong(component.getResource().getMemory());
      if (mem > maxResource.getMemorySize()) {
        throw new YarnException(
            "Component " + component.getName() + " memory size (" + mem
                + ") is larger than configured max container memory size ("
                + maxResource.getMemorySize() + ")");
      }
    }
  }

  public static boolean hasComponent(Application application) {
    if (application.getComponents() == null || application.getComponents()
        .isEmpty()) {
      return false;
    }
    return true;
  }

  public static Component createDefaultComponent(Application app) {
    Component comp = new Component();
    comp.setName(RestApiConstants.DEFAULT_COMPONENT_NAME);
    comp.setArtifact(app.getArtifact());
    comp.setResource(app.getResource());
    comp.setNumberOfContainers(app.getNumberOfContainers());
    comp.setLaunchCommand(app.getLaunchCommand());
    comp.setConfiguration(app.getConfiguration());
    return comp;
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
      if (!SliderUtils.isEmpty(component.getDependencies())) {
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

  public static String $(String s) {
    return "${" + s +"}";
  }
}
