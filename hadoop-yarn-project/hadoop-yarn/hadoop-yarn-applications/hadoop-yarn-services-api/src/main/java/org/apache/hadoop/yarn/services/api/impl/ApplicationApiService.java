/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.services.api.impl;

import static org.apache.hadoop.yarn.services.utils.RestApiConstants.*;
import static org.apache.hadoop.yarn.services.utils.RestApiErrorMessages.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.services.api.ApplicationApi;
import org.apache.hadoop.yarn.services.resource.Application;
import org.apache.hadoop.yarn.services.resource.ApplicationState;
import org.apache.hadoop.yarn.services.resource.ApplicationStatus;
import org.apache.hadoop.yarn.services.resource.Artifact;
import org.apache.hadoop.yarn.services.resource.Component;
import org.apache.hadoop.yarn.services.resource.ConfigFile;
import org.apache.hadoop.yarn.services.resource.Configuration;
import org.apache.hadoop.yarn.services.resource.Container;
import org.apache.hadoop.yarn.services.resource.ContainerState;
import org.apache.hadoop.yarn.services.resource.Resource;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.StateValues;
import org.apache.slider.client.SliderClient;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.params.ActionCreateArgs;
import org.apache.slider.common.params.ActionFlexArgs;
import org.apache.slider.common.params.ActionFreezeArgs;
import org.apache.slider.common.params.ActionListArgs;
import org.apache.slider.common.params.ActionRegistryArgs;
import org.apache.slider.common.params.ActionThawArgs;
import org.apache.slider.common.params.ActionUpdateArgs;
import org.apache.slider.common.params.ComponentArgsDelegate;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.common.tools.SliderVersionInfo;
import org.apache.slider.core.buildutils.BuildHelper;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.NotFoundException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException;
import org.apache.slider.core.registry.docstore.ConfigFormat;
import org.apache.slider.providers.docker.DockerKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Singleton;

@Singleton
@Path(APPLICATIONS_API_RESOURCE_PATH)
@Consumes({ MediaType.APPLICATION_JSON })
@Produces({ MediaType.APPLICATION_JSON })
public class ApplicationApiService implements ApplicationApi {
  private static final Logger logger = LoggerFactory
      .getLogger(ApplicationApiService.class);
  private static org.apache.hadoop.conf.Configuration SLIDER_CONFIG;
  private static UserGroupInformation SLIDER_USER;
  private static SliderClient SLIDER_CLIENT;
  private static Response SLIDER_VERSION;
  private static final JsonParser JSON_PARSER = new JsonParser();
  private static final JsonObject EMPTY_JSON_OBJECT = new JsonObject();
  private static final ActionListArgs ACTION_LIST_ARGS = new ActionListArgs();
  private static final ActionFreezeArgs ACTION_FREEZE_ARGS = new ActionFreezeArgs();

  static {
    init();
  }

  // initialize all the common resources - order is important
  protected static void init() {
    SLIDER_CONFIG = getSliderClientConfiguration();
    SLIDER_USER = getSliderUser();
    SLIDER_CLIENT = createSliderClient();
    SLIDER_VERSION = initSliderVersion();
  }

  @GET
  @Path("/versions/slider-version")
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response getSliderVersion() {
    logger.info("GET: getSliderVersion");
    return SLIDER_VERSION;
  }

  private static Response initSliderVersion() {
    Map<String, Object> metadata = new HashMap<>();
    BuildHelper.addBuildMetadata(metadata, "org.apache.hadoop.yarn.services");
    String sliderVersion = metadata.toString();
    logger.info("Slider version = {}", sliderVersion);
    String hadoopVersion = SliderVersionInfo.getHadoopVersionString();
    logger.info("Hadoop version = {}", hadoopVersion);
    return Response.ok("{ \"slider_version\": \"" + sliderVersion
        + "\", \"hadoop_version\": \"" + hadoopVersion + "\"}").build();
  }

  @POST
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response createApplication(Application application) {
    logger.info("POST: createApplication for app = {}", application);
    ApplicationStatus applicationStatus = new ApplicationStatus();

    Map<String, String> compNameArtifactIdMap = new HashMap<>();
    // post payload validation
    try {
      validateApplicationPostPayload(application, compNameArtifactIdMap);
    } catch (IllegalArgumentException e) {
      applicationStatus.setDiagnostics(e.getMessage());
      return Response.status(Status.BAD_REQUEST).entity(applicationStatus)
          .build();
    }
    String applicationId = null;
    try {
      applicationId = createSliderApp(application, compNameArtifactIdMap);
      applicationStatus.setState(ApplicationState.ACCEPTED);
    } catch (SliderException se) {
      logger.error("Create application failed", se);
      if (se.getExitCode() == SliderExitCodes.EXIT_APPLICATION_IN_USE) {
        applicationStatus.setDiagnostics(ERROR_APPLICATION_IN_USE);
        return Response.status(Status.BAD_REQUEST).entity(applicationStatus)
            .build();
      } else if (se.getExitCode() == SliderExitCodes.EXIT_INSTANCE_EXISTS) {
        applicationStatus.setDiagnostics(ERROR_APPLICATION_INSTANCE_EXISTS);
        return Response.status(Status.BAD_REQUEST).entity(applicationStatus)
            .build();
      } else {
        applicationStatus.setDiagnostics(se.getMessage());
      }
    } catch (Exception e) {
      logger.error("Create application failed", e);
      applicationStatus.setDiagnostics(e.getMessage());
    }

    if (StringUtils.isNotEmpty(applicationId)) {
      applicationStatus.setUri(CONTEXT_ROOT + APPLICATIONS_API_RESOURCE_PATH
          + "/" + application.getName());
      // 202 = ACCEPTED
      return Response.status(HTTP_STATUS_CODE_ACCEPTED)
          .entity(applicationStatus).build();
    } else {
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(applicationStatus).build();
    }
  }

  @VisibleForTesting
  protected void validateApplicationPostPayload(Application application,
      Map<String, String> compNameArtifactIdMap) {
    if (StringUtils.isEmpty(application.getName())) {
      throw new IllegalArgumentException(ERROR_APPLICATION_NAME_INVALID);
    }
    if (!SliderUtils.isClusternameValid(application.getName())) {
      throw new IllegalArgumentException(ERROR_APPLICATION_NAME_INVALID_FORMAT);
    }

    // If the application has no components do top-level checks
    if (application.getComponents() == null
        || application.getComponents().size() == 0) {
      // artifact
      if (application.getArtifact() == null) {
        throw new IllegalArgumentException(ERROR_ARTIFACT_INVALID);
      }
      if (StringUtils.isEmpty(application.getArtifact().getId())) {
        throw new IllegalArgumentException(ERROR_ARTIFACT_ID_INVALID);
      }

      // If artifact is of type APPLICATION, add a slider specific property
      if (application.getArtifact().getType() == Artifact.TypeEnum.APPLICATION) {
        if (application.getConfiguration() == null) {
          application.setConfiguration(new Configuration());
        }
        addPropertyToConfiguration(application.getConfiguration(),
            SliderKeys.COMPONENT_TYPE_KEY,
            SliderKeys.COMPONENT_TYPE_EXTERNAL_APP);
      }
      // resource
      validateApplicationResource(application.getResource(), null, application
          .getArtifact().getType());

      // container size
      if (application.getNumberOfContainers() == null) {
        throw new IllegalArgumentException(ERROR_CONTAINERS_COUNT_INVALID);
      }

      // Since it is a simple app with no components, create a default component
      application.setComponents(getDefaultComponentAsList(application));
    } else {
      // If the application has components, then run checks for each component.
      // Let global values take effect if component level values are not
      // provided.
      Artifact globalArtifact = application.getArtifact();
      Resource globalResource = application.getResource();
      Long globalNumberOfContainers = application.getNumberOfContainers();
      for (Component comp : application.getComponents()) {
        // artifact
        if (comp.getArtifact() == null) {
          comp.setArtifact(globalArtifact);
        }
        // If still null raise validation exception
        if (comp.getArtifact() == null) {
          throw new IllegalArgumentException(String.format(
              ERROR_ARTIFACT_FOR_COMP_INVALID, comp.getName()));
        }
        if (StringUtils.isEmpty(comp.getArtifact().getId())) {
          throw new IllegalArgumentException(String.format(
              ERROR_ARTIFACT_ID_FOR_COMP_INVALID, comp.getName()));
        }

        // If artifact is of type APPLICATION, add a slider specific property
        if (comp.getArtifact().getType() == Artifact.TypeEnum.APPLICATION) {
          if (comp.getConfiguration() == null) {
            comp.setConfiguration(new Configuration());
          }
          addPropertyToConfiguration(comp.getConfiguration(),
              SliderKeys.COMPONENT_TYPE_KEY,
              SliderKeys.COMPONENT_TYPE_EXTERNAL_APP);
          compNameArtifactIdMap.put(comp.getName(), comp.getArtifact().getId());
          comp.setName(comp.getArtifact().getId());
        }

        // resource
        if (comp.getResource() == null) {
          comp.setResource(globalResource);
        }
        validateApplicationResource(comp.getResource(), comp, comp
            .getArtifact().getType());

        // container count
        if (comp.getNumberOfContainers() == null) {
          comp.setNumberOfContainers(globalNumberOfContainers);
        }
        if (comp.getNumberOfContainers() == null) {
          throw new IllegalArgumentException(String.format(
              ERROR_CONTAINERS_COUNT_FOR_COMP_INVALID, comp.getName()));
        }
      }
    }

    // Application lifetime if not specified, is set to unlimited lifetime
    if (application.getLifetime() == null) {
      application.setLifetime(DEFAULT_UNLIMITED_LIFETIME);
    }
  }

  private void validateApplicationResource(Resource resource, Component comp,
      Artifact.TypeEnum artifactType) {
    // Only apps/components of type APPLICATION can skip resource requirement
    if (resource == null && artifactType == Artifact.TypeEnum.APPLICATION) {
      return;
    }
    if (resource == null) {
      throw new IllegalArgumentException(comp == null ? ERROR_RESOURCE_INVALID
          : String.format(ERROR_RESOURCE_FOR_COMP_INVALID, comp.getName()));
    }
    // One and only one of profile OR cpus & memory can be specified. Specifying
    // both raises validation error.
    if (StringUtils.isNotEmpty(resource.getProfile())
        && (resource.getCpus() != null
            || StringUtils.isNotEmpty(resource.getMemory()))) {
      throw new IllegalArgumentException(
          comp == null ? ERROR_RESOURCE_PROFILE_MULTIPLE_VALUES_NOT_SUPPORTED
              : String.format(
                  ERROR_RESOURCE_PROFILE_MULTIPLE_VALUES_FOR_COMP_NOT_SUPPORTED,
                  comp.getName()));
    }
    // Currently resource profile is not supported yet, so we will raise
    // validation error if only resource profile is specified
    if (StringUtils.isNotEmpty(resource.getProfile())) {
      throw new IllegalArgumentException(
          ERROR_RESOURCE_PROFILE_NOT_SUPPORTED_YET);
    }

    String memory = resource.getMemory();
    Integer cpus = resource.getCpus();
    if (StringUtils.isEmpty(memory)) {
      throw new IllegalArgumentException(
          comp == null ? ERROR_RESOURCE_MEMORY_INVALID : String.format(
              ERROR_RESOURCE_MEMORY_FOR_COMP_INVALID, comp.getName()));
    }
    if (cpus == null) {
      throw new IllegalArgumentException(
          comp == null ? ERROR_RESOURCE_CPUS_INVALID : String.format(
              ERROR_RESOURCE_CPUS_FOR_COMP_INVALID, comp.getName()));
    }
    if (cpus <= 0) {
      throw new IllegalArgumentException(
          comp == null ? ERROR_RESOURCE_CPUS_INVALID_RANGE : String.format(
              ERROR_RESOURCE_CPUS_FOR_COMP_INVALID_RANGE, comp.getName()));
    }
  }

  private String createSliderApp(Application application,
      Map<String, String> compNameArtifactIdMap) throws IOException,
      YarnException, InterruptedException {
    final String appName = application.getName();
    final String queueName = application.getQueue();

    final ActionCreateArgs createArgs = new ActionCreateArgs();
    addAppConfOptions(createArgs, application, compNameArtifactIdMap);
    addResourceOptions(createArgs, application);

    createArgs.provider = DockerKeys.PROVIDER_DOCKER;

    if (queueName != null && queueName.trim().length() > 0) {
      createArgs.queue = queueName.trim();
    }
    createArgs.lifetime = application.getLifetime();
    return invokeSliderClientRunnable(new SliderClientContextRunnable<String>() {
      @Override
      public String run(SliderClient sliderClient) throws YarnException,
          IOException, InterruptedException {
        sliderClient.actionCreate(appName, createArgs);
        ApplicationId applicationId = sliderClient.applicationId;
        if (applicationId != null) {
          return applicationId.toString();
          // return getApplicationIdString(applicationId);
        }
        return null;
      }
    });
  }

  private void addAppConfOptions(ActionCreateArgs createArgs,
      Application application, Map<String, String> compNameArtifactIdMap) throws IOException {
    List<String> appCompOptionTriples = createArgs.optionsDelegate.compOptTriples; // TODO: optionTuples instead of compOptTriples
    logger.info("Initial appCompOptionTriples = {}",
        Arrays.toString(appCompOptionTriples.toArray()));
    List<String> appOptions = createArgs.optionsDelegate.optionTuples;
    logger.info("Initial appOptions = {}",
        Arrays.toString(appOptions.toArray()));
    // TODO: Set Slider-AM memory and vcores here
    //    appCompOptionTriples.addAll(Arrays.asList(SLIDER_APPMASTER_COMPONENT_NAME,
    //        "", ""));

    // Global configuration - for override purpose
    // TODO: add it to yaml
    Configuration globalConfig = null;
    //    Configuration globalConfig = (Configuration) SerializationUtils
    //        .clone(application.getConfiguration());

    // TODO: Add the below into globalConfig
    //    if (application.getConfigurations() != null) {
    //      for (Entry<String, String> entry : application.getConfigurations()
    //          .entrySet()) {
    //        globalConf.addProperty(entry.getKey(), entry.getValue());
    //      }
    //    }

    Set<String> uniqueGlobalPropertyCache = new HashSet<>();
    if (application.getConfiguration() != null) {
      if (application.getConfiguration().getProperties() != null) {
        for (Map.Entry<String, String> propEntry : application
            .getConfiguration().getProperties().entrySet()) {
          addOptionsIfNotPresent(appOptions, uniqueGlobalPropertyCache,
              propEntry.getKey(), propEntry.getValue());
        }
      }
      List<ConfigFile> configFiles = application.getConfiguration().getFiles();
      if (configFiles != null && !configFiles.isEmpty()) {
        addOptionsIfNotPresent(appOptions, uniqueGlobalPropertyCache,
            SliderKeys.AM_CONFIG_GENERATION, "true");
        for (ConfigFile configFile : configFiles) {
          addOptionsIfNotPresent(appOptions, uniqueGlobalPropertyCache,
              OptionKeys.CONF_FILE_PREFIX + configFile.getSrcFile() +
                  OptionKeys.NAME_SUFFIX, configFile.getDestFile());
          addOptionsIfNotPresent(appOptions, uniqueGlobalPropertyCache,
              OptionKeys.CONF_FILE_PREFIX + configFile.getSrcFile() +
                  OptionKeys.TYPE_SUFFIX, configFile.getType().toString());
        }
      }
    }
    if (application.getComponents() != null) {

      Map<String, String> appQuicklinks = application.getQuicklinks();
      if (appQuicklinks != null) {
        for (Map.Entry<String, String> quicklink : appQuicklinks.entrySet()) {
          addOptionsIfNotPresent(appOptions, uniqueGlobalPropertyCache,
              OptionKeys.EXPORT_PREFIX + quicklink.getKey(),
              quicklink.getValue());
        }
      }

      Map<String, String> placeholders = new HashMap<>();
      placeholders.put(PLACEHOLDER_APP_NAME, application.getName());
      for (Component comp : application.getComponents()) {
        placeholders.put(PLACEHOLDER_APP_COMPONENT_NAME, comp.getName());
        if (comp.getArtifact().getType() == Artifact.TypeEnum.DOCKER) {
          appCompOptionTriples.addAll(Arrays.asList(comp.getName(),
              DockerKeys.DOCKER_IMAGE, comp.getArtifact().getId() == null ?
              application.getArtifact().getId() : comp.getArtifact().getId()));
          appCompOptionTriples.addAll(Arrays.asList(comp.getName(),
              DockerKeys.DOCKER_START_COMMAND, comp.getLaunchCommand() == null ?
              replacePlaceholders(application.getLaunchCommand(), placeholders)
              : replacePlaceholders(comp.getLaunchCommand(), placeholders)));
          appCompOptionTriples.addAll(Arrays.asList(comp.getName(),
              DockerKeys.DOCKER_NETWORK, DockerKeys.DEFAULT_DOCKER_NETWORK));
          if (comp.getRunPrivilegedContainer() != null) {
            appCompOptionTriples.addAll(Arrays.asList(comp.getName(),
                DockerKeys.DOCKER_USE_PRIVILEGED,
                comp.getRunPrivilegedContainer().toString()));
          }
        }

        if (comp.getConfiguration() != null) {
          List<ConfigFile> configFiles = comp.getConfiguration().getFiles();
          if (configFiles != null && !configFiles.isEmpty()) {
            appCompOptionTriples.addAll(Arrays.asList(comp.getName(),
                SliderKeys.AM_CONFIG_GENERATION, "true"));
            for (ConfigFile configFile : configFiles) {
              appCompOptionTriples.addAll(Arrays.asList(comp.getName(),
                  OptionKeys.CONF_FILE_PREFIX + configFile.getSrcFile() +
                      OptionKeys.NAME_SUFFIX, configFile.getDestFile()));
              appCompOptionTriples.addAll(Arrays.asList(comp.getName(),
                  OptionKeys.CONF_FILE_PREFIX + configFile.getSrcFile() +
                  OptionKeys.TYPE_SUFFIX, configFile.getType().toString()));
            }
          }
        }

        if (Boolean.TRUE.equals(comp.getUniqueComponentSupport())) {
          for (int i = 1; i <= comp.getNumberOfContainers(); i++) {
            placeholders.put(PLACEHOLDER_COMPONENT_ID, Integer.toString(i));
            appCompOptionTriples.addAll(createAppConfigComponent(
                comp.getName() + i, comp, comp.getName() + i, globalConfig,
                placeholders, compNameArtifactIdMap));
          }
        } else {
          appCompOptionTriples.addAll(createAppConfigComponent(comp.getName(),
              comp, comp.getName(), globalConfig, null, compNameArtifactIdMap));
        }
      }
    }

    logger.info("Updated appCompOptionTriples = {}",
        Arrays.toString(appCompOptionTriples.toArray()));
    logger.info("Updated appOptions = {}",
        Arrays.toString(appOptions.toArray()));
  }

  private void addOptionsIfNotPresent(List<String> options,
      Set<String> uniqueGlobalPropertyCache, String key, String value) {
    if (uniqueGlobalPropertyCache == null) {
      options.addAll(Arrays.asList(key, value));
    } else if (!uniqueGlobalPropertyCache.contains(key)) {
      options.addAll(Arrays.asList(key, value));
      uniqueGlobalPropertyCache.add(key);
    }
  }

  private void addPropertyToConfiguration(Configuration conf, String key,
      String value) {
    if (conf == null) {
      return;
    }
    if (conf.getProperties() == null) {
      conf.setProperties(new HashMap<String, String>());
    }
    conf.getProperties().put(key, value);
  }

  private List<String> createAppConfigComponent(String compName,
      Component component, String configPrefix, Configuration globalConf,
      Map<String, String> placeholders,
      Map<String, String> compNameArtifactIdMap) {
    List<String> appConfOptTriples = new ArrayList<>();

    if (component.getConfiguration() != null
        && component.getConfiguration().getProperties() != null) {
      for (Map.Entry<String, String> propEntry : component.getConfiguration()
          .getProperties().entrySet()) {
        appConfOptTriples.addAll(Arrays.asList(compName, propEntry.getKey(),
            replacePlaceholders(propEntry.getValue(), placeholders)));
      }
    }

    // If artifact is of type APPLICATION, then in the POST JSON there will
    // be no component definition for that artifact. Hence it's corresponding id
    // field is added. Every external APPLICATION has a unique id field.
    List<String> convertedDeps = new ArrayList<>();
    for (String dep : component.getDependencies()) {
      if (compNameArtifactIdMap.containsKey(dep)) {
        convertedDeps.add(compNameArtifactIdMap.get(dep));
      } else {
        convertedDeps.add(dep);
      }
    }
    // If the DNS dependency property is set to true for a component, it means
    // that it is ensured that DNS entry has been added for all the containers
    // of this component, before moving on to the next component in the DAG.
    if (hasPropertyWithValue(component, PROPERTY_DNS_DEPENDENCY, "true")) {
      if (component.getArtifact().getType() == Artifact.TypeEnum.APPLICATION) {
        convertedDeps.add(component.getArtifact().getId());
      } else {
        convertedDeps.add(compName);
      }
    }
    if (convertedDeps.size() > 0) {
      appConfOptTriples.addAll(Arrays.asList(compName, "requires",
          StringUtils.join(convertedDeps, ",")));
    }
    return appConfOptTriples;
  }

  private String replacePlaceholders(String value,
      Map<String, String> placeholders) {
    if (StringUtils.isEmpty(value) || placeholders == null) {
      return value;
    }
    for (Map.Entry<String, String> placeholder : placeholders.entrySet()) {
      value = value.replaceAll(Pattern.quote(placeholder.getKey()),
          placeholder.getValue());
    }
    return value;
  }

  private List<String> createAppConfigGlobal(Component component,
      Configuration globalConf, Set<String> uniqueGlobalPropertyCache) {
    List<String> appOptions = new ArrayList<>();
    if (component.getConfiguration() != null
        && component.getConfiguration().getProperties() != null) {
      for (Map.Entry<String, String> propEntry : component.getConfiguration()
          .getProperties().entrySet()) {
        addOptionsIfNotPresent(appOptions, uniqueGlobalPropertyCache,
            propEntry.getKey(), propEntry.getValue());
      }
    }
    return appOptions;
  }

  private void addResourceOptions(ActionCreateArgs createArgs,
      Application application) throws IOException {
    List<String> resCompOptionTriples = createArgs.optionsDelegate.resCompOptTriples;
    logger.info("Initial resCompOptTriples = {}",
        Arrays.toString(resCompOptionTriples.toArray()));
    // TODO: Add any Slider AM resource specific props here like jvm.heapsize
    //    resCompOptionTriples.addAll(Arrays.asList(SLIDER_APPMASTER_COMPONENT_NAME,
    //        "", ""));

    // Global resource - for override purpose
    Resource globalResource = (Resource) SerializationUtils.clone(application
        .getResource());
    // Priority seeded with 1, expecting every new component will increase it by
    // 1 making it ready for the next component to use.
    if (application.getComponents() != null) {
      int priority = 1;
      for (Component comp : application.getComponents()) {
        if (hasPropertyWithValue(comp, SliderKeys.COMPONENT_TYPE_KEY,
            SliderKeys.COMPONENT_TYPE_EXTERNAL_APP)) {
          continue;
        }
        if (Boolean.TRUE.equals(comp.getUniqueComponentSupport())) {
          for (int i = 1; i <= comp.getNumberOfContainers(); i++) {
            resCompOptionTriples.addAll(createResourcesComponent(comp.getName()
                + i, comp, priority, 1, globalResource));
            priority++;
          }
        } else {
          resCompOptionTriples.addAll(createResourcesComponent(comp.getName(),
              comp, priority, comp.getNumberOfContainers(), globalResource));
          priority++;
        }
      }
    }

    logger.info("Updated resCompOptTriples = {}",
        Arrays.toString(resCompOptionTriples.toArray()));
  }

  private boolean hasPropertyWithValue(Component comp, String key, String value) {
    if (comp == null || key == null) {
      return false;
    }
    if (comp.getConfiguration() == null
        || comp.getConfiguration().getProperties() == null) {
      return false;
    }
    Map<String, String> props = comp.getConfiguration().getProperties();
    if (props.containsKey(key)) {
      if (value == null) {
        return props.get(key) == null;
      } else {
        if (value.equals(props.get(key))) {
          return true;
        }
      }
    }
    return false;
  }

  private List<String> createResourcesComponent(String compName,
      Component component, int priority, long numInstances,
      Resource globalResource) {
    String memory = component.getResource() == null ? globalResource
        .getMemory() : component.getResource().getMemory();
    Integer cpus = component.getResource() == null ? globalResource.getCpus()
        : component.getResource().getCpus();

    List<String> resCompOptTriples = new ArrayList<String>();
    resCompOptTriples.addAll(Arrays.asList(compName,
        ResourceKeys.COMPONENT_PRIORITY, Integer.toString(priority)));
    resCompOptTriples.addAll(Arrays.asList(compName,
        ResourceKeys.COMPONENT_INSTANCES, Long.toString(numInstances)));
    resCompOptTriples.addAll(Arrays.asList(compName, ResourceKeys.YARN_MEMORY,
        memory));
    resCompOptTriples.addAll(Arrays.asList(compName, ResourceKeys.YARN_CORES,
        cpus.toString()));
    if (component.getPlacementPolicy() != null) {
      resCompOptTriples.addAll(Arrays.asList(compName,
          ResourceKeys.COMPONENT_PLACEMENT_POLICY,
          component.getPlacementPolicy().getLabel()));
    }

    return resCompOptTriples;
  }

  private static UserGroupInformation getSliderUser() {
    if (SLIDER_USER != null) {
      return SLIDER_USER;
    }
    UserGroupInformation sliderUser = null;
    UserGroupInformation.setConfiguration(SLIDER_CONFIG);
    String loggedInUser = getUserToRunAs();
    try {
      sliderUser = UserGroupInformation.getBestUGI(null, loggedInUser);
      // TODO: Once plugged into RM process we should remove the previous call
      // and replace it with getCurrentUser as commented below.
      // sliderUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException("Unable to create UGI (slider user)", e);
    }
    return sliderUser;
  }

  private <T> T invokeSliderClientRunnable(
      final SliderClientContextRunnable<T> runnable)
      throws IOException, InterruptedException, YarnException {
    try {
      T value = SLIDER_USER.doAs(new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          return runnable.run(SLIDER_CLIENT);
        }
      });
      return value;
    } catch (UndeclaredThrowableException e) {
      Throwable cause = e.getCause();
      if (cause instanceof YarnException) {
        YarnException ye = (YarnException) cause;
        throw ye;
      }
      throw e;
    }
  }

  protected static SliderClient createSliderClient() {
    if (SLIDER_CLIENT != null) {
      return SLIDER_CLIENT;
    }
    org.apache.hadoop.conf.Configuration sliderClientConfiguration = SLIDER_CONFIG;
    SliderClient client = new SliderClient() {
      @Override
      public void init(org.apache.hadoop.conf.Configuration conf) {
        super.init(conf);
        try {
          initHadoopBinding();
        } catch (SliderException e) {
          throw new RuntimeException(
              "Unable to automatically init Hadoop binding", e);
        } catch (IOException e) {
          throw new RuntimeException(
              "Unable to automatically init Hadoop binding", e);
        }
      }
    };
    try {
      logger
          .debug("Slider Client configuration: {}", sliderClientConfiguration);
      sliderClientConfiguration = client.bindArgs(sliderClientConfiguration,
          new String[] { "help" });
      client.init(sliderClientConfiguration);
      client.start();
    } catch (Exception e) {
      logger.error("Unable to create SliderClient", e);
      throw new RuntimeException(e.getMessage(), e);
    }
    return client;
  }

  private static String getUserToRunAs() {
    String user = System.getenv(PROPERTY_APP_RUNAS_USER);
    if (StringUtils.isEmpty(user)) {
      user = "root";
    }
    return user;
  }

  private static org.apache.hadoop.conf.Configuration getSliderClientConfiguration() {
    if (SLIDER_CONFIG != null) {
      return SLIDER_CONFIG;
    }
    YarnConfiguration yarnConfig = new YarnConfiguration();
    logger.info("prop yarn.resourcemanager.address = {}",
        yarnConfig.get("yarn.resourcemanager.address"));

    return yarnConfig;
  }

  private interface SliderClientContextRunnable<T> {
    T run(SliderClient sliderClient)
        throws YarnException, IOException, InterruptedException;
  }

  @GET
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response getApplications(@QueryParam("state") String state) {
    logger.info("GET: getApplications with param state = {}", state);

    // Get all applications in a specific state - lighter projection. For full
    // detail, call getApplication on a specific app.
    Set<ApplicationReport> applications;
    try {
      if (StringUtils.isNotEmpty(state)) {
        ApplicationStatus appStatus = new ApplicationStatus();
        try {
          ApplicationState.valueOf(state);
        } catch (IllegalArgumentException e) {
          appStatus.setDiagnostics("Invalid value for param state - " + state);
          return Response.status(Status.BAD_REQUEST).entity(appStatus).build();
        }
        applications = getSliderApplications(state);
      } else {
        applications = getSliderApplications(true);
      }
    } catch (Exception e) {
      logger.error("Get applications failed", e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).build();
    }

    Set<Application> apps = new HashSet<Application>();
    if (applications.size() > 0) {
      try {
        for (ApplicationReport app : applications) {
          Application application = new Application();
          application.setLifetime(app.getApplicationTimeouts().get(
              ApplicationTimeoutType.LIFETIME).getRemainingTime());
          application.setLaunchTime(new Date(app.getStartTime()));
          application.setName(app.getName());
          // Containers not required, setting to null to avoid empty list
          application.setContainers(null);
          apps.add(application);
        }
      } catch (Exception e) {
        logger.error("Get applications failed", e);
        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
      }
    }

    return Response.ok().entity(apps).build();
  }

  @GET
  @Path("/{app_name}")
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response getApplication(@PathParam("app_name") String appName) {
    logger.info("GET: getApplication for appName = {}", appName);

    // app name validation
    if (!SliderUtils.isClusternameValid(appName)) {
      ApplicationStatus applicationStatus = new ApplicationStatus();
      applicationStatus.setDiagnostics("Invalid application name");
      applicationStatus.setCode(ERROR_CODE_APP_NAME_INVALID);
      return Response.status(Status.NOT_FOUND).entity(applicationStatus)
          .build();
    }

    // Check if app exists
    try {
      int livenessCheck = getSliderList(appName);
      if (livenessCheck < 0) {
        logger.info("Application not running");
        ApplicationStatus applicationStatus = new ApplicationStatus();
        applicationStatus.setDiagnostics(ERROR_APPLICATION_NOT_RUNNING);
        applicationStatus.setCode(ERROR_CODE_APP_IS_NOT_RUNNING);
        return Response.status(Status.NOT_FOUND).entity(applicationStatus)
            .build();
      }
    } catch (UnknownApplicationInstanceException e) {
      logger.error("Get application failed, application not found", e);
      ApplicationStatus applicationStatus = new ApplicationStatus();
      applicationStatus.setDiagnostics(ERROR_APPLICATION_DOES_NOT_EXIST);
      applicationStatus.setCode(ERROR_CODE_APP_DOES_NOT_EXIST);
      return Response.status(Status.NOT_FOUND).entity(applicationStatus)
          .build();
    } catch (Exception e) {
      logger.error("Get application failed, application not running", e);
      ApplicationStatus applicationStatus = new ApplicationStatus();
      applicationStatus.setDiagnostics(ERROR_APPLICATION_NOT_RUNNING);
      applicationStatus.setCode(ERROR_CODE_APP_IS_NOT_RUNNING);
      return Response.status(Status.NOT_FOUND).entity(applicationStatus)
          .build();
    }

    Application app = new Application();
    app.setName(appName);
    app.setUri(CONTEXT_ROOT + APPLICATIONS_API_RESOURCE_PATH + "/"
        + appName);
    // TODO: add status
    app.setState(ApplicationState.ACCEPTED);
    JsonObject appStatus = null;
    JsonObject appRegistryQuicklinks = null;
    try {
      appStatus = getSliderApplicationStatus(appName);
      appRegistryQuicklinks = getSliderApplicationRegistry(appName,
          "quicklinks");
      return populateAppData(app, appStatus, appRegistryQuicklinks);
    } catch (BadClusterStateException | NotFoundException e) {
      logger.error(
          "Get application failed, application not in running state yet", e);
      ApplicationStatus applicationStatus = new ApplicationStatus();
      applicationStatus.setDiagnostics("Application not running yet");
      applicationStatus.setCode(ERROR_CODE_APP_SUBMITTED_BUT_NOT_RUNNING_YET);
      return Response.status(Status.NOT_FOUND).entity(applicationStatus)
          .build();
    } catch (Exception e) {
      logger.error("Get application failed", e);
      ApplicationStatus applicationStatus = new ApplicationStatus();
      applicationStatus.setDiagnostics("Failed to retrieve application: "
          + e.getMessage());
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(applicationStatus).build();
    }
  }

  private Response populateAppData(Application app, JsonObject appStatus,
      JsonObject appRegistryQuicklinks) {
    String appName = jsonGetAsString(appStatus, "name");
    Long totalNumberOfRunningContainers = 0L;
    Long totalExpectedNumberOfRunningContainers = 0L;
    Long totalNumberOfIpAssignedContainers = 0L;

    // info
    JsonObject applicationInfo = jsonGetAsObject(appStatus, "info");
    if (applicationInfo != null) {
      String applicationId = jsonGetAsString(applicationInfo, "info.am.app.id");
      if (applicationId != null) {
        app.setId(applicationId);
      }
    }

    // state
    String appState = jsonGetAsString(appStatus, "state");
    if (appState == null) {
      // consider that app is still in ACCEPTED state
      appState = String.valueOf(StateValues.STATE_INCOMPLETE);
    }
    switch (Integer.parseInt(appState)) {
      case StateValues.STATE_LIVE:
        app.setState(ApplicationState.STARTED);
        break;
      case StateValues.STATE_CREATED:
      case StateValues.STATE_INCOMPLETE:
      case StateValues.STATE_SUBMITTED:
        app.setState(ApplicationState.ACCEPTED);
        return Response.ok(app).build();
      case StateValues.STATE_DESTROYED:
      case StateValues.STATE_STOPPED:
        app.setState(ApplicationState.STOPPED);
        return Response.ok(app).build();
      default:
        break;
    }

    // start time
    app.setLaunchTime(appStatus.get("createTime") == null ? null
        : new Date(appStatus.get("createTime").getAsLong()));

    app.setLifetime(queryLifetime(appName));

    // Quicklinks
    Map<String, String> appQuicklinks = new HashMap<>();
    for (Map.Entry<String, JsonElement> quicklink : appRegistryQuicklinks
        .entrySet()) {
      appQuicklinks.put(quicklink.getKey(), quicklink.getValue() == null ? null
          : quicklink.getValue().getAsString());
    }
    if (!appQuicklinks.isEmpty()) {
      app.setQuicklinks(appQuicklinks);
    }

    ArrayList<String> componentNames = new ArrayList<>();

    // status.live
    JsonObject applicationStatus = jsonGetAsObject(appStatus, "status");
    // roles
    JsonObject applicationRoles = jsonGetAsObject(appStatus, "roles");
    // statistics
    JsonObject applicationStatistics = jsonGetAsObject(appStatus, "statistics");
    if (applicationRoles == null) {
      // initialize to empty object to avoid too many null checks
      applicationRoles = EMPTY_JSON_OBJECT;
    }
    if (applicationStatus != null) {
      JsonObject applicationLive = jsonGetAsObject(applicationStatus, "live");
      if (applicationLive != null) {
        for (Entry<String, JsonElement> entry : applicationLive.entrySet()) {
          if (entry.getKey().equals(SLIDER_APPMASTER_COMPONENT_NAME)) {
            continue;
          }
          componentNames.add(entry.getKey());
          JsonObject componentRole = applicationRoles
              .get(entry.getKey()) == null ? EMPTY_JSON_OBJECT
                  : applicationRoles.get(entry.getKey()).getAsJsonObject();
          JsonObject liveContainers = entry.getValue().getAsJsonObject();
          if (liveContainers != null) {
            for (Map.Entry<String, JsonElement> liveContainerEntry : liveContainers
                .entrySet()) {
              String containerId = liveContainerEntry.getKey();
              Container container = new Container();
              container.setId(containerId);
              JsonObject liveContainer = (JsonObject) liveContainerEntry
                  .getValue();
              container
                  .setLaunchTime(liveContainer.get("startTime") == null ? null
                      : new Date(liveContainer.get("startTime").getAsLong()));
              container
                  .setComponentName(jsonGetAsString(liveContainer, "role"));
              container.setIp(jsonGetAsString(liveContainer, "ip"));
              // If ip is non-null increment count
              if (container.getIp() != null) {
                totalNumberOfIpAssignedContainers++;
              }
              container.setHostname(jsonGetAsString(liveContainer, "hostname"));
              container.setState(ContainerState.INIT);
              if (StringUtils.isNotEmpty(container.getIp())
                  && StringUtils.isNotEmpty(container.getHostname())) {
                container.setState(ContainerState.READY);
              }
              container.setBareHost(jsonGetAsString(liveContainer, "host"));
              container.setUri(CONTEXT_ROOT + APPLICATIONS_API_RESOURCE_PATH
                  + "/" + appName + CONTAINERS_API_RESOURCE_PATH + "/"
                  + containerId);
              Resource resource = new Resource();
              resource.setCpus(jsonGetAsInt(componentRole, "yarn.vcores"));
              resource.setMemory(jsonGetAsString(componentRole, "yarn.memory"));
              container.setResource(resource);
              Artifact artifact = new Artifact();
              String dockerImageName = jsonGetAsString(componentRole,
                  "docker.image");
              if (StringUtils.isNotEmpty(dockerImageName)) {
                artifact.setId(dockerImageName);
                artifact.setType(Artifact.TypeEnum.DOCKER);
              } else {
                // Might have to handle tarballs here
                artifact.setType(null);
              }
              container.setArtifact(artifact);
              container.setPrivilegedContainer(
                  jsonGetAsBoolean(componentRole, "docker.usePrivileged"));
              // TODO: add container property - for response only?
              app.addContainer(container);
            }
          }
        }
      }
    }

    // application info
    if (applicationRoles != null && !componentNames.isEmpty()) {
      JsonObject applicationRole = jsonGetAsObject(applicationRoles,
          componentNames.get(0));
      if (applicationRole != null) {
        Artifact artifact = new Artifact();
        // how to get artifact id - docker image name??
        artifact.setId(null);
      }
    }

    // actual and expected number of containers
    if (applicationStatistics != null) {
      for (Entry<String, JsonElement> entry : applicationStatistics.entrySet()) {
        if (entry.getKey().equals(SLIDER_APPMASTER_COMPONENT_NAME)) {
          continue;
        }
        JsonObject containerStats = (JsonObject) entry.getValue();
        totalNumberOfRunningContainers += jsonGetAsInt(containerStats,
            "containers.live");
        totalExpectedNumberOfRunningContainers += jsonGetAsInt(containerStats,
            "containers.desired");
      }
      app.setNumberOfContainers(totalExpectedNumberOfRunningContainers);
      app.setNumberOfRunningContainers(totalNumberOfRunningContainers);
    }

    // If all containers of the app has IP assigned, then according to the REST
    // API it is considered to be READY. Note, application readiness from
    // end-users point of view, is out of scope of the REST API. Also, this
    // readiness has nothing to do with readiness-check defined at the component
    // level (which is used for dependency resolution of component DAG).
    if (totalNumberOfIpAssignedContainers
        .longValue() == totalExpectedNumberOfRunningContainers.longValue()) {
      app.setState(ApplicationState.READY);
    }
    logger.info("Application = {}", app);
    return Response.ok(app).build();
  }

  private String jsonGetAsString(JsonObject object, String key) {
    return object.get(key) == null ? null : object.get(key).getAsString();
  }

  private Integer jsonGetAsInt(JsonObject object, String key) {
    return object.get(key) == null ? null
        : object.get(key).isJsonNull() ? null : object.get(key).getAsInt();
  }

  private Boolean jsonGetAsBoolean(JsonObject object, String key) {
    return object.get(key) == null ? null
        : object.get(key).isJsonNull() ? null : object.get(key).getAsBoolean();
  }

  private JsonObject jsonGetAsObject(JsonObject object, String key) {
    return object.get(key) == null ? null : object.get(key).getAsJsonObject();
  }

  private long queryLifetime(String appName) {
    try {
      return invokeSliderClientRunnable(
          new SliderClientContextRunnable<Long>() {
            @Override
            public Long run(SliderClient sliderClient)
                throws YarnException, IOException, InterruptedException {
              ApplicationReport report = sliderClient.findInstance(appName);
              return report.getApplicationTimeouts()
                  .get(ApplicationTimeoutType.LIFETIME).getRemainingTime();
            }
          });
    } catch (Exception e) {
      logger.error("Error when querying lifetime for " + appName, e);
      return DEFAULT_UNLIMITED_LIFETIME;
    }
  }

  private JsonObject getSliderApplicationStatus(final String appName)
      throws IOException, YarnException, InterruptedException {

    return invokeSliderClientRunnable(
        new SliderClientContextRunnable<JsonObject>() {
          @Override
          public JsonObject run(SliderClient sliderClient)
              throws YarnException, IOException, InterruptedException {
            String status = null;
            try {
              status = sliderClient.actionStatus(appName);
            } catch (BadClusterStateException e) {
              logger.warn("Application not running yet", e);
              return EMPTY_JSON_OBJECT;
            } catch (Exception e) {
              logger.error("Exception calling slider.actionStatus", e);
              return EMPTY_JSON_OBJECT;
            }
            JsonElement statusElement = JSON_PARSER.parse(status);
            return (statusElement == null || statusElement instanceof JsonNull)
                ? EMPTY_JSON_OBJECT : (JsonObject) statusElement;
          }
        });
  }

  private JsonObject getSliderApplicationRegistry(final String appName,
      final String registryName)
      throws IOException, YarnException, InterruptedException {
    final ActionRegistryArgs registryArgs = new ActionRegistryArgs();
    registryArgs.name = appName;
    registryArgs.getConf = registryName;
    registryArgs.format = ConfigFormat.JSON.toString();

    return invokeSliderClientRunnable(
        new SliderClientContextRunnable<JsonObject>() {
          @Override
          public JsonObject run(SliderClient sliderClient)
              throws YarnException, IOException, InterruptedException {
            String registry = null;
            try {
              registry = sliderClient.actionRegistryGetConfig(registryArgs)
                .asJson();
            } catch (FileNotFoundException | NotFoundException e) {
              // ignore and return empty object
              return EMPTY_JSON_OBJECT;
            } catch (Exception e) {
              logger.error("Exception calling slider.actionRegistryGetConfig",
                  e);
              return EMPTY_JSON_OBJECT;
            }
            JsonElement registryElement = JSON_PARSER.parse(registry);
            return (registryElement == null
                || registryElement instanceof JsonNull) ? EMPTY_JSON_OBJECT
                    : (JsonObject) registryElement;
          }
        });
  }

  private Integer getSliderList(final String appName)
      throws IOException, YarnException, InterruptedException {
    return getSliderList(appName, true);
  }

  private Integer getSliderList(final String appName, final boolean liveOnly)
      throws IOException, YarnException, InterruptedException {
    return invokeSliderClientRunnable(new SliderClientContextRunnable<Integer>() {
      @Override
      public Integer run(SliderClient sliderClient) throws YarnException,
          IOException, InterruptedException {
        int status = 0;
        if (liveOnly) {
          status = sliderClient.actionList(appName);
        } else {
          status = sliderClient.actionList(appName, ACTION_LIST_ARGS);
        }
        return status;
      }
    });
  }

  private Set<ApplicationReport> getSliderApplications(final String state)
      throws IOException, YarnException, InterruptedException {
    return getSliderApplications(false, state);
  }

  private Set<ApplicationReport> getSliderApplications(final boolean liveOnly)
      throws IOException, YarnException, InterruptedException {
    return getSliderApplications(liveOnly, null);
  }

  private Set<ApplicationReport> getSliderApplications(final boolean liveOnly,
      final String state)
      throws IOException, YarnException, InterruptedException {
    return invokeSliderClientRunnable(
        new SliderClientContextRunnable<Set<ApplicationReport>>() {
          @Override
          public Set<ApplicationReport> run(SliderClient sliderClient)
              throws YarnException, IOException, InterruptedException {
            Set<ApplicationReport> apps;
            ActionListArgs listArgs = new ActionListArgs();
            if (liveOnly) {
              apps = sliderClient.getApplicationList(null);
            } else if (StringUtils.isNotEmpty(state)) {
              listArgs.state = state;
              apps = sliderClient.getApplicationList(null, listArgs);
            } else {
              apps = sliderClient.getApplicationList(null, listArgs);
            }
            return apps;
          }
        });
  }

  @DELETE
  @Path("/{app_name}")
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response deleteApplication(@PathParam("app_name") String appName) {
    logger.info("DELETE: deleteApplication for appName = {}", appName);

    try {
      Response stopResponse = stopSliderApplication(appName);
      if (stopResponse.getStatus() == Status.INTERNAL_SERVER_ERROR
          .getStatusCode()) {
        return Response.status(Status.NOT_FOUND).build();
      }
    } catch (UnknownApplicationInstanceException e) {
      logger.error("Application does not exist", e);
      return Response.status(Status.NOT_FOUND).build();
    } catch (Exception e) {
      logger.error("Delete application failed", e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).build();
    }

    // Although slider client stop returns immediately, it usually takes a
    // little longer for it to stop from YARN point of view. Slider destroy
    // fails if the application is not completely stopped. Hence the need to
    // call destroy in a controlled loop few times (only if exit code is
    // EXIT_APPLICATION_IN_USE or EXIT_INSTANCE_EXISTS), before giving up.
    boolean keepTrying = true;
    int maxDeleteAttempts = 5;
    int deleteAttempts = 0;
    int sleepIntervalInMillis = 500;
    while (keepTrying && deleteAttempts < maxDeleteAttempts) {
      try {
        destroySliderApplication(appName);
        keepTrying = false;
      } catch (SliderException e) {
        if (e.getExitCode() == SliderExitCodes.EXIT_APPLICATION_IN_USE
            || e.getExitCode() == SliderExitCodes.EXIT_INSTANCE_EXISTS) {
          deleteAttempts++;
          // If we used up all the allowed delete attempts, let's log it as
          // error before giving up. Otherwise log as warn.
          if (deleteAttempts < maxDeleteAttempts) {
            logger.warn("Application not in stopped state, waiting for {}ms"
                + " before trying delete again", sleepIntervalInMillis);
          } else {
            logger.error("Delete application failed", e);
          }
          try {
            Thread.sleep(sleepIntervalInMillis);
          } catch (InterruptedException e1) {
          }
        } else {
          logger.error("Delete application threw exception", e);
          return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }
      } catch (Exception e) {
        logger.error("Delete application failed", e);
        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
      }
    }
    return Response.status(Status.NO_CONTENT).build();
  }

  private Response stopSliderApplication(final String appName)
      throws IOException, YarnException, InterruptedException {
    return invokeSliderClientRunnable(new SliderClientContextRunnable<Response>() {
      @Override
      public Response run(SliderClient sliderClient) throws YarnException,
          IOException, InterruptedException {
        int returnCode = sliderClient.actionFreeze(appName, ACTION_FREEZE_ARGS);
        if (returnCode == 0) {
          logger.info("Successfully stopped application {}", appName);
          return Response.status(Status.NO_CONTENT).build();
        } else {
          logger.error("Stop of application {} failed with return code ",
              appName, returnCode);
          ApplicationStatus applicationStatus = new ApplicationStatus();
          applicationStatus.setDiagnostics("Stop of application " + appName
              + " failed");
          return Response.status(Status.INTERNAL_SERVER_ERROR)
              .entity(applicationStatus).build();
        }
      }
    });
  }

  private Response startSliderApplication(final String appName, Application app)
      throws IOException, YarnException, InterruptedException {
    return invokeSliderClientRunnable(new SliderClientContextRunnable<Response>() {
      @Override
      public Response run(SliderClient sliderClient) throws YarnException,
          IOException, InterruptedException {
        ActionThawArgs thawArgs = new ActionThawArgs();
        if (app.getLifetime() == null) {
          app.setLifetime(DEFAULT_UNLIMITED_LIFETIME);
        }
        thawArgs.lifetime = app.getLifetime();
        int returnCode = sliderClient.actionThaw(appName, thawArgs);
        if (returnCode == 0) {
          logger.info("Successfully started application {}", appName);
          ApplicationStatus applicationStatus = new ApplicationStatus();
          applicationStatus.setState(ApplicationState.ACCEPTED);
          applicationStatus.setUri(CONTEXT_ROOT
              + APPLICATIONS_API_RESOURCE_PATH + "/" + appName);
          // 202 = ACCEPTED
          return Response.status(HTTP_STATUS_CODE_ACCEPTED)
              .entity(applicationStatus).build();
        } else {
          logger.error("Start of application {} failed with returnCode ",
              appName, returnCode);
          ApplicationStatus applicationStatus = new ApplicationStatus();
          applicationStatus.setDiagnostics("Start of application " + appName
              + " failed");
          return Response.status(Status.INTERNAL_SERVER_ERROR)
              .entity(applicationStatus).build();
        }
      }
    });
  }

  private Void destroySliderApplication(final String appName)
      throws IOException, YarnException, InterruptedException {
    return invokeSliderClientRunnable(new SliderClientContextRunnable<Void>() {
      @Override
      public Void run(SliderClient sliderClient) throws YarnException,
          IOException, InterruptedException {
        sliderClient.actionDestroy(appName);
        return null;
      }
    });
  }

  @PUT
  @Path("/{app_name}")
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON })
  public Response updateApplication(@PathParam("app_name") String appName,
      Application updateAppData) {
    logger.info("PUT: updateApplication for app = {} with data = {}", appName,
        updateAppData);

    // Ignore the app name provided in updateAppData and always use appName
    // path param
    updateAppData.setName(appName);

    // Adding support for stop and start
    // For STOP the app should be running. If already stopped then this
    // operation will be a no-op. For START it should be in stopped state.
    // If already running then this operation will be a no-op.

    // Check if app exists in any state
    try {
      int appsFound = getSliderList(appName, false);
      if (appsFound < 0) {
        return Response.status(Status.NOT_FOUND).build();
      }
    } catch (Exception e) {
      logger.error("Update application failed", e);
      return Response.status(Status.NOT_FOUND).build();
    }

    // If a STOP is requested
    if (updateAppData.getState() != null
        && updateAppData.getState() == ApplicationState.STOPPED) {
      try {
        int livenessCheck = getSliderList(appName);
        if (livenessCheck == 0) {
          return stopSliderApplication(appName);
        } else {
          logger.info("Application {} is already stopped", appName);
          ApplicationStatus applicationStatus = new ApplicationStatus();
          applicationStatus.setDiagnostics("Application " + appName
              + " is already stopped");
          return Response.status(Status.BAD_REQUEST).entity(applicationStatus)
              .build();
        }
      } catch (Exception e) {
        logger.error("Stop application failed", e);
        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
      }
    }

    // If a START is requested
    if (updateAppData.getState() != null
        && updateAppData.getState() == ApplicationState.STARTED) {
      try {
        int livenessCheck = getSliderList(appName);
        if (livenessCheck != 0) {
          return startSliderApplication(appName, updateAppData);
        } else {
          logger.info("Application {} is already running", appName);
          ApplicationStatus applicationStatus = new ApplicationStatus();
          applicationStatus.setDiagnostics("Application " + appName
              + " is already running");
          applicationStatus.setUri(CONTEXT_ROOT
              + APPLICATIONS_API_RESOURCE_PATH + "/" + appName);
          return Response.status(Status.BAD_REQUEST).entity(applicationStatus)
              .build();
        }
      } catch (Exception e) {
        logger.error("Start application failed", e);
        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
      }
    }

    // If no of instances specified then treat it as a flex
    if (updateAppData.getNumberOfContainers() != null
        && updateAppData.getComponents() == null) {
      updateAppData.setComponents(getDefaultComponentAsList());
    }

    // At this point if there are components then it is a flex
    if (updateAppData.getComponents() != null) {
      try {
        int livenessCheck = getSliderList(appName);
        if (livenessCheck == 0) {
          flexSliderApplication(appName, updateAppData);
        }
        return Response.status(Status.NO_CONTENT).build();
      } catch (Exception e) {
        logger.error("Update application failed", e);
        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
      }
    }

    // If new lifetime value specified then update it
    if (updateAppData.getLifetime() != null
        && updateAppData.getLifetime() > 0) {
      try {
        updateAppLifetime(appName, updateAppData.getLifetime());
      } catch (Exception e) {
        logger.error("Failed to update application (" + appName + ") lifetime ("
            + updateAppData.getLifetime() + ")", e);
        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
      }
    }

    // If nothing happens consider it a no-op
    return Response.status(Status.NO_CONTENT).build();
  }

  private Void updateAppLifetime(String appName, long lifetime)
      throws InterruptedException, YarnException, IOException {
    return invokeSliderClientRunnable(new SliderClientContextRunnable<Void>() {
      @Override public Void run(SliderClient sliderClient)
          throws YarnException, IOException, InterruptedException {
        ActionUpdateArgs args = new ActionUpdateArgs();
        args.lifetime = lifetime;
        sliderClient.actionUpdate(appName, args);
        return null;
      }
    });
  }

  // create default component and initialize with app level global values
  private List<Component> getDefaultComponentAsList(Application app) {
    List<Component> comps = getDefaultComponentAsList();
    Component comp = comps.get(0);
    comp.setArtifact(app.getArtifact());
    comp.setResource(app.getResource());
    comp.setNumberOfContainers(app.getNumberOfContainers());
    comp.setLaunchCommand(app.getLaunchCommand());
    return comps;
  }

  private List<Component> getDefaultComponentAsList() {
    Component comp = new Component();
    comp.setName(DEFAULT_COMPONENT_NAME);
    List<Component> comps = new ArrayList<>();
    comps.add(comp);
    return comps;
  }

  private Void flexSliderApplication(final String appName,
      final Application updateAppData) throws IOException, YarnException,
      InterruptedException {
    return invokeSliderClientRunnable(new SliderClientContextRunnable<Void>() {
      @Override
      public Void run(SliderClient sliderClient) throws YarnException,
          IOException, InterruptedException {
        ActionFlexArgs flexArgs = new ActionFlexArgs();
        ComponentArgsDelegate compDelegate = new ComponentArgsDelegate();
        Long globalNumberOfContainers = updateAppData.getNumberOfContainers();
        for (Component comp : updateAppData.getComponents()) {
          Long noOfContainers = comp.getNumberOfContainers() == null
              ? globalNumberOfContainers : comp.getNumberOfContainers();
          if (noOfContainers != null) {
            compDelegate.componentTuples.addAll(
                Arrays.asList(comp.getName(), String.valueOf(noOfContainers)));
          }
        }
        if (!compDelegate.componentTuples.isEmpty()) {
          flexArgs.componentDelegate = compDelegate;
          sliderClient.actionFlex(appName, flexArgs);
        }
        return null;
      }
    });
  }
}
