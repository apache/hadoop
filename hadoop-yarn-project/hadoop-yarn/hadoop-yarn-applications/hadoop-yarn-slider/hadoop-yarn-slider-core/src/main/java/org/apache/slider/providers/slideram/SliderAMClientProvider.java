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

package org.apache.slider.providers.slideram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.AbstractLauncher;
import org.apache.slider.core.launch.JavaCommandLineBuilder;
import org.apache.slider.providers.AbstractClientProvider;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.slider.api.ResourceKeys.COMPONENT_INSTANCES;

/**
 * handles the setup of the Slider AM.
 * This keeps aspects of role, cluster validation and Clusterspec setup
 * out of the core slider client
 */
public class SliderAMClientProvider extends AbstractClientProvider
    implements SliderKeys {


  protected static final Logger log =
    LoggerFactory.getLogger(SliderAMClientProvider.class);
  protected static final String NAME = "SliderAM";
  public static final String INSTANCE_RESOURCE_BASE = PROVIDER_RESOURCE_BASE_ROOT +
                                                       "slideram/instance/";
  public static final String INTERNAL_JSON =
    INSTANCE_RESOURCE_BASE + "internal.json";
  public static final String APPCONF_JSON =
    INSTANCE_RESOURCE_BASE + "appconf.json";
  public static final String RESOURCES_JSON =
    INSTANCE_RESOURCE_BASE + "resources.json";

  public SliderAMClientProvider(Configuration conf) {
    super(conf);
  }

  /**
   * List of roles
   */
  public static final List<ProviderRole> ROLES =
    new ArrayList<ProviderRole>();

  public static final int KEY_AM = ROLE_AM_PRIORITY_INDEX;

  public static final ProviderRole APPMASTER =
      new ProviderRole(COMPONENT_AM, KEY_AM,
          PlacementPolicy.EXCLUDE_FROM_FLEXING,
          ResourceKeys.DEFAULT_NODE_FAILURE_THRESHOLD, 
          0, "");

  /**
   * Initialize role list
   */
  static {
    ROLES.add(APPMASTER);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<ProviderRole> getRoles() {
    return ROLES;
  }


  @Override //Client
  public void preflightValidateClusterConfiguration(SliderFileSystem sliderFileSystem,
                                                    String clustername,
                                                    Configuration configuration,
                                                    AggregateConf instanceDefinition,
                                                    Path clusterDirPath,
                                                    Path generatedConfDirPath,
                                                    boolean secure)
      throws SliderException, IOException {

    super.preflightValidateClusterConfiguration(sliderFileSystem, clustername, configuration, instanceDefinition, clusterDirPath, generatedConfDirPath, secure);
    //add a check for the directory being writeable by the current user
    String
      dataPath = instanceDefinition.getInternalOperations()
                                   .getGlobalOptions()
                                   .getMandatoryOption(
                                     InternalKeys.INTERNAL_DATA_DIR_PATH);

    Path path = new Path(dataPath);
    sliderFileSystem.verifyDirectoryWriteAccess(path);
    Path historyPath = new Path(clusterDirPath, SliderKeys.HISTORY_DIR_NAME);
    sliderFileSystem.verifyDirectoryWriteAccess(historyPath);
  }

  /**
   * Verify that an instance definition is considered valid by the provider
   * @param instanceDefinition instance definition
   * @throws SliderException if the configuration is not valid
   */
  public void validateInstanceDefinition(AggregateConf instanceDefinition, SliderFileSystem fs) throws
      SliderException {

    super.validateInstanceDefinition(instanceDefinition, fs);
    
    // make sure there is no negative entry in the instance count
    Map<String, Map<String, String>> instanceMap =
        instanceDefinition.getResources().components;
    for (Map.Entry<String, Map<String, String>> entry : instanceMap.entrySet()) {
      MapOperations mapOperations = new MapOperations(entry);
      int instances = mapOperations.getOptionInt(COMPONENT_INSTANCES, 0);
      if (instances < 0) {
        throw new BadClusterStateException(
            "Component %s has negative instance count: %d",
            mapOperations.name,
            instances);
      }
    }
  }
  
  /**
   * The Slider AM sets up all the dependency JARs above slider.jar itself
   * {@inheritDoc}
   */
  public void prepareAMAndConfigForLaunch(SliderFileSystem fileSystem,
      Configuration serviceConf,
      AbstractLauncher launcher,
      AggregateConf instanceDescription,
      Path snapshotConfDirPath,
      Path generatedConfDirPath,
      Configuration clientConfExtras,
      String libdir,
      Path tempPath, boolean miniClusterTestRun)
    throws IOException, SliderException {

    Map<String, LocalResource> providerResources = new HashMap<>();

    ProviderUtils.addProviderJar(providerResources,
        this,
        SLIDER_JAR,
        fileSystem,
        tempPath,
        libdir,
        miniClusterTestRun);

    String libDirProp =
        System.getProperty(SliderKeys.PROPERTY_LIB_DIR);
    log.info("Loading all dependencies for AM.");
    // If slider.tar.gz is available in hdfs use it, else upload all jars
    Path dependencyLibTarGzip = fileSystem.getDependencyTarGzip();
    if (fileSystem.isFile(dependencyLibTarGzip)) {
      SliderUtils.putAmTarGzipAndUpdate(providerResources, fileSystem);
    } else {
      ProviderUtils.addAllDependencyJars(providerResources,
                                         fileSystem,
                                         tempPath,
                                         libdir,
                                         libDirProp);
    }
    addKeytabResourceIfNecessary(fileSystem,
                                 instanceDescription,
                                 providerResources);

    launcher.addLocalResources(providerResources);

    //also pick up all env variables from a map
    launcher.copyEnvVars(
      instanceDescription.getInternalOperations().getOrAddComponent(
        SliderKeys.COMPONENT_AM));
  }

  /**
   * If the cluster is secure, and an HDFS installed keytab is available for AM
   * authentication, add this keytab as a local resource for the AM launch.
   *
   * @param fileSystem
   * @param instanceDescription
   * @param providerResources
   * @throws IOException
   * @throws BadConfigException if there's no keytab and it is explicitly required.
   */
  protected void addKeytabResourceIfNecessary(SliderFileSystem fileSystem,
                                              AggregateConf instanceDescription,
                                              Map<String, LocalResource> providerResources)
    throws IOException, BadConfigException {
    if (UserGroupInformation.isSecurityEnabled()) {
      String keytabPathOnHost = instanceDescription.getAppConfOperations()
          .getComponent(SliderKeys.COMPONENT_AM).get(
              SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH);
      if (SliderUtils.isUnset(keytabPathOnHost)) {
        String amKeytabName = instanceDescription.getAppConfOperations()
            .getComponent(SliderKeys.COMPONENT_AM).get(
                SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME);
        String keytabDir = instanceDescription.getAppConfOperations()
            .getComponent(SliderKeys.COMPONENT_AM).get(
                SliderXmlConfKeys.KEY_HDFS_KEYTAB_DIR);
        Path keytabPath = fileSystem.buildKeytabPath(keytabDir, amKeytabName,
                                                     instanceDescription.getName());
        if (fileSystem.getFileSystem().exists(keytabPath)) {
          LocalResource keytabRes = fileSystem.createAmResource(keytabPath,
                                                  LocalResourceType.FILE);

          providerResources.put(SliderKeys.KEYTAB_DIR + "/" +
                                 amKeytabName, keytabRes);
        } else {
          log.warn("No keytab file was found at {}.", keytabPath);
          if (getConf().getBoolean(KEY_AM_LOGIN_KEYTAB_REQUIRED, false)) {
            throw new BadConfigException("No keytab file was found at %s.", keytabPath);

          } else {
            log.warn("The AM will be "
              + "started without a kerberos authenticated identity. "
              + "The application is therefore not guaranteed to remain "
              + "operational beyond 24 hours.");
          }
        }
      }
    }
  }

  /**
   * Update the AM resource with any local needs
   * @param capability capability to update
   */
  public void prepareAMResourceRequirements(MapOperations sliderAM,
                                            Resource capability) {
    capability.setMemory(sliderAM.getOptionInt(
      ResourceKeys.YARN_MEMORY,
      capability.getMemory()));
    capability.setVirtualCores(
        sliderAM.getOptionInt(ResourceKeys.YARN_CORES, capability.getVirtualCores()));
  }
  
  /**
   * Extract any JVM options from the cluster specification and
   * add them to the command line
   */
  public void addJVMOptions(AggregateConf aggregateConf,
                            JavaCommandLineBuilder cmdLine)
      throws BadConfigException {

    MapOperations sliderAM =
        aggregateConf.getAppConfOperations().getMandatoryComponent(
        SliderKeys.COMPONENT_AM);
    cmdLine.forceIPv4().headless();
    String heap = sliderAM.getOption(RoleKeys.JVM_HEAP,
                                   DEFAULT_JVM_HEAP);
    cmdLine.setJVMHeap(heap);
    String jvmopts = sliderAM.getOption(RoleKeys.JVM_OPTS, "");
    if (SliderUtils.isSet(jvmopts)) {
      cmdLine.add(jvmopts);
    }
  }


  @Override
  public void prepareInstanceConfiguration(AggregateConf aggregateConf)
      throws SliderException, IOException {
    mergeTemplates(aggregateConf,
        INTERNAL_JSON, RESOURCES_JSON, APPCONF_JSON
                  );
  }
}
