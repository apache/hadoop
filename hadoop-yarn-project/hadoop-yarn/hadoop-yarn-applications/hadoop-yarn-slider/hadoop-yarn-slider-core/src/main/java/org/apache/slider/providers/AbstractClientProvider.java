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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.AbstractLauncher;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

import static org.apache.slider.api.ResourceKeys.COMPONENT_INSTANCES;
import static org.apache.slider.api.ResourceKeys.DEF_YARN_CORES;
import static org.apache.slider.api.ResourceKeys.DEF_YARN_MEMORY;
import static org.apache.slider.api.ResourceKeys.YARN_CORES;
import static org.apache.slider.api.ResourceKeys.YARN_MEMORY;

public abstract class AbstractClientProvider extends Configured {
  private static final Logger log =
    LoggerFactory.getLogger(AbstractClientProvider.class);
  protected static final ProviderUtils providerUtils =
    new ProviderUtils(log);

  public static final String PROVIDER_RESOURCE_BASE =
    "org/apache/slider/providers/";
  public static final String PROVIDER_RESOURCE_BASE_ROOT =
    "/" + PROVIDER_RESOURCE_BASE;

  public AbstractClientProvider(Configuration conf) {
    super(conf);
  }

  public abstract String getName();

  public abstract List<ProviderRole> getRoles();

  /**
   * Verify that an instance definition is considered valid by the provider
   * @param instanceDefinition instance definition
   * @throws SliderException if the configuration is not valid
   */
  public void validateInstanceDefinition(AggregateConf instanceDefinition, SliderFileSystem fs) throws
      SliderException {

    List<ProviderRole> roles = getRoles();
    ConfTreeOperations resources =
      instanceDefinition.getResourceOperations();
    for (ProviderRole role : roles) {
      String name = role.name;
      MapOperations component = resources.getComponent(role.group);
      if (component != null) {
        String instances = component.get(COMPONENT_INSTANCES);
        if (instances == null) {
          String message = "No instance count provided for " + name;
          log.error("{} with \n{}", message, resources.toString());
          throw new BadClusterStateException(message);
        }
        String ram = component.get(YARN_MEMORY);
        String cores = component.get(YARN_CORES);


        providerUtils.getRoleResourceRequirement(ram,
                                                 DEF_YARN_MEMORY,
                                                 Integer.MAX_VALUE);
        providerUtils.getRoleResourceRequirement(cores,
                                                 DEF_YARN_CORES,
                                                 Integer.MAX_VALUE);
      }
    }
  }


  /**
   * Any provider-side alteration of a configuration can take place here.
   * @param aggregateConf config to patch
   * @throws IOException IO problems
   * @throws SliderException Slider-specific issues
   */
  public void prepareInstanceConfiguration(AggregateConf aggregateConf) throws
      SliderException,
                                                                    IOException {
    //default: do nothing
  }


  /**
   * Prepare the AM settings for launch
   * @param fileSystem filesystem
   * @param serviceConf configuration of the client
   * @param launcher launcher to set up
   * @param instanceDescription instance description being launched
   * @param snapshotConfDirPath
   * @param generatedConfDirPath
   * @param clientConfExtras
   * @param libdir
   * @param tempPath
   * @param miniClusterTestRun flag set to true on a mini cluster run
   * @throws IOException
   * @throws SliderException
   */
  public void prepareAMAndConfigForLaunch(SliderFileSystem fileSystem,
      Configuration serviceConf,
      AbstractLauncher launcher,
      AggregateConf instanceDescription,
      Path snapshotConfDirPath,
      Path generatedConfDirPath,
      Configuration clientConfExtras,
      String libdir,
      Path tempPath,
      boolean miniClusterTestRun)
    throws IOException, SliderException {
    
  }
  
  /**
   * Load in and merge in templates. Null arguments means "no such template"
   * @param instanceConf instance to patch 
   * @param internalTemplate patch to internal.json
   * @param resourceTemplate path to resources.json
   * @param appConfTemplate path to app_conf.json
   * @throws IOException any IO problems
   */
  protected void mergeTemplates(AggregateConf instanceConf,
                                String internalTemplate,
                                String resourceTemplate,
                                String appConfTemplate) throws IOException {
    if (internalTemplate != null) {
      ConfTreeOperations template =
        ConfTreeOperations.fromResource(internalTemplate);
      instanceConf.getInternalOperations()
                  .mergeWithoutOverwrite(template.confTree);
    }

    if (resourceTemplate != null) {
      ConfTreeOperations resTemplate =
        ConfTreeOperations.fromResource(resourceTemplate);
      instanceConf.getResourceOperations()
                   .mergeWithoutOverwrite(resTemplate.confTree);
    }
   
    if (appConfTemplate != null) {
      ConfTreeOperations template =
        ConfTreeOperations.fromResource(appConfTemplate);
      instanceConf.getAppConfOperations()
                   .mergeWithoutOverwrite(template.confTree);
    }
    
  }

  /**
   * This is called pre-launch to validate that the cluster specification
   * is valid. This can include checking that the security options
   * are in the site files prior to launch, that there are no conflicting operations
   * etc.
   *
   * This check is made prior to every launch of the cluster -so can 
   * pick up problems which manually edited cluster files have added,
   * or from specification files from previous versions.
   *
   * The provider MUST NOT change the remote specification. This is
   * purely a pre-launch validation of options.
   *
   *
   * @param sliderFileSystem filesystem
   * @param clustername name of the cluster
   * @param configuration cluster configuration
   * @param instanceDefinition cluster specification
   * @param clusterDirPath directory of the cluster
   * @param generatedConfDirPath path to place generated artifacts
   * @param secure flag to indicate that the cluster is secure
   * @throws SliderException on any validation issue
   * @throws IOException on any IO problem
   */
  public void preflightValidateClusterConfiguration(SliderFileSystem sliderFileSystem,
                                                      String clustername,
                                                      Configuration configuration,
                                                      AggregateConf instanceDefinition,
                                                      Path clusterDirPath,
                                                      Path generatedConfDirPath,
                                                      boolean secure)
      throws SliderException, IOException {
    validateInstanceDefinition(instanceDefinition, sliderFileSystem);
  }

  /**
   * Return a set of application specific string tags.
   * @return the set of tags.
   */
  public Set<String> getApplicationTags(SliderFileSystem fileSystem,
      ConfTreeOperations appConf, String appName) throws SliderException {
    return Collections.emptySet();
  }

  /**
   * Generates a fixed format of application tags given one or more of
   * application name, version and description. This allows subsequent query for
   * an application with a name only, version only or description only or any
   * combination of those as filters.
   *
   * @param appName name of the application
   * @param appVersion version of the application
   * @param appDescription brief description of the application
   * @return
   */
  public final Set<String> createApplicationTags(String appName,
      String appVersion, String appDescription) {
    Set<String> tags = new HashSet<>();
    tags.add(SliderUtils.createNameTag(appName));
    if (appVersion != null) {
      tags.add(SliderUtils.createVersionTag(appVersion));
    }
    if (appDescription != null) {
      tags.add(SliderUtils.createDescriptionTag(appDescription));
    }
    return tags;
  }

  /**
   * Process client operations for applications such as install, configure
   * @param fileSystem
   * @param registryOperations
   * @param configuration
   * @param operation
   * @param clientInstallPath
   * @param clientPackage
   * @param clientConfig
   * @param name
   * @throws SliderException
   */
  public void processClientOperation(SliderFileSystem fileSystem,
                                     RegistryOperations registryOperations,
                                     Configuration configuration,
                                     String operation,
                                     File clientInstallPath,
                                     File clientPackage,
                                     JSONObject clientConfig,
                                     String name)
      throws SliderException {
    throw new SliderException("Provider does not support client operations.");
  }

}
