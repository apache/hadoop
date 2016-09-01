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
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.info.CustomRegistryConstants;
import org.apache.slider.providers.AbstractProviderService;
import org.apache.slider.providers.ProviderCore;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.agent.AgentKeys;
import org.apache.slider.server.appmaster.PublishedArtifacts;
import org.apache.slider.server.appmaster.web.rest.RestPaths;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.apache.slider.server.appmaster.web.rest.RestPaths.*;

/**
 * Exists just to move some functionality out of AppMaster into a peer class
 * of the actual service provider doing the real work
 */
public class SliderAMProviderService extends AbstractProviderService implements
    ProviderCore,
    AgentKeys,
    SliderKeys {

  public SliderAMProviderService() {
    super("SliderAMProviderService");
  }

  @Override
  public String getHumanName() {
    return "Slider Application";
  }
  
  @Override
  public Configuration loadProviderConfigurationInformation(File confDir) throws
      BadCommandArgumentsException,
      IOException {
    return null;
  }

  @Override
  public void buildContainerLaunchContext(ContainerLauncher containerLauncher,
      AggregateConf instanceDefinition,
      Container container,
      ProviderRole role,
      SliderFileSystem sliderFileSystem,
      Path generatedConfPath,
      MapOperations resourceComponent,
      MapOperations appComponent,
      Path containerTmpDirPath) throws IOException, SliderException {
  }

  @Override
  public List<ProviderRole> getRoles() {
    return new ArrayList<>(0);
  }

  @Override
  public void validateInstanceDefinition(AggregateConf instanceDefinition) throws
      SliderException {

  }

  @Override
  public void applyInitialRegistryDefinitions(URL amWebURI,
      ServiceRecord serviceRecord)
      throws IOException {
    super.applyInitialRegistryDefinitions(amWebURI,
        serviceRecord);
    // now publish site.xml files
    YarnConfiguration defaultYarnConfig = new YarnConfiguration();
    amState.getPublishedSliderConfigurations().put(
        PublishedArtifacts.COMPLETE_CONFIG,
        new PublishedConfiguration(
            "Complete slider application settings",
            getConfig(), getConfig()));
    amState.getPublishedSliderConfigurations().put(
        PublishedArtifacts.YARN_SITE_CONFIG,
        new PublishedConfiguration(
            "YARN site settings",
            ConfigHelper.loadFromResource("yarn-site.xml"),
            defaultYarnConfig) );

    amState.getPublishedSliderConfigurations().put(
        PublishedArtifacts.CORE_SITE_CONFIG,
        new PublishedConfiguration(
            "Core site settings",
            ConfigHelper.loadFromResource("core-site.xml"),
            defaultYarnConfig) );
    amState.getPublishedSliderConfigurations().put(
        PublishedArtifacts.HDFS_SITE_CONFIG,
        new PublishedConfiguration(
            "HDFS site settings",
            ConfigHelper.loadFromResource("hdfs-site.xml"),
            new HdfsConfiguration(true)) );


    try {

      URL managementAPI = new URL(amWebURI, RELATIVE_PATH_MANAGEMENT);
      URL registryREST = new URL(amWebURI, RELATIVE_PATH_REGISTRY);

      URL publisherURL = new URL(amWebURI, RELATIVE_PATH_PUBLISHER);

      // Set the configurations URL.

      String configurationsURL = SliderUtils.appendToURL(
          publisherURL.toExternalForm(), RestPaths.SLIDER_CONFIGSET);
      String exportsURL = SliderUtils.appendToURL(
          publisherURL.toExternalForm(), RestPaths.SLIDER_EXPORTS);

      serviceRecord.addExternalEndpoint(
          RegistryTypeUtils.webEndpoint(
              CustomRegistryConstants.WEB_UI, amWebURI.toURI()));
      
      serviceRecord.addExternalEndpoint(
          RegistryTypeUtils.webEndpoint(
              CustomRegistryConstants.AM_REST_BASE, amWebURI.toURI()));
      
      serviceRecord.addExternalEndpoint(
          RegistryTypeUtils.restEndpoint(
              CustomRegistryConstants.MANAGEMENT_REST_API,
              managementAPI.toURI()));
      serviceRecord.addExternalEndpoint(
          RegistryTypeUtils.restEndpoint(
              CustomRegistryConstants.PUBLISHER_REST_API,
              publisherURL.toURI()));
      serviceRecord.addExternalEndpoint(
          RegistryTypeUtils.restEndpoint(
              CustomRegistryConstants.REGISTRY_REST_API,
              registryREST.toURI()));
      serviceRecord.addExternalEndpoint(
          RegistryTypeUtils.restEndpoint(
              CustomRegistryConstants.PUBLISHER_CONFIGURATIONS_API,
              new URI(configurationsURL)));
      serviceRecord.addExternalEndpoint(
          RegistryTypeUtils.restEndpoint(
              CustomRegistryConstants.PUBLISHER_EXPORTS_API,
              new URI(exportsURL)));

    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }
}
