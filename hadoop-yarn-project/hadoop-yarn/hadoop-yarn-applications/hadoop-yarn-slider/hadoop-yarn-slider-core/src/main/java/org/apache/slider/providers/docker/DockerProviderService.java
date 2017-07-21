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
package org.apache.slider.providers.docker;

import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.providers.AbstractProviderService;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;

public class DockerProviderService extends AbstractProviderService
    implements DockerKeys {

  protected static final Logger log =
      LoggerFactory.getLogger(DockerProviderService.class);

  protected DockerProviderService() {
    super(DockerProviderService.class.getSimpleName());
  }

  public void processArtifact(ContainerLauncher launcher, Application
      application, RoleInstance roleInstance, SliderFileSystem fileSystem)
      throws IOException {
    Component component = roleInstance.providerRole.component;
    launcher.setYarnDockerMode(true);
    launcher.setDockerImage(component.getArtifact().getId());
    launcher.setDockerNetwork(component.getConfiguration()
        .getProperty(DOCKER_NETWORK, DEFAULT_DOCKER_NETWORK));
    String domain = getConfig().get(RegistryConstants.KEY_DNS_DOMAIN);
    String hostname;
    if (domain == null || domain.isEmpty()) {
      hostname = MessageFormat.format("{0}.{1}.{2}", roleInstance
          .getCompInstanceName(), application.getName(), RegistryUtils
          .currentUser());
    } else {
      hostname = MessageFormat.format("{0}.{1}.{2}.{3}", roleInstance
          .getCompInstanceName(), application.getName(), RegistryUtils
          .currentUser(), domain);
    }
    launcher.setDockerHostname(hostname);
    launcher.setRunPrivilegedContainer(component.getRunPrivilegedContainer());
  }
}
