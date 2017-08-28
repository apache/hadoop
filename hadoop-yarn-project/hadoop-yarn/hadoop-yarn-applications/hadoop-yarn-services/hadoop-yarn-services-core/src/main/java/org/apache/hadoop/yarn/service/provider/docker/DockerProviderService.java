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
package org.apache.hadoop.yarn.service.provider.docker;

import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.provider.AbstractProviderService;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.apache.hadoop.yarn.service.containerlaunch.AbstractLauncher;

import java.io.IOException;
import java.text.MessageFormat;

public class DockerProviderService extends AbstractProviderService
    implements DockerKeys {

  public void processArtifact(AbstractLauncher launcher,
      ComponentInstance compInstance, SliderFileSystem fileSystem,
      Service service) throws IOException{
    launcher.setYarnDockerMode(true);
    launcher.setDockerImage(compInstance.getCompSpec().getArtifact().getId());
    launcher.setDockerNetwork(compInstance.getCompSpec().getConfiguration()
        .getProperty(DOCKER_NETWORK, DEFAULT_DOCKER_NETWORK));
    String domain = compInstance.getComponent().getScheduler().getConfig()
        .get(RegistryConstants.KEY_DNS_DOMAIN);
    String hostname;
    if (domain == null || domain.isEmpty()) {
      hostname = MessageFormat
          .format("{0}.{1}.{2}", compInstance.getCompInstanceName(),
              service.getName(), RegistryUtils.currentUser());
    } else {
      hostname = MessageFormat
          .format("{0}.{1}.{2}.{3}", compInstance.getCompInstanceName(),
              service.getName(), RegistryUtils.currentUser(), domain);
    }
    launcher.setDockerHostname(hostname);
    launcher.setRunPrivilegedContainer(
        compInstance.getCompSpec().getRunPrivilegedContainer());
  }
}
