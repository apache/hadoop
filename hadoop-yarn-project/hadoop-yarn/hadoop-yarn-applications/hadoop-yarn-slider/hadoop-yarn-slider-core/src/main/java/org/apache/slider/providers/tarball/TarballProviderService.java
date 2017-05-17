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
package org.apache.slider.providers.tarball;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.slider.api.resource.Component;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.providers.AbstractProviderService;

import java.io.IOException;

public class TarballProviderService extends AbstractProviderService {

  protected TarballProviderService() {
    super(TarballProviderService.class.getSimpleName());
  }

  @Override
  public void processArtifact(ContainerLauncher launcher, Component
      component, SliderFileSystem fileSystem) throws IOException {
    Path artifact =  new Path(component.getArtifact().getId());
    if (!fileSystem.isFile(artifact)) {
      throw new IOException("Package doesn't exist as a resource: " +
          artifact.toString());
    }
    log.info("Adding resource {}", artifact.toString());
    LocalResourceType type = LocalResourceType.ARCHIVE;
    LocalResource packageResource = fileSystem.createAmResource(
        artifact, type);
    launcher.addLocalResource(APP_INSTALL_DIR, packageResource);
  }
}
