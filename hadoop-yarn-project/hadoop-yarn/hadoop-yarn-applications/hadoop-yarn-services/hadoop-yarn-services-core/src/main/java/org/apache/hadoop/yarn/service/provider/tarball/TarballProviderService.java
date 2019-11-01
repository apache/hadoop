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
package org.apache.hadoop.yarn.service.provider.tarball;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.containerlaunch.ContainerLaunchService;
import org.apache.hadoop.yarn.service.provider.AbstractProviderService;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.apache.hadoop.yarn.service.containerlaunch.AbstractLauncher;

import java.io.IOException;

public class TarballProviderService extends AbstractProviderService {

  @Override
  public void processArtifact(AbstractLauncher launcher,
      ComponentInstance instance, SliderFileSystem fileSystem,
      Service service, ContainerLaunchService.ComponentLaunchContext
      compLaunchCtx) throws IOException {
    Path artifact = new Path(compLaunchCtx.getArtifact().getId());
    if (!fileSystem.isFile(artifact)) {
      throw new IOException(
          "Package doesn't exist as a resource: " + artifact);
    }
    log.info("Adding resource {}", artifact);
    LocalResourceType type = LocalResourceType.ARCHIVE;
    LocalResource packageResource = fileSystem.createAmResource(artifact, type,
        LocalResourceVisibility.APPLICATION);
    launcher.addLocalResource(APP_LIB_DIR, packageResource);
  }
}
