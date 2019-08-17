/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.pytorch.component;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.PyTorchRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.Role;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.AbstractComponent;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.PyTorchLaunchCommandFactory;

import java.io.IOException;

/**
 * Component implementation for Worker process of PyTorch.
 */
public class PyTorchWorkerComponent extends AbstractComponent {
  public PyTorchWorkerComponent(FileSystemOperations fsOperations,
      RemoteDirectoryManager remoteDirectoryManager,
      PyTorchRunJobParameters parameters, Role role,
      PyTorchLaunchCommandFactory launchCommandFactory,
      Configuration yarnConfig) {
    super(fsOperations, remoteDirectoryManager, parameters, role,
        yarnConfig, launchCommandFactory);
  }

  @Override
  public Component createComponent() throws IOException {
    return createComponentInternal();
  }
}
