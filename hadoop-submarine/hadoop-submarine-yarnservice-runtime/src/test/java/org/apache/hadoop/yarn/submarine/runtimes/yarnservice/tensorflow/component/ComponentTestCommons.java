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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.component;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.service.api.ServiceApiConstants;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.runjob.Framework;
import org.apache.hadoop.yarn.submarine.common.Envs;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.api.Role;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.AbstractLaunchCommand;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.LaunchCommandFactory;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.PyTorchLaunchCommandFactory;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.TensorFlowLaunchCommandFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class has some helper methods and fields
 * in order to test TensorFlow-related Components easier.
 */
public class ComponentTestCommons {
  String userName;
  Role role;
  LaunchCommandFactory mockLaunchCommandFactory;
  FileSystemOperations fsOperations;
  public MockClientContext mockClientContext;
  Configuration yarnConfig;
  public Resource resource;

  public ComponentTestCommons(Role role) {
    this.role = role;
  }

  public void setupTensorFlow() throws IOException {
    setupInternal(Framework.TENSORFLOW);
  }

  public void setupPyTorch() throws IOException {
    setupInternal(Framework.PYTORCH);
  }

  private void setupInternal(Framework framework) throws IOException {
    this.userName = System.getProperty("user.name");
    this.resource = Resource.newInstance(4000, 10);
    setupDependencies(framework);
  }

  private void setupDependencies(Framework framework) throws IOException {
    fsOperations = mock(FileSystemOperations.class);
    mockClientContext = new MockClientContext();

    if (framework == Framework.TENSORFLOW) {
      mockLaunchCommandFactory = mock(TensorFlowLaunchCommandFactory.class);
    } else if (framework == Framework.PYTORCH) {
      mockLaunchCommandFactory = mock(PyTorchLaunchCommandFactory.class);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported framework type specified!");
    }

    AbstractLaunchCommand mockLaunchCommand = mock(AbstractLaunchCommand.class);
    when(mockLaunchCommand.generateLaunchScript()).thenReturn("mockScript");
    when(mockLaunchCommandFactory.createLaunchCommand(eq(role),
        any(Component.class))).thenReturn(mockLaunchCommand);

    yarnConfig = new Configuration();
  }

  void verifyCommonConfigEnvs(Component component) {
    assertNotNull(component.getConfiguration().getEnv());
    assertEquals(2, component.getConfiguration().getEnv().size());
    assertEquals(ServiceApiConstants.COMPONENT_ID,
        component.getConfiguration().getEnv().get(Envs.TASK_INDEX_ENV));
    assertEquals(role.getName(),
        component.getConfiguration().getEnv().get(Envs.TASK_TYPE_ENV));
  }

  void verifyResources(Component component) {
    assertNotNull(component.getResource());
    assertEquals(10, (int) component.getResource().getCpus());
    assertEquals(4000,
        (int) Integer.valueOf(component.getResource().getMemory()));
  }
}
