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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.pytorch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.PyTorchRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.api.PyTorchRole;
import org.apache.hadoop.yarn.submarine.common.api.TensorFlowRole;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.ServiceWrapper;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.PyTorchLaunchCommandFactory;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.component.ComponentTestCommons;
import org.apache.hadoop.yarn.submarine.utils.Localizer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestPyTorchServiceSpec {

  private ComponentTestCommons testCommons =
      new ComponentTestCommons(PyTorchRole.PRIMARY_WORKER);

  @Before
  public void setUp() throws IOException {
    testCommons.setupPyTorch();
  }

  @Test
  public void testPytorchServiceSpec() throws IOException {
    testCommons = new ComponentTestCommons(TensorFlowRole.PRIMARY_WORKER);
    testCommons.setupTensorFlow();

    PyTorchRunJobParameters parameters = new PyTorchRunJobParameters();
    parameters.setWorkerResource(testCommons.resource);
    parameters.setName("testJobName");
    parameters.setNumWorkers(1);
    parameters.setWorkerLaunchCmd("testWorkerLaunchCommand");

    MockClientContext mockClientContext = new MockClientContext();
    FileSystemOperations fsOperations =
        new FileSystemOperations(mockClientContext);

    HadoopEnvironmentSetup hadoopEnv =
        new HadoopEnvironmentSetup(mockClientContext, fsOperations);

    PyTorchLaunchCommandFactory launchCommandFactory =
        new PyTorchLaunchCommandFactory(hadoopEnv, parameters,
            new Configuration());

    Localizer localizer = new Localizer(fsOperations,
        mockClientContext.getRemoteDirectoryManager(), parameters);

    PyTorchServiceSpec pyTorchServiceSpec = new PyTorchServiceSpec(parameters,
        mockClientContext, fsOperations, launchCommandFactory, localizer);

    ServiceWrapper serviceWrapper = pyTorchServiceSpec.create();
    Service service = serviceWrapper.getService();

    assertNotNull("Service must not be null!", service);
    List<Component> components = service.getComponents();
    assertEquals("Number of components is not correct!", 1, components.size());

    Component component = components.get(0);
    assertEquals(1L, (long) component.getNumberOfContainers());
    assertEquals("./run-PRIMARY_WORKER.sh", component.getLaunchCommand());
  }
}