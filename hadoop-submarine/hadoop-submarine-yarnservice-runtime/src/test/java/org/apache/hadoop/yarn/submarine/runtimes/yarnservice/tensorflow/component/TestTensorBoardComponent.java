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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Component.RestartPolicyEnum;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.TensorFlowRole;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.TensorFlowLaunchCommandFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/**
 * This class is to test {@link TensorBoardComponent}.
 */
public class TestTensorBoardComponent {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private ComponentTestCommons testCommons =
      new ComponentTestCommons(TensorFlowRole.TENSORBOARD);

  @Before
  public void setUp() throws IOException {
    testCommons.setupTensorFlow();
  }

  private TensorBoardComponent createTensorBoardComponent(
      RunJobParameters parameters) {
    return new TensorBoardComponent(
        testCommons.fsOperations,
        testCommons.mockClientContext.getRemoteDirectoryManager(),
        parameters,
        (TensorFlowLaunchCommandFactory) testCommons.mockLaunchCommandFactory,
        testCommons.yarnConfig);
  }

  @Test
  public void testTensorBoardComponentWithNullResource() throws IOException {
    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setTensorboardResource(null);

    TensorBoardComponent tensorBoardComponent =
        createTensorBoardComponent(parameters);

    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("TensorBoard resource must not be null");
    tensorBoardComponent.createComponent();
  }

  @Test
  public void testTensorBoardComponentWithNullJobName() throws IOException {
    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setTensorboardResource(testCommons.resource);
    parameters.setName(null);

    TensorBoardComponent tensorBoardComponent =
        createTensorBoardComponent(parameters);

    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("Job name must not be null");
    tensorBoardComponent.createComponent();
  }

  @Test
  public void testTensorBoardComponent() throws IOException {
    testCommons.yarnConfig.set("hadoop.registry.dns.domain-name", "testDomain");

    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setTensorboardResource(testCommons.resource);
    parameters.setName("testJobName");
    parameters.setTensorboardDockerImage("testTBDockerImage");

    TensorBoardComponent tensorBoardComponent =
        createTensorBoardComponent(parameters);

    Component component = tensorBoardComponent.createComponent();

    assertEquals(testCommons.role.getComponentName(), component.getName());
    testCommons.verifyCommonConfigEnvs(component);

    assertEquals(1L, (long) component.getNumberOfContainers());
    assertEquals(RestartPolicyEnum.NEVER, component.getRestartPolicy());
    testCommons.verifyResources(component);
    assertEquals(
        new Artifact().type(Artifact.TypeEnum.DOCKER).id("testTBDockerImage"),
        component.getArtifact());

    assertEquals(String.format(
        "http://tensorboard-0.testJobName.%s" + ".testDomain:6006",
        testCommons.userName),
        tensorBoardComponent.getTensorboardLink());

    assertEquals("./run-TENSORBOARD.sh", component.getLaunchCommand());
    verify(testCommons.fsOperations)
        .uploadToRemoteFileAndLocalizeToContainerWorkDir(
        any(Path.class), eq("mockScript"), eq("run-TENSORBOARD.sh"),
        eq(component));
  }

}