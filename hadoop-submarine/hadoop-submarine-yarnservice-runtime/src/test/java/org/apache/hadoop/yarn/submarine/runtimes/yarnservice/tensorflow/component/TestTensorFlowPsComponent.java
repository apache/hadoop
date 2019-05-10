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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import java.io.IOException;
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

/**
 * This class is to test {@link TensorFlowPsComponent}.
 */
public class TestTensorFlowPsComponent {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private ComponentTestCommons testCommons =
      new ComponentTestCommons(TensorFlowRole.PS);

  @Before
  public void setUp() throws IOException {
    testCommons.setupTensorFlow();
  }

  private TensorFlowPsComponent createPsComponent(RunJobParameters parameters) {
    return new TensorFlowPsComponent(
        testCommons.fsOperations,
        testCommons.mockClientContext.getRemoteDirectoryManager(),
        (TensorFlowLaunchCommandFactory) testCommons.mockLaunchCommandFactory,
        parameters,
        testCommons.yarnConfig);
  }

  private void verifyCommons(Component component) throws IOException {
    assertEquals(testCommons.role.getComponentName(), component.getName());
    testCommons.verifyCommonConfigEnvs(component);

    assertTrue(component.getConfiguration().getProperties().isEmpty());

    assertEquals(RestartPolicyEnum.NEVER, component.getRestartPolicy());
    testCommons.verifyResources(component);
    assertEquals(
        new Artifact().type(Artifact.TypeEnum.DOCKER).id("testPSDockerImage"),
        component.getArtifact());

    String taskTypeUppercase = testCommons.role.getName().toUpperCase();
    String expectedScriptName = String.format("run-%s.sh", taskTypeUppercase);
    assertEquals(String.format("./%s", expectedScriptName),
        component.getLaunchCommand());
    verify(testCommons.fsOperations)
        .uploadToRemoteFileAndLocalizeToContainerWorkDir(
        any(Path.class), eq("mockScript"), eq(expectedScriptName),
        eq(component));
  }

  @Test
  public void testPSComponentWithNullResource() throws IOException {
    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setPsResource(null);

    TensorFlowPsComponent psComponent =
        createPsComponent(parameters);

    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("PS resource must not be null");
    psComponent.createComponent();
  }

  @Test
  public void testPSComponentWithNullJobName() throws IOException {
    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setPsResource(testCommons.resource);
    parameters.setNumPS(1);
    parameters.setName(null);

    TensorFlowPsComponent psComponent =
        createPsComponent(parameters);

    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("Job name must not be null");
    psComponent.createComponent();
  }

  @Test
  public void testPSComponentZeroNumberOfPS() throws IOException {
    testCommons.yarnConfig.set("hadoop.registry.dns.domain-name", "testDomain");

    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setPsResource(testCommons.resource);
    parameters.setName("testJobName");
    parameters.setPsDockerImage("testPSDockerImage");
    parameters.setNumPS(0);

    TensorFlowPsComponent psComponent =
        createPsComponent(parameters);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Number of PS should be at least 1!");
    psComponent.createComponent();
  }

  @Test
  public void testPSComponentNumPSIsOne() throws IOException {
    testCommons.yarnConfig.set("hadoop.registry.dns.domain-name", "testDomain");

    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setPsResource(testCommons.resource);
    parameters.setName("testJobName");
    parameters.setNumPS(1);
    parameters.setPsDockerImage("testPSDockerImage");

    TensorFlowPsComponent psComponent =
        createPsComponent(parameters);

    Component component = psComponent.createComponent();

    assertEquals(1L, (long) component.getNumberOfContainers());
    verifyCommons(component);
  }

  @Test
  public void testPSComponentNumPSIsTwo() throws IOException {
    testCommons.yarnConfig.set("hadoop.registry.dns.domain-name", "testDomain");

    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setPsResource(testCommons.resource);
    parameters.setName("testJobName");
    parameters.setNumPS(2);
    parameters.setPsDockerImage("testPSDockerImage");

    TensorFlowPsComponent psComponent =
        createPsComponent(parameters);

    Component component = psComponent.createComponent();

    assertEquals(2L, (long) component.getNumberOfContainers());
    verifyCommons(component);
  }

}