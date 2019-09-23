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

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Component.RestartPolicyEnum;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.api.TensorFlowRole;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.TensorFlowLaunchCommandFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/**
 * This class is to test {@link TensorFlowWorkerComponent}.
 */
public class TestTensorFlowWorkerComponent {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private ComponentTestCommons testCommons =
      new ComponentTestCommons(TensorFlowRole.TENSORBOARD);

  @Before
  public void setUp() throws IOException {
    testCommons.setupTensorFlow();
  }

  private TensorFlowWorkerComponent createWorkerComponent(
      TensorFlowRunJobParameters parameters) {
    return new TensorFlowWorkerComponent(
        testCommons.fsOperations,
        testCommons.mockClientContext.getRemoteDirectoryManager(),
        parameters, testCommons.role,
        (TensorFlowLaunchCommandFactory) testCommons.mockLaunchCommandFactory,
        testCommons.yarnConfig);
  }

  private void verifyCommons(Component component) throws IOException {
    verifyCommonsInternal(component, ImmutableMap.of());
  }

  private void verifyCommons(Component component,
      Map<String, String> expectedProperties) throws IOException {
    verifyCommonsInternal(component, expectedProperties);
  }

  private void verifyCommonsInternal(Component component,
      Map<String, String> expectedProperties) throws IOException {
    assertEquals(testCommons.role.getComponentName(), component.getName());
    testCommons.verifyCommonConfigEnvs(component);

    Map<String, String> actualProperties =
        component.getConfiguration().getProperties();
    if (!expectedProperties.isEmpty()) {
      assertFalse(actualProperties.isEmpty());
      expectedProperties.forEach(
          (k, v) -> assertEquals(v, actualProperties.get(k)));
    } else {
      assertTrue(actualProperties.isEmpty());
    }

    assertEquals(RestartPolicyEnum.NEVER, component.getRestartPolicy());
    testCommons.verifyResources(component);
    assertEquals(
        new Artifact().type(Artifact.TypeEnum.DOCKER)
            .id("testWorkerDockerImage"),
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
  public void testWorkerComponentWithNullResource() throws IOException {
    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setWorkerResource(null);

    TensorFlowWorkerComponent workerComponent =
        createWorkerComponent(parameters);

    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("Worker resource must not be null");
    workerComponent.createComponent();
  }

  @Test
  public void testWorkerComponentWithNullJobName() throws IOException {
    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setWorkerResource(testCommons.resource);
    parameters.setNumWorkers(1);
    parameters.setName(null);

    TensorFlowWorkerComponent workerComponent =
        createWorkerComponent(parameters);

    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("Job name must not be null");
    workerComponent.createComponent();
  }

  @Test
  public void testNormalWorkerComponentZeroNumberOfWorkers()
      throws IOException {
    testCommons.yarnConfig.set("hadoop.registry.dns.domain-name", "testDomain");

    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setWorkerResource(testCommons.resource);
    parameters.setName("testJobName");
    parameters.setWorkerDockerImage("testWorkerDockerImage");
    parameters.setNumWorkers(0);

    TensorFlowWorkerComponent workerComponent =
        createWorkerComponent(parameters);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Number of workers should be at least 1!");
    workerComponent.createComponent();
  }

  @Test
  public void testNormalWorkerComponentNumWorkersIsOne() throws IOException {
    testCommons.yarnConfig.set("hadoop.registry.dns.domain-name", "testDomain");

    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setWorkerResource(testCommons.resource);
    parameters.setName("testJobName");
    parameters.setNumWorkers(1);
    parameters.setWorkerDockerImage("testWorkerDockerImage");

    TensorFlowWorkerComponent workerComponent =
        createWorkerComponent(parameters);

    Component component = workerComponent.createComponent();

    assertEquals(0L, (long) component.getNumberOfContainers());
    verifyCommons(component);
  }

  @Test
  public void testNormalWorkerComponentNumWorkersIsTwo() throws IOException {
    testCommons.yarnConfig.set("hadoop.registry.dns.domain-name", "testDomain");

    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setWorkerResource(testCommons.resource);
    parameters.setName("testJobName");
    parameters.setNumWorkers(2);
    parameters.setWorkerDockerImage("testWorkerDockerImage");

    TensorFlowWorkerComponent workerComponent =
        createWorkerComponent(parameters);

    Component component = workerComponent.createComponent();

    assertEquals(1L, (long) component.getNumberOfContainers());
    verifyCommons(component);
  }

  @Test
  public void testPrimaryWorkerComponentNumWorkersIsTwo() throws IOException {
    testCommons.yarnConfig.set("hadoop.registry.dns.domain-name", "testDomain");
    testCommons = new ComponentTestCommons(TensorFlowRole.PRIMARY_WORKER);
    testCommons.setupTensorFlow();

    TensorFlowRunJobParameters parameters = new TensorFlowRunJobParameters();
    parameters.setWorkerResource(testCommons.resource);
    parameters.setName("testJobName");
    parameters.setNumWorkers(2);
    parameters.setWorkerDockerImage("testWorkerDockerImage");

    TensorFlowWorkerComponent workerComponent =
        createWorkerComponent(parameters);

    Component component = workerComponent.createComponent();

    assertEquals(1L, (long) component.getNumberOfContainers());
    // If the dependencies are upgraded to hadoop 3.3.0.
    // yarn.service.container-state-report-as-service-state can be replaced
    // with CONTAINER_STATE_REPORT_AS_SERVICE_STATE
    verifyCommons(component, ImmutableMap.of(
        "yarn.service.container-state-report-as-service-state", "true"));
  }

}