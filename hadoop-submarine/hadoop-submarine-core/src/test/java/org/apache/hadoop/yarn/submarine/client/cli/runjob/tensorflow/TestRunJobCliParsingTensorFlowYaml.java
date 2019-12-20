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


package org.apache.hadoop.yarn.submarine.client.cli.runjob.tensorflow;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.submarine.client.cli.YamlConfigTestUtils;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.runjob.RunJobCli;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineRuntimeException;
import org.apache.hadoop.yarn.submarine.common.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

import static org.apache.hadoop.yarn.submarine.client.cli.runjob.TestRunJobCliParsingCommon.getMockClientContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class that verifies the correctness of TF YAML configuration parsing.
 */
public class TestRunJobCliParsingTensorFlowYaml {
  private static final String OVERRIDDEN_PREFIX = "overridden_";
  private static final String DIR_NAME = "runjob-tensorflow-yaml";
  private File yamlConfig;
  private static Logger LOG = LoggerFactory.getLogger(
      TestRunJobCliParsingTensorFlowYaml.class);

  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }

  @After
  public void after() {
    YamlConfigTestUtils.deleteFile(yamlConfig);
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private void verifyBasicConfigValues(RunJobParameters jobRunParameters) {
    verifyBasicConfigValues(jobRunParameters,
        ImmutableList.of("env1=env1Value", "env2=env2Value"));
  }

  private void verifyBasicConfigValues(RunJobParameters jobRunParameters,
      List<String> expectedEnvs) {
    assertEquals("testInputPath", jobRunParameters.getInputPath());
    assertEquals("testCheckpointPath", jobRunParameters.getCheckpointPath());
    assertEquals("testDockerImage", jobRunParameters.getDockerImageName());

    assertNotNull(jobRunParameters.getLocalizations());
    assertEquals(2, jobRunParameters.getLocalizations().size());

    assertNotNull(jobRunParameters.getQuicklinks());
    assertEquals(2, jobRunParameters.getQuicklinks().size());

    assertTrue(SubmarineLogs.isVerbose());
    assertTrue(jobRunParameters.isWaitJobFinish());

    for (String env : expectedEnvs) {
      assertTrue(String.format(
          "%s should be in env list of jobRunParameters!", env),
          jobRunParameters.getEnvars().contains(env));
    }
  }

  private void verifyPsValues(RunJobParameters jobRunParameters,
      String prefix) {
    assertTrue(RunJobParameters.class + " must be an instance of " +
            TensorFlowRunJobParameters.class,
        jobRunParameters instanceof TensorFlowRunJobParameters);
    TensorFlowRunJobParameters tensorFlowParams =
        (TensorFlowRunJobParameters) jobRunParameters;

    assertEquals(4, tensorFlowParams.getNumPS());
    assertEquals(prefix + "testLaunchCmdPs", tensorFlowParams.getPSLaunchCmd());
    assertEquals(prefix + "testDockerImagePs",
        tensorFlowParams.getPsDockerImage());
    assertEquals(Resources.createResource(20500, 34),
        tensorFlowParams.getPsResource());
  }

  private TensorFlowRunJobParameters verifyWorkerCommonValues(
      RunJobParameters jobRunParameters, String prefix) {
    assertTrue(RunJobParameters.class + " must be an instance of " +
            TensorFlowRunJobParameters.class,
        jobRunParameters instanceof TensorFlowRunJobParameters);
    TensorFlowRunJobParameters tensorFlowParams =
        (TensorFlowRunJobParameters) jobRunParameters;

    assertEquals(3, tensorFlowParams.getNumWorkers());
    assertEquals(prefix + "testLaunchCmdWorker",
        tensorFlowParams.getWorkerLaunchCmd());
    assertEquals(prefix + "testDockerImageWorker",
        tensorFlowParams.getWorkerDockerImage());
    return tensorFlowParams;
  }

  private void verifyWorkerValues(RunJobParameters jobRunParameters,
      String prefix) {
    TensorFlowRunJobParameters tensorFlowParams = verifyWorkerCommonValues
        (jobRunParameters, prefix);
    assertEquals(Resources.createResource(20480, 32),
        tensorFlowParams.getWorkerResource());
  }

  private void verifyWorkerValuesWithGpu(RunJobParameters jobRunParameters,
                                  String prefix) {
    TensorFlowRunJobParameters tensorFlowParams = verifyWorkerCommonValues
        (jobRunParameters, prefix);
    Resource workResource = Resources.createResource(20480, 32);
    ResourceUtils.setResource(workResource, ResourceUtils.GPU_URI, 2);
    assertEquals(workResource, tensorFlowParams.getWorkerResource());
  }

  private void verifySecurityValues(RunJobParameters jobRunParameters) {
    assertEquals("keytabPath", jobRunParameters.getKeytab());
    assertEquals("testPrincipal", jobRunParameters.getPrincipal());
    assertTrue(jobRunParameters.isDistributeKeytab());
  }

  private void verifyTensorboardValues(RunJobParameters jobRunParameters) {
    assertTrue(RunJobParameters.class + " must be an instance of " +
            TensorFlowRunJobParameters.class,
        jobRunParameters instanceof TensorFlowRunJobParameters);
    TensorFlowRunJobParameters tensorFlowParams =
        (TensorFlowRunJobParameters) jobRunParameters;

    assertTrue(tensorFlowParams.isTensorboardEnabled());
    assertEquals("tensorboardDockerImage",
        tensorFlowParams.getTensorboardDockerImage());
    assertEquals(Resources.createResource(21000, 37),
        tensorFlowParams.getTensorboardResource());
  }

  @Test
  public void testValidYamlParsing() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/valid-config.yaml");
    runJobCli.run(
        new String[] {"-f", yamlConfig.getAbsolutePath(), "--verbose"});

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();
    verifyBasicConfigValues(jobRunParameters);
    verifyPsValues(jobRunParameters, "");
    verifyWorkerValues(jobRunParameters, "");
    verifySecurityValues(jobRunParameters);
    verifyTensorboardValues(jobRunParameters);
  }

  @Test
  public void testValidGpuYamlParsing() throws Exception {
    try {
      ResourceUtils.configureResourceType(ResourceUtils.GPU_URI);
    } catch (SubmarineRuntimeException e) {
      LOG.info("The hadoop dependency doesn't support gpu resource, " +
          "so just skip this test case.");
      return;
    }

    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/valid-gpu-config.yaml");
    runJobCli.run(
        new String[] {"-f", yamlConfig.getAbsolutePath(), "--verbose"});

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();
    verifyBasicConfigValues(jobRunParameters);
    verifyPsValues(jobRunParameters, "");
    verifyWorkerValuesWithGpu(jobRunParameters, "");
    verifySecurityValues(jobRunParameters);
    verifyTensorboardValues(jobRunParameters);
  }

  @Test
  public void testRoleOverrides() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/valid-config-with-overrides.yaml");
    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();
    verifyBasicConfigValues(jobRunParameters);
    verifyPsValues(jobRunParameters, OVERRIDDEN_PREFIX);
    verifyWorkerValues(jobRunParameters, OVERRIDDEN_PREFIX);
    verifySecurityValues(jobRunParameters);
    verifyTensorboardValues(jobRunParameters);
  }

  @Test
  public void testMissingPrincipalUnderSecuritySection() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/security-principal-is-missing.yaml");
    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();
    verifyBasicConfigValues(jobRunParameters);
    verifyPsValues(jobRunParameters, "");
    verifyWorkerValues(jobRunParameters, "");
    verifyTensorboardValues(jobRunParameters);

    //Verify security values
    assertEquals("keytabPath", jobRunParameters.getKeytab());
    assertNull("Principal should be null!", jobRunParameters.getPrincipal());
    assertTrue(jobRunParameters.isDistributeKeytab());
  }

  @Test
  public void testMissingTensorBoardDockerImage() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/tensorboard-dockerimage-is-missing.yaml");
    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();

    verifyBasicConfigValues(jobRunParameters);
    verifyPsValues(jobRunParameters, "");
    verifyWorkerValues(jobRunParameters, "");
    verifySecurityValues(jobRunParameters);

    TensorFlowRunJobParameters tensorFlowParams =
        (TensorFlowRunJobParameters) jobRunParameters;

    assertTrue(tensorFlowParams.isTensorboardEnabled());
    assertNull("tensorboardDockerImage should be null!",
        tensorFlowParams.getTensorboardDockerImage());
    assertEquals(Resources.createResource(21000, 37),
        tensorFlowParams.getTensorboardResource());
  }

  @Test
  public void testMissingEnvs() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/envs-are-missing.yaml");
    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();
    verifyBasicConfigValues(jobRunParameters, ImmutableList.of());
    verifyPsValues(jobRunParameters, "");
    verifyWorkerValues(jobRunParameters, "");
    verifySecurityValues(jobRunParameters);
    verifyTensorboardValues(jobRunParameters);
  }

}
