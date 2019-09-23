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


package org.apache.hadoop.yarn.submarine.client.cli.runjob.pytorch;

import static org.apache.hadoop.yarn.submarine.client.cli.runjob.TestRunJobCliParsingCommon.getMockClientContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.submarine.client.cli.YamlConfigTestUtils;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.PyTorchRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.YamlParseException;
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

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class that verifies the correctness of PyTorch
 * YAML configuration parsing.
 */
public class TestRunJobCliParsingPyTorchYaml {
  private static final String OVERRIDDEN_PREFIX = "overridden_";
  private static final String DIR_NAME = "runjob-pytorch-yaml";
  private File yamlConfig;
  private static Logger LOG = LoggerFactory.getLogger(
      TestRunJobCliParsingPyTorchYaml.class);

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

  private PyTorchRunJobParameters verifyWorkerCommonValues(RunJobParameters
      jobRunParameters, String prefix) {
    assertTrue(RunJobParameters.class + " must be an instance of " +
            PyTorchRunJobParameters.class,
        jobRunParameters instanceof PyTorchRunJobParameters);
    PyTorchRunJobParameters pyTorchParams =
        (PyTorchRunJobParameters) jobRunParameters;

    assertEquals(3, pyTorchParams.getNumWorkers());
    assertEquals(prefix + "testLaunchCmdWorker",
        pyTorchParams.getWorkerLaunchCmd());
    assertEquals(prefix + "testDockerImageWorker",
        pyTorchParams.getWorkerDockerImage());
    return pyTorchParams;
  }

  private void verifyWorkerValues(RunJobParameters jobRunParameters,
      String prefix) {
    PyTorchRunJobParameters pyTorchParams = verifyWorkerCommonValues
        (jobRunParameters, prefix);
    assertEquals(Resources.createResource(20480, 32),
        pyTorchParams.getWorkerResource());
  }

  private void verifyWorkerValuesWithGpu(RunJobParameters jobRunParameters,
      String prefix) {

    PyTorchRunJobParameters pyTorchParams = verifyWorkerCommonValues
        (jobRunParameters, prefix);
    Resource workResource = Resources.createResource(20480, 32);
    ResourceUtils.setResource(workResource, ResourceUtils.GPU_URI, 2);
    assertEquals(workResource, pyTorchParams.getWorkerResource());
  }

  private void verifySecurityValues(RunJobParameters jobRunParameters) {
    assertEquals("keytabPath", jobRunParameters.getKeytab());
    assertEquals("testPrincipal", jobRunParameters.getPrincipal());
    assertTrue(jobRunParameters.isDistributeKeytab());
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
    verifyWorkerValues(jobRunParameters, "");
    verifySecurityValues(jobRunParameters);
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
    verifyWorkerValuesWithGpu(jobRunParameters, "");
    verifySecurityValues(jobRunParameters);
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
    verifyWorkerValues(jobRunParameters, OVERRIDDEN_PREFIX);
    verifySecurityValues(jobRunParameters);
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
    verifyWorkerValues(jobRunParameters, "");

    //Verify security values
    assertEquals("keytabPath", jobRunParameters.getKeytab());
    assertNull("Principal should be null!", jobRunParameters.getPrincipal());
    assertTrue(jobRunParameters.isDistributeKeytab());
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
    verifyWorkerValues(jobRunParameters, "");
    verifySecurityValues(jobRunParameters);
  }

  @Test
  public void testInvalidConfigPsSectionIsDefined() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    exception.expect(YamlParseException.class);
    exception.expectMessage("PS section should not be defined " +
        "when PyTorch is the selected framework");
    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/invalid-config-ps-section.yaml");
    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});
  }

  @Test
  public void testInvalidConfigTensorboardSectionIsDefined() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    exception.expect(YamlParseException.class);
    exception.expectMessage("TensorBoard section should not be defined " +
        "when PyTorch is the selected framework");
    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/invalid-config-tensorboard-section.yaml");
    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});
  }

}
