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


package org.apache.hadoop.yarn.submarine.client.cli;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.resourcetypes.ResourceTypesTestHelper;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.YamlParseException;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.submarine.client.cli.TestRunJobCliParsing.getMockClientContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class that verifies the correctness of YAML configuration parsing.
 */
public class TestRunJobCliParsingYaml {
  private static final String OVERRIDDEN_PREFIX = "overridden_";
  private static final String DIR_NAME = "runjobcliparsing";
  private File yamlConfig;

  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }

  @After
  public void after() {
    YamlConfigTestUtils.deleteFile(yamlConfig);
  }

  @BeforeClass
  public static void configureResourceTypes() {
    List<ResourceTypeInfo> resTypes = new ArrayList<>(
        ResourceUtils.getResourcesTypeInfo());
    resTypes.add(ResourceTypeInfo.newInstance(ResourceInformation.GPU_URI, ""));
    ResourceUtils.reinitializeResources(resTypes);
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
    assertEquals(4, jobRunParameters.getNumPS());
    assertEquals(prefix + "testLaunchCmdPs", jobRunParameters.getPSLaunchCmd());
    assertEquals(prefix + "testDockerImagePs",
        jobRunParameters.getPsDockerImage());
    assertEquals(ResourceTypesTestHelper.newResource(20500L, 34,
        ImmutableMap.<String, String> builder()
            .put(ResourceInformation.GPU_URI, "4").build()),
        jobRunParameters.getPsResource());
  }

  private void verifyWorkerValues(RunJobParameters jobRunParameters,
      String prefix) {
    assertEquals(3, jobRunParameters.getNumWorkers());
    assertEquals(prefix + "testLaunchCmdWorker",
        jobRunParameters.getWorkerLaunchCmd());
    assertEquals(prefix + "testDockerImageWorker",
        jobRunParameters.getWorkerDockerImage());
    assertEquals(ResourceTypesTestHelper.newResource(20480L, 32,
        ImmutableMap.<String, String> builder()
            .put(ResourceInformation.GPU_URI, "2").build()),
        jobRunParameters.getWorkerResource());
  }

  private void verifySecurityValues(RunJobParameters jobRunParameters) {
    assertEquals("keytabPath", jobRunParameters.getKeytab());
    assertEquals("testPrincipal", jobRunParameters.getPrincipal());
    assertTrue(jobRunParameters.isDistributeKeytab());
  }

  private void verifyTensorboardValues(RunJobParameters jobRunParameters) {
    assertTrue(jobRunParameters.isTensorboardEnabled());
    assertEquals("tensorboardDockerImage",
        jobRunParameters.getTensorboardDockerImage());
    assertEquals(ResourceTypesTestHelper.newResource(21000L, 37,
        ImmutableMap.<String, String> builder()
            .put(ResourceInformation.GPU_URI, "3").build()),
        jobRunParameters.getTensorboardResource());
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
  public void testYamlAndCliOptionIsDefinedIsInvalid() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/valid-config.yaml");
    String[] args = new String[] {"--name", "my-job",
        "--docker_image", "tf-docker:1.1.0",
        "-f", yamlConfig.getAbsolutePath() };

    exception.expect(YarnException.class);
    exception.expectMessage("defined both with YAML config and with " +
        "CLI argument");

    runJobCli.run(args);
  }

  @Test
  public void testYamlAndCliOptionIsDefinedIsInvalidWithListOption()
      throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/valid-config.yaml");
    String[] args = new String[] {"--name", "my-job",
        "--quicklink", "AAA=http://master-0:8321",
        "--quicklink", "BBB=http://worker-0:1234",
        "-f", yamlConfig.getAbsolutePath()};

    exception.expect(YarnException.class);
    exception.expectMessage("defined both with YAML config and with " +
        "CLI argument");

    runJobCli.run(args);
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
  public void testFalseValuesForBooleanFields() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/test-false-values.yaml");
    runJobCli.run(
        new String[] {"-f", yamlConfig.getAbsolutePath(), "--verbose"});
    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();

    assertFalse(jobRunParameters.isDistributeKeytab());
    assertFalse(jobRunParameters.isWaitJobFinish());
    assertFalse(jobRunParameters.isTensorboardEnabled());
  }

  @Test
  public void testWrongIndentation() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/wrong-indentation.yaml");

    exception.expect(YamlParseException.class);
    exception.expectMessage("Failed to parse YAML config, details:");
    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});
  }

  @Test
  public void testWrongFilename() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    exception.expect(YamlParseException.class);
    runJobCli.run(
        new String[]{"-f", "not-existing", "--verbose"});
  }

  @Test
  public void testEmptyFile() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createEmptyTempFile();

    exception.expect(YamlParseException.class);
    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});
  }

  @Test
  public void testNotExistingFile() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    exception.expect(YamlParseException.class);
    exception.expectMessage("file does not exist");
    runJobCli.run(
        new String[]{"-f", "blabla", "--verbose"});
  }

  @Test
  public void testWrongPropertyName() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/wrong-property-name.yaml");

    exception.expect(YamlParseException.class);
    exception.expectMessage("Failed to parse YAML config, details:");
    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});
  }

  @Test
  public void testMissingConfigsSection() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/missing-configs.yaml");

    exception.expect(YamlParseException.class);
    exception.expectMessage("config section should be defined, " +
        "but it cannot be found");
    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});
  }

  @Test
  public void testMissingSectionsShouldParsed() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/some-sections-missing.yaml");
    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});
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

    assertTrue(jobRunParameters.isTensorboardEnabled());
    assertNull("tensorboardDockerImage should be null!",
        jobRunParameters.getTensorboardDockerImage());
    assertEquals(ResourceTypesTestHelper.newResource(21000L, 37,
        ImmutableMap.<String, String> builder()
            .put(ResourceInformation.GPU_URI, "3").build()),
        jobRunParameters.getTensorboardResource());
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
