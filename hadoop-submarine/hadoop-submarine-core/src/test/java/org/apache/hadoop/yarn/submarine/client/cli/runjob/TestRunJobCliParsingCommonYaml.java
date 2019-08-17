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

package org.apache.hadoop.yarn.submarine.client.cli.runjob;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.YamlConfigTestUtils;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.YamlParseException;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineRuntimeException;
import org.apache.hadoop.yarn.submarine.common.resource.ResourceUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.apache.hadoop.yarn.submarine.client.cli.runjob.TestRunJobCliParsingCommon.getMockClientContext;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This class contains some test methods to test common YAML parsing
 * functionality (including TF / PyTorch) of the run job Submarine command.
 */
public class TestRunJobCliParsingCommonYaml {
  private static final String DIR_NAME = "runjob-common-yaml";
  private static final String TF_DIR = "runjob-pytorch-yaml";
  private File yamlConfig;
  private static Logger LOG = LoggerFactory.getLogger(
      TestRunJobCliParsingCommonYaml.class);

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
    try {
      ResourceUtils.configureResourceType(ResourceUtils.GPU_URI);
    } catch (SubmarineRuntimeException e) {
      LOG.info("The hadoop dependency doesn't support gpu resource, " +
          "so just skip this test case.");
    }
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testYamlAndCliOptionIsDefinedIsInvalid() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        TF_DIR + "/valid-config.yaml");
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
        TF_DIR + "/valid-config.yaml");
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
  public void testFalseValuesForBooleanFields() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/test-false-values.yaml");
    runJobCli.run(
        new String[] {"-f", yamlConfig.getAbsolutePath(), "--verbose"});
    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();

    assertTrue(RunJobParameters.class + " must be an instance of " +
            TensorFlowRunJobParameters.class,
        jobRunParameters instanceof TensorFlowRunJobParameters);
    TensorFlowRunJobParameters tensorFlowParams =
        (TensorFlowRunJobParameters) jobRunParameters;

    assertFalse(jobRunParameters.isDistributeKeytab());
    assertFalse(jobRunParameters.isWaitJobFinish());
    assertFalse(tensorFlowParams.isTensorboardEnabled());
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
  public void testAbsentFramework() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/missing-framework.yaml");

    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});
  }

  @Test
  public void testEmptyFramework() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/empty-framework.yaml");

    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});
  }

  @Test
  public void testInvalidFramework() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        DIR_NAME + "/invalid-framework.yaml");

    exception.expect(YamlParseException.class);
    exception.expectMessage("framework should is defined, " +
        "but it has an invalid value");
    runJobCli.run(
        new String[]{"-f", yamlConfig.getAbsolutePath(), "--verbose"});
  }
}
