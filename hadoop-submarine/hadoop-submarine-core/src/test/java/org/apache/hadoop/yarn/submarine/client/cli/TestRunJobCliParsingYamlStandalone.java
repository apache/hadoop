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

import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Configs;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Role;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Roles;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Scheduling;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Security;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Spec;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.TensorBoard;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.YamlConfigFile;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.submarine.client.cli.YamlConfigTestUtils.readYamlConfigFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class that verifies the correctness of YAML configuration parsing.
 * Please note that this class just tests YAML parsing,
 * but only in an isolated fashion.
 */
public class TestRunJobCliParsingYamlStandalone {
  private static final String OVERRIDDEN_PREFIX = "overridden_";
  private static final String DIR_NAME = "runjobcliparsing";

  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }

  private void verifyBasicConfigValues(YamlConfigFile yamlConfigFile) {
    assertNotNull("Spec file should not be null!", yamlConfigFile);
    Spec spec = yamlConfigFile.getSpec();
    assertNotNull("Spec should not be null!", spec);

    assertEquals("testJobName", spec.getName());
    assertEquals("testJobType", spec.getJobType());

    Configs configs = yamlConfigFile.getConfigs();
    assertNotNull("Configs should not be null!", configs);

    assertEquals("testInputPath", configs.getInputPath());
    assertEquals("testCheckpointPath", configs.getCheckpointPath());
    assertEquals("testSavedModelPath", configs.getSavedModelPath());
    assertEquals("testDockerImage", configs.getDockerImage());

    Map<String, String> envs = configs.getEnvs();
    assertNotNull("Envs should not be null!", envs);
    assertEquals(2, envs.size());
    assertEquals("env1Value", envs.get("env1"));
    assertEquals("env2Value", envs.get("env2"));

    List<String> localizations = configs.getLocalizations();
    assertNotNull("Localizations should not be null!", localizations);
    assertEquals("Size of localizations must be 2!", 2, localizations.size());
    assertEquals("hdfs://remote-file1:/local-filename1:rw",
        localizations.get(0));
    assertEquals("nfs://remote-file2:/local-filename2:rw",
        localizations.get(1));

    List<String> mounts = configs.getMounts();
    assertNotNull("Mounts should not be null!", mounts);
    assertEquals("Size of mounts must be 2!", 2, mounts.size());
    assertEquals("/etc/passwd:/etc/passwd:rw", mounts.get(0));
    assertEquals("/etc/hosts:/etc/hosts:rw", mounts.get(1));

    assertTrue(
        configs.getQuicklinks().contains("Notebook_UI=https://master-0:7070"));
    assertTrue(
        configs.getQuicklinks().contains("Notebook_UI2=https://master-0:7071"));
    assertEquals("true", configs.getWaitJobFinish());
  }

  private void assertRoleConfigOverrides(Role role, String prefix,
      String roleType) {
    assertNotNull(roleType + " role should not be null!", role);

    assertEquals(String.format("%stestDockerImage%s", prefix, roleType),
        role.getDockerImage());

    //envs, localizations and mounts for Roles
    // are only present in valid-config-with-overrides.yaml
    boolean validateAll = !prefix.equals("");
    if (validateAll) {
      Map<String, String> envs = role.getEnvs();
      assertNotNull("Envs should not be null!", envs);
      assertEquals(String.format("%senv1%s", prefix, roleType),
          envs.get("env1"));
      assertEquals(String.format("%senv2%s", prefix, roleType),
          envs.get("env2"));
    }

    if (validateAll) {
      List<String> localizations = role.getLocalizations();
      assertNotNull("Localizations should not be null!", localizations);
      assertEquals("Size of localizations must be 2!", 2, localizations.size());
      assertEquals(String.format("hdfs://remote-file1:/%slocal" +
          "-filename1%s:rw", prefix, roleType), localizations.get(0));
      assertEquals(String.format("nfs://remote-file2:/%slocal" +
          "-filename2%s:rw", prefix, roleType), localizations.get(1));
    }

    if (validateAll) {
      List<String> mounts = role.getMounts();
      assertNotNull("Mounts should not be null!", mounts);
      assertEquals("Size of mounts must be 2!", 2, mounts.size());
      assertEquals(String.format("/etc/passwd:/%s%s", prefix, roleType),
          mounts.get(0));
      assertEquals(String.format("/etc/hosts:/%s%s", prefix, roleType),
          mounts.get(1));
    }
  }

  private void assertWorkerValues(Role worker) {
    assertEquals("testLaunchCmdWorker", worker.getLaunchCmd());
    assertEquals("testDockerImageWorker", worker.getDockerImage());
    assertEquals("memory=20480M,vcores=32,gpu=2", worker.getResources());
    assertEquals(3, worker.getReplicas());
  }

  private void assertPsValues(Role ps) {
    assertEquals("testLaunchCmdPs", ps.getLaunchCmd());
    assertEquals("testDockerImagePs", ps.getDockerImage());
    assertEquals("memory=20500M,vcores=34,gpu=4", ps.getResources());
    assertEquals(4, ps.getReplicas());
  }

  private void verifySchedulingValues(YamlConfigFile yamlConfigFile) {
    Scheduling scheduling = yamlConfigFile.getScheduling();
    assertNotNull("Scheduling should not be null!", scheduling);
    assertEquals("queue1", scheduling.getQueue());
  }

  private void verifySecurityValues(YamlConfigFile yamlConfigFile) {
    Security security = yamlConfigFile.getSecurity();
    assertNotNull("Security should not be null!", security);
    assertEquals("keytabPath", security.getKeytab());
    assertEquals("testPrincipal", security.getPrincipal());
    assertTrue(security.isDistributeKeytab());
  }

  private void verifyTensorboardValues(YamlConfigFile yamlConfigFile) {
    TensorBoard tensorBoard = yamlConfigFile.getTensorBoard();
    assertNotNull("Tensorboard should not be null!", tensorBoard);
    assertEquals("tensorboardDockerImage", tensorBoard.getDockerImage());
    assertEquals("memory=21000M,vcores=37,gpu=3", tensorBoard.getResources());
  }

  @Test
  public void testLaunchCommandYaml() {
    YamlConfigFile yamlConfigFile = readYamlConfigFile(DIR_NAME +
        "/valid-config.yaml");

    verifyBasicConfigValues(yamlConfigFile);

    Roles roles = yamlConfigFile.getRoles();
    assertNotNull("Roles should not be null!", roles);
    assertRoleConfigOverrides(roles.getWorker(), "", "Worker");
    assertRoleConfigOverrides(roles.getPs(), "", "Ps");

    assertWorkerValues(roles.getWorker());
    assertPsValues(roles.getPs());

    verifySchedulingValues(yamlConfigFile);
    verifySecurityValues(yamlConfigFile);
    verifyTensorboardValues(yamlConfigFile);
  }

  @Test
  public void testOverrides() {
    YamlConfigFile yamlConfigFile = readYamlConfigFile(DIR_NAME +
        "/valid-config-with-overrides.yaml");

    verifyBasicConfigValues(yamlConfigFile);

    Roles roles = yamlConfigFile.getRoles();
    assertNotNull("Roles should not be null!", roles);
    assertRoleConfigOverrides(roles.getWorker(), OVERRIDDEN_PREFIX, "Worker");
    assertRoleConfigOverrides(roles.getPs(), OVERRIDDEN_PREFIX, "Ps");
  }

}
