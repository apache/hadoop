/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.hadoop.yarn.service.conf.SliderXmlConfKeys;
import org.apache.slider.common.params.AbstractClusterBuildingActionArgs;
import org.apache.hadoop.yarn.service.client.params.ActionBuildArgs;
import org.apache.hadoop.yarn.service.client.params.ActionCreateArgs;
import org.apache.hadoop.yarn.service.client.params.ActionDestroyArgs;
import org.apache.slider.common.params.ActionExistsArgs;
import org.apache.hadoop.yarn.service.client.params.ActionFlexArgs;
import org.apache.slider.common.params.ActionFreezeArgs;
import org.apache.slider.common.params.ActionListArgs;
import org.apache.slider.common.params.ActionStatusArgs;
import org.apache.slider.common.params.ActionThawArgs;
import org.apache.slider.common.params.ActionUpdateArgs;
import org.apache.hadoop.yarn.service.client.params.ArgOps;
import org.apache.hadoop.yarn.service.client.params.Arguments;
import org.apache.hadoop.yarn.service.client.params.ClientArgs;
import org.apache.hadoop.yarn.service.client.params.SliderActions;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.SliderException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test handling of common arguments, specifically how things get split up.
 */
public class TestCommonArgParsing implements SliderActions, Arguments {


  public static final String CLUSTERNAME = "clustername";

  //@Test
  public void testCreateActionArgs() throws Throwable {
    ClientArgs clientArgs = createClientArgs(Arrays.asList(ACTION_CREATE,
        "cluster1"));
    assertEquals("cluster1", clientArgs.getClusterName());
  }

  //@Test
  public void testCreateFailsNoClustername() throws Throwable {
    assertParseFails(Arrays.asList(ACTION_CREATE));
  }

  //@Test
  public void testCreateFailsTwoClusternames() throws Throwable {
    assertParseFails(Arrays.asList(
        ACTION_CREATE,
        "c1",
        "c2"
    ));
  }

  //@Test
  public void testHelp() throws Throwable {
    ClientArgs clientArgs = createClientArgs(Arrays.asList(ACTION_HELP));
    assertNull(clientArgs.getClusterName());
  }

  //@Test
  public void testSliderBasePath() throws Throwable {
    ClientArgs clientArgs = createClientArgs(Arrays.asList(ACTION_LIST,
        ARG_BASE_PATH,  "/projects/slider/clusters"));
    assertEquals(new Path("/projects/slider/clusters"),
        clientArgs.getBasePath());
  }

  //@Test
  public void testNoSliderBasePath() throws Throwable {
    ClientArgs clientArgs = createClientArgs(Arrays.asList(ACTION_LIST));
    assertNull(clientArgs.getBasePath());
  }

  //@Test
  public void testListNoClusternames() throws Throwable {
    ClientArgs clientArgs = createClientArgs(Arrays.asList(ACTION_LIST));
    assertNull(clientArgs.getClusterName());
  }

  //@Test
  public void testListNoClusternamesDefinition() throws Throwable {
    ClientArgs clientArgs = createClientArgs(Arrays.asList(
        ACTION_LIST,
        ARG_DEFINE,
        "fs.default.FS=file://localhost"
        ));
    assertNull(clientArgs.getClusterName());
  }

  //@Test
  public void testList1Clustername() throws Throwable {
    ClientArgs ca = createClientArgs(Arrays.asList(ACTION_LIST, "cluster1"));
    assertEquals("cluster1", ca.getClusterName());
    assertTrue(ca.getCoreAction() instanceof ActionListArgs);
  }

  //@Test
  public void testListFailsTwoClusternames() throws Throwable {
    assertParseFails(Arrays.asList(
        ACTION_LIST,
        "c1",
        "c2"
      ));
  }

  //@Test
  public void testDefinitions() throws Throwable {
    ClientArgs ca = createClientArgs(Arrays.asList(
        ACTION_CREATE,
        CLUSTERNAME,
        "-D", "yarn.resourcemanager.principal=yarn/server@LOCAL",
        "-D", "dfs.datanode.kerberos.principal=hdfs/server@LOCAL"
    ));
    Configuration conf = new Configuration(false);
    ca.applyDefinitions(conf);
    assertEquals(CLUSTERNAME, ca.getClusterName());
    assertNull(conf.get(SliderXmlConfKeys.KEY_SLIDER_BASE_PATH));
    SliderUtils.verifyPrincipalSet(conf, YarnConfiguration.RM_PRINCIPAL);
    SliderUtils.verifyPrincipalSet(
        conf,
        SliderXmlConfKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY);

  }

  //@Test
  public void testDefinitionsSettingBaseSliderDir() throws Throwable {
    ClientArgs ca = createClientArgs(Arrays.asList(
        ACTION_CREATE,
        CLUSTERNAME,
        "--basepath", "/projects/slider/clusters",
        "-D", "yarn.resourcemanager.principal=yarn/server@LOCAL",
        "-D", "dfs.datanode.kerberos.principal=hdfs/server@LOCAL"
    ));
    Configuration conf = new Configuration(false);
    ca.applyDefinitions(conf);
    assertEquals(CLUSTERNAME, ca.getClusterName());
    assertEquals("/projects/slider/clusters", conf.get(SliderXmlConfKeys
        .KEY_SLIDER_BASE_PATH));
    SliderUtils.verifyPrincipalSet(conf, YarnConfiguration.RM_PRINCIPAL);
    SliderUtils.verifyPrincipalSet(conf, SliderXmlConfKeys
        .DFS_DATANODE_KERBEROS_PRINCIPAL_KEY);

  }

  /**
   * Test a start command.
   * @throws Throwable
   */
  //@Test
  public void testComplexThaw() throws Throwable {
    ClientArgs ca = createClientArgs(Arrays.asList(
        ACTION_START,
        "--manager", "rhel:8032",
        "--filesystem", "hdfs://rhel:9090",
        "-S", "java.security.krb5.realm=LOCAL",
        "-S", "java.security.krb5.kdc=rhel",
        "-D", "yarn.resourcemanager.principal=yarn/rhel@LOCAL",
        "-D", "namenode.resourcemanager.principal=hdfs/rhel@LOCAL",
        "cl1"
    ));
    assertEquals("cl1", ca.getClusterName());
    assertTrue(ca.getCoreAction() instanceof ActionThawArgs);
  }

  /**
   * Test a force kill command where the app comes at the end of the line.
   * @throws Throwable
   *
   */
  //@Test
  public void testStatusSplit() throws Throwable {

    String appId = "application_1381252124398_0013";
    ClientArgs ca = createClientArgs(Arrays.asList(
        ACTION_STATUS,
        "--manager", "rhel:8032",
        "--filesystem", "hdfs://rhel:9090",
        "-S", "java.security.krb5.realm=LOCAL",
        "-S", "java.security.krb5.kdc=rhel",
        "-D", "yarn.resourcemanager.principal=yarn/rhel@LOCAL",
        "-D", "namenode.resourcemanager.principal=hdfs/rhel@LOCAL",
        appId
    ));
    assertEquals(appId, ca.getClusterName());
  }

  //@Test
  public void testFreezeFailsNoArg() throws Throwable {
    assertParseFails(Arrays.asList(
        ACTION_STOP
    ));
  }

  //@Test
  public void testFreezeWorks1Arg() throws Throwable {
    ClientArgs ca = createClientArgs(Arrays.asList(
        ACTION_STOP,
        CLUSTERNAME
    ));
    assertEquals(CLUSTERNAME, ca.getClusterName());
    assertTrue(ca.getCoreAction() instanceof ActionFreezeArgs);
  }

  //@Test
  public void testFreezeFails2Arg() throws Throwable {
    assertParseFails(Arrays.asList(
        ACTION_STOP, "cluster", "cluster2"
    ));
  }

  //@Test
  public void testFreezeForceWaitAndMessage() throws Throwable {
    ClientArgs ca = createClientArgs(Arrays.asList(
        ACTION_STOP, CLUSTERNAME,
        ARG_FORCE,
        ARG_WAIT, "0",
        ARG_MESSAGE, "explanation"
    ));
    assertEquals(CLUSTERNAME, ca.getClusterName());
    assertTrue(ca.getCoreAction() instanceof ActionFreezeArgs);
    ActionFreezeArgs freezeArgs = (ActionFreezeArgs) ca.getCoreAction();
    assertEquals("explanation", freezeArgs.message);
    assertTrue(freezeArgs.force);
  }

  //@Test
  public void testGetStatusWorks1Arg() throws Throwable {
    ClientArgs ca = createClientArgs(Arrays.asList(
        ACTION_STATUS,
        CLUSTERNAME
    ));
    assertEquals(CLUSTERNAME, ca.getClusterName());
    assertTrue(ca.getCoreAction() instanceof ActionStatusArgs);
  }

  //@Test
  public void testExistsWorks1Arg() throws Throwable {
    ClientArgs ca = createClientArgs(Arrays.asList(
        ACTION_EXISTS,
        CLUSTERNAME,
        ARG_LIVE
    ));
    assertEquals(CLUSTERNAME, ca.getClusterName());
    assertTrue(ca.getCoreAction() instanceof ActionExistsArgs);
    assertTrue(ca.getActionExistsArgs().live);
  }

  //@Test
  public void testDestroy1Arg() throws Throwable {
    ClientArgs ca = createClientArgs(Arrays.asList(
        ACTION_DESTROY,
        CLUSTERNAME
    ));
    assertEquals(CLUSTERNAME, ca.getClusterName());
    assertTrue(ca.getCoreAction() instanceof ActionDestroyArgs);
  }

  /**
   * Assert that a pass fails with a BadCommandArgumentsException.
   * @param argsList
   */

  private void assertParseFails(List argsList) throws SliderException {
    try {
      ClientArgs clientArgs = createClientArgs(argsList);
      Assert.fail("exected an exception, got " + clientArgs);
    } catch (BadCommandArgumentsException ignored) {
      //expected
    }
  }

  /**
   * Build and parse client args, after adding the base args list.
   * @param argsList
   */
  public ClientArgs createClientArgs(List<String> argsList)
      throws SliderException {
    ClientArgs serviceArgs = new ClientArgs(argsList);
    serviceArgs.parse();
    return serviceArgs;
  }

  public ActionCreateArgs createAction(List<String> argsList)
      throws SliderException {
    ClientArgs ca = createClientArgs(argsList);
    assertEquals(ACTION_CREATE, ca.getAction());
    ActionCreateArgs args = ca.getActionCreateArgs();
    assertNotNull(args);
    return args;
  }

  //@Test
  public void testSingleRoleArg() throws Throwable {
    ActionCreateArgs createArgs = createAction(Arrays.asList(
        ACTION_CREATE, "cluster1",
        ARG_COMPONENT, "master", "5"
    ));
    List<String> tuples = createArgs.getComponentTuples();
    assertEquals(2, tuples.size());
    Map<String, String> roleMap = ArgOps.convertTupleListToMap("roles", tuples);
    assertEquals("5", roleMap.get("master"));
  }

  //@Test
  public void testNoRoleArg() throws Throwable {
    ActionCreateArgs createArgs = createAction(Arrays.asList(
        ACTION_CREATE, "cluster1"
    ));
    List<String> tuples = createArgs.getComponentTuples();
    Map<String, String> roleMap = ArgOps.convertTupleListToMap("roles", tuples);
    assertNull(roleMap.get("master"));
  }


  //@Test
  public void testMultiRoleArgBuild() throws Throwable {
    ClientArgs ca = createClientArgs(Arrays.asList(
        ACTION_BUILD, "cluster1",
        ARG_COMPONENT, "master", "1",
        ARG_COMPONENT, "worker", "2"
    ));
    assertEquals(ACTION_BUILD, ca.getAction());
    assertTrue(ca.getCoreAction() instanceof ActionBuildArgs);
    assertTrue(ca.getBuildingActionArgs() instanceof ActionBuildArgs);
    AbstractClusterBuildingActionArgs args = ca.getActionBuildArgs();
    List<String> tuples = args.getComponentTuples();
    assertEquals(4, tuples.size());
    Map<String, String> roleMap = ArgOps.convertTupleListToMap("roles", tuples);
    assertEquals("1", roleMap.get("master"));
    assertEquals("2", roleMap.get("worker"));
  }

  //@Test
  public void testArgUpdate() throws Throwable {
    ClientArgs ca = createClientArgs(Arrays.asList(
        ACTION_UPDATE, "cluster1",
        ARG_APPDEF, "app.json"
    ));
    assertEquals(ACTION_UPDATE, ca.getAction());
    assertTrue(ca.getCoreAction() instanceof ActionUpdateArgs);
    assertTrue(ca.getActionUpdateArgs() instanceof ActionUpdateArgs);
    AbstractClusterBuildingActionArgs args = ca.getActionUpdateArgs();
    assertNotNull(args.appDef);
  }

  //@Test
  public void testFlexArgs() throws Throwable {
    ClientArgs ca = createClientArgs(Arrays.asList(
        ACTION_FLEX, "cluster1",
        ARG_COMPONENT, "master", "1",
        ARG_COMPONENT, "worker", "2"
    ));
    assertTrue(ca.getCoreAction() instanceof ActionFlexArgs);
    List<String> tuples = ca.getActionFlexArgs().getComponentTuples();
    assertEquals(4, tuples.size());
    Map<String, String> roleMap = ArgOps.convertTupleListToMap("roles", tuples);
    assertEquals("1", roleMap.get("master"));
    assertEquals("2", roleMap.get("worker"));
  }

  //@Test
  public void testDuplicateRole() throws Throwable {
    ActionCreateArgs createArgs = createAction(Arrays.asList(
        ACTION_CREATE, "cluster1",
        ARG_COMPONENT, "master", "1",
        ARG_COMPONENT, "master", "2"
    ));
    List<String> tuples = createArgs.getComponentTuples();
    assertEquals(4, tuples.size());
    try {
      Map<String, String> roleMap = ArgOps.convertTupleListToMap(
          "roles",
          tuples);
      Assert.fail("got a role map $roleMap not a failure");
    } catch (BadCommandArgumentsException expected) {
      assertTrue(expected.getMessage().contains(ErrorStrings
          .ERROR_DUPLICATE_ENTRY));
    }
  }

  //@Test
  public void testOddRoleCount() throws Throwable {
    ActionCreateArgs createArgs = createAction(Arrays.asList(
        ACTION_CREATE, "cluster1",
        ARG_COMPONENT, "master", "1",
        ARG_COMPONENT, "master", "2"
    ));
    List<String> tuples = createArgs.getComponentTuples();
    tuples.add("loggers");
    assertEquals(5, tuples.size());
    try {
      Map<String, String> roleMap = ArgOps.convertTupleListToMap("roles",
          tuples);
      Assert.fail("got a role map " + roleMap + " not a failure");
    } catch (BadCommandArgumentsException expected) {
      assertTrue(expected.getMessage().contains(ErrorStrings
          .ERROR_PARSE_FAILURE));
    }
  }

  /**
   * Create some role-opt client args, so that multiple tests can use it.
   * @return the args
   */
  public ActionCreateArgs createRoleOptClientArgs() throws SliderException {
    ActionCreateArgs createArgs = createAction(Arrays.asList(
        ACTION_CREATE, "cluster1",
        ARG_COMPONENT, "master", "1",
        ARG_COMP_OPT, "master", "cheese", "swiss",
        ARG_COMP_OPT, "master", "env.CHEESE", "cheddar",
        ARG_COMP_OPT, "master", ResourceKeys.YARN_CORES, "3",

        ARG_COMPONENT, "worker", "2",
        ARG_COMP_OPT, "worker", ResourceKeys.YARN_CORES, "2",
        ARG_COMP_OPT, "worker", RoleKeys.JVM_HEAP, "65536",
        ARG_COMP_OPT, "worker", "env.CHEESE", "stilton"
    ));
    return createArgs;
  }

  //@Test
  public void testRoleOptionParse() throws Throwable {
    ActionCreateArgs createArgs = createRoleOptClientArgs();
    Map<String, Map<String, String>> tripleMaps = createArgs.getCompOptionMap();
    Map<String, String> workerOpts = tripleMaps.get("worker");
    assertEquals(3, workerOpts.size());
    assertEquals("2", workerOpts.get(ResourceKeys.YARN_CORES));
    assertEquals("65536", workerOpts.get(RoleKeys.JVM_HEAP));

    Map<String, String> masterOpts = tripleMaps.get("master");
    assertEquals(3, masterOpts.size());
    assertEquals("3", masterOpts.get(ResourceKeys.YARN_CORES));

  }

  //@Test
  public void testRoleOptionsMerge() throws Throwable {
    ActionCreateArgs createArgs = createRoleOptClientArgs();

    Map<String, Map<String, String>> roleOpts = createArgs.getCompOptionMap();

    Map<String, Map<String, String>> clusterRoleMap = createEnvMap();
    SliderUtils.applyCommandLineRoleOptsToRoleMap(clusterRoleMap, roleOpts);

    Map<String, String> masterOpts = clusterRoleMap.get("master");
    assertEquals("swiss", masterOpts.get("cheese"));

    Map<String, String> workerOpts = clusterRoleMap.get("worker");
    assertEquals("stilton", workerOpts.get("env.CHEESE"));
  }

  //@Test
  public void testEnvVariableApply() throws Throwable {
    ActionCreateArgs createArgs = createRoleOptClientArgs();


    Map<String, Map<String, String>> roleOpts = createArgs.getCompOptionMap();

    Map<String, Map<String, String>> clusterRoleMap = createEnvMap();
    SliderUtils.applyCommandLineRoleOptsToRoleMap(clusterRoleMap, roleOpts);

    Map<String, String> workerOpts = clusterRoleMap.get("worker");
    assertEquals("stilton", workerOpts.get("env.CHEESE"));

    Map<String, String> envmap = SliderUtils.buildEnvMap(workerOpts);
    assertEquals("stilton", envmap.get("CHEESE"));

  }

  /**
   * Static compiler complaining about matching LinkedHashMap with Map,
   * so some explicit creation here.
   * @return a map of maps
   */
  public Map<String, Map<String, String>> createEnvMap() {

    Map<String, String> cheese = new HashMap<>();
    cheese.put("cheese", "french");
    Map<String, String> envCheese = new HashMap<>();
    envCheese.put("env.CHEESE", "french");
    Map<String, Map<String, String>> envMap = new HashMap<>();
    envMap.put("master", cheese);
    envMap.put("worker", envCheese);
    return envMap;
  }


}
