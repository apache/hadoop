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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.DYNAMIC_MAX_ASSIGN;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.MAX_CAPACITY_PERCENTAGE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.MAX_CHILD_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.QUEUE_AUTO_CREATE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.RESERVATION_SYSTEM;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.SPECIFIED_NOT_FIRST;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.USER_MAX_APPS_DEFAULT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.USER_MAX_RUNNING_APPS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.RuleAction.ABORT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.RuleAction.WARNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;


/**
 * Unit tests for FSConfigToCSConfigConverter.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class TestFSConfigToCSConfigConverter {
  private static final String CLUSTER_RESOURCE_STRING =
      "vcores=20, memory-mb=240";
  private static final Resource CLUSTER_RESOURCE =
      Resource.newInstance(16384, 16);
  private static final String FILE_PREFIX = "file:";
  private static final String FAIR_SCHEDULER_XML =
      prepareFileName("fair-scheduler-conversion.xml");
  private static final String FS_INVALID_PLACEMENT_RULES_XML =
      prepareFileName("fair-scheduler-invalidplacementrules.xml");
  private static final String FS_ONLY_FAIR_POLICY_XML =
      prepareFileName("fair-scheduler-onlyfairpolicy.xml");
  private static final String FS_MIXED_POLICY_XML =
      prepareFileName("fair-scheduler-orderingpolicy-mixed.xml");
  private static final String FS_NO_PLACEMENT_RULES_XML =
      prepareFileName("fair-scheduler-noplacementrules.xml");
  private static final String FS_MAX_AM_SHARE_DISABLED_XML =
      prepareFileName("fair-scheduler-defaultMaxAmShareDisabled.xml");

  @Mock
  private FSConfigToCSConfigRuleHandler ruleHandler;

  @Mock
  private DryRunResultHolder dryRunResultHolder;

  @Mock
  private QueuePlacementConverter placementConverter;

  private FSConfigToCSConfigConverter converter;
  private Configuration config;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private FSConfigConverterTestCommons converterTestCommons;

  private static String prepareFileName(String f) {
    return FILE_PREFIX + new File("src/test/resources/" + f).getAbsolutePath();
  }

  private static final String FAIR_SCHEDULER_XML_INVALID =
      prepareFileName("fair-scheduler-invalid.xml");
  private static final String YARN_SITE_XML =
      prepareFileName("yarn-site-with-allocation-file-ref.xml");
  private static final String YARN_SITE_XML_NO_REF_TO_FS_XML =
      prepareFileName("yarn-site.xml");
  private static final String YARN_SITE_XML_INVALID =
      prepareFileName("yarn-site-with-invalid-allocation-file-ref.xml");
  private static final String CONVERSION_RULES_FILE =
      new File("src/test/resources/conversion-rules.properties")
        .getAbsolutePath();

  private ConversionOptions createDefaultConversionOptions() {
    return new ConversionOptions(new DryRunResultHolder(), false);
  }

  @Before
  public void setup() throws IOException {
    config = new Configuration(false);
    config.set(FairSchedulerConfiguration.ALLOCATION_FILE, FAIR_SCHEDULER_XML);
    config.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
    createConverter();
    converterTestCommons = new FSConfigConverterTestCommons();
    converterTestCommons.setUp();
  }

  @After
  public void tearDown() {
    converterTestCommons.tearDown();
  }

  private void createConverter() {
    converter = new FSConfigToCSConfigConverter(ruleHandler,
        createDefaultConversionOptions());
    converter.setClusterResource(CLUSTER_RESOURCE);
  }

  private FSConfigToCSConfigConverterParams.Builder
      createDefaultParamsBuilder() {
    return FSConfigToCSConfigConverterParams.Builder.create()
        .withYarnSiteXmlConfig(YARN_SITE_XML)
        .withOutputDirectory(FSConfigConverterTestCommons.OUTPUT_DIR);
  }

  private FSConfigToCSConfigConverterParams.Builder
      createParamsBuilder(String yarnSiteConfig) {
    return FSConfigToCSConfigConverterParams.Builder.create()
        .withYarnSiteXmlConfig(yarnSiteConfig)
        .withOutputDirectory(FSConfigConverterTestCommons.OUTPUT_DIR);
  }



  @Test
  public void testDefaultMaxAMShare() throws Exception {
    converter.convert(config);

    Configuration conf = converter.getCapacitySchedulerConfig();
    String maxAmShare =
        conf.get(CapacitySchedulerConfiguration.
            MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT);

    assertEquals("Default max AM share", "0.16", maxAmShare);

    assertEquals("root.admins.alice max-am-resource-percent", "0.15",
        conf.get(PREFIX + "root.admins.alice.maximum-am-resource-percent"));

    assertNull("root.users.joe maximum-am-resource-percent should be null",
        conf.get(PREFIX + "root.users.joe maximum-am-resource-percent"));
  }

  @Test
  public void testDefaultMaxAMShareDisabled() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .withFairSchedulerXmlConfig(FS_MAX_AM_SHARE_DISABLED_XML)
        .build();

    converter.convert(params);

    Configuration conf = converter.getCapacitySchedulerConfig();

    // -1.0 means disabled ==> 1.0 in CS
    assertEquals("Default max-am-resource-percent", "1.0",
        conf.get(CapacitySchedulerConfiguration.
            MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT));

    // root.admins.bob -1.0 equals to the default -1.0
    assertNull("root.admins.bob maximum-am-resource-percent should be null",
        conf.get(PREFIX + "root.admins.bob.maximum-am-resource-percent"));

    // root.admins.alice 0.15 != -1.0
    assertEquals("root.admins.alice max-am-resource-percent", "0.15",
        conf.get(PREFIX + "root.admins.alice.maximum-am-resource-percent"));

    // root.users.joe is unset, inherits -1.0
    assertNull("root.users.joe maximum-am-resource-percent should be null",
        conf.get(PREFIX + "root.users.joe.maximum-am-resource-percent"));
  }

  @Test
  public void testConvertACLs() throws Exception {
    converter.convert(config);

    Configuration conf = converter.getCapacitySchedulerConfig();

    // root
    assertEquals("root submit ACL", "alice,bob,joe,john hadoop_users",
        conf.get(PREFIX + "root.acl_submit_applications"));
    assertEquals("root admin ACL", "alice,bob,joe,john hadoop_users",
        conf.get(PREFIX + "root.acl_administer_queue"));

    // root.admins.bob
    assertEquals("root.admins.bob submit ACL", "bob ",
        conf.get(PREFIX + "root.admins.bob.acl_submit_applications"));
    assertEquals("root.admins.bob admin ACL", "bob ",
        conf.get(PREFIX + "root.admins.bob.acl_administer_queue"));

    // root.admins.alice
    assertEquals("root.admins.alice submit ACL", "alice ",
        conf.get(PREFIX + "root.admins.alice.acl_submit_applications"));
    assertEquals("root.admins.alice admin ACL", "alice ",
        conf.get(PREFIX + "root.admins.alice.acl_administer_queue"));

    // root.users.john
    assertEquals("root.users.john submit ACL", "john ",
        conf.get(PREFIX + "root.users.john.acl_submit_applications"));
    assertEquals("root.users.john admin ACL", "john ",
        conf.get(PREFIX + "root.users.john.acl_administer_queue"));

    // root.users.joe
    assertEquals("root.users.joe submit ACL", "joe ",
        conf.get(PREFIX + "root.users.joe.acl_submit_applications"));
    assertEquals("root.users.joe admin ACL", "joe ",
        conf.get(PREFIX + "root.users.joe.acl_administer_queue"));
  }

  @Test
  public void testDefaultQueueMaxParallelApps() throws Exception {
    converter.convert(config);

    Configuration conf = converter.getCapacitySchedulerConfig();

    assertEquals("Default max parallel apps", 15,
        conf.getInt(PREFIX + "max-parallel-apps", -1));
  }

  @Test
  public void testSpecificQueueMaxParallelApps() throws Exception {
    converter.convert(config);

    Configuration conf = converter.getCapacitySchedulerConfig();

    assertEquals("root.admins.alice max parallel apps", 2,
        conf.getInt(PREFIX + "root.admins.alice.max-parallel-apps", -1));
  }

  @Test
  public void testDefaultUserMaxParallelApps() throws Exception {
    converter.convert(config);

    Configuration conf = converter.getCapacitySchedulerConfig();
    int userMaxParallelApps =
        conf.getInt(
            PREFIX + "user.max-parallel-apps", -1);

    assertEquals("Default user max parallel apps", 10,
        userMaxParallelApps);
  }

  @Test
  public void testSpecificUserMaxParallelApps() throws Exception {
    converter.convert(config);

    Configuration conf = converter.getCapacitySchedulerConfig();

    assertEquals("Max parallel apps for alice", 30,
        conf.getInt(PREFIX + "user.alice.max-parallel-apps", -1));
    assertNull("Max parallel apps should be undefined for user bob",
        conf.get(PREFIX + "user.bob.max-parallel-apps"));
    assertNull("Max parallel apps should be undefined for user joe",
        conf.get(PREFIX + "user.joe.max-parallel-apps"));
    assertNull("Max parallel apps should be undefined for user john",
        conf.get(PREFIX + "user.john.max-parallel-apps"));
  }

  @Test
  public void testQueueMaxChildCapacityNotSupported() throws Exception {
    expectedException.expect(UnsupportedPropertyException.class);
    expectedException.expectMessage("test");

    Mockito.doThrow(new UnsupportedPropertyException("test"))
      .when(ruleHandler).handleMaxChildCapacity();

    converter.convert(config);
  }

  @Test
  public void testReservationSystemNotSupported() throws Exception {
    expectedException.expect(UnsupportedPropertyException.class);
    expectedException.expectMessage("maxCapacity");

    Mockito.doThrow(new UnsupportedPropertyException("maxCapacity"))
      .when(ruleHandler).handleMaxChildCapacity();
    config.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);

    converter.convert(config);
  }

  @Test
  public void testConvertFSConfigurationClusterResource() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .build();
    converter.convert(params);
    assertEquals("Resource", Resource.newInstance(240, 20),
        converter.getClusterResource());
  }

  @Test
  public void testConvertFSConfigPctModeUsedAndClusterResourceDefined()
      throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
            .withClusterResource(CLUSTER_RESOURCE_STRING)
            .build();
    converter.convert(params);
    assertEquals("Resource", Resource.newInstance(240, 20),
        converter.getClusterResource());
  }

  @Test
  public void testConvertFSConfigurationClusterResourceInvalid()
      throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
            .withClusterResource("vcores=20, memory-mb=240G")
            .build();

    expectedException.expect(ConversionException.class);
    expectedException.expectMessage("Error while parsing resource");

    converter.convert(params);
  }

  @Test
  public void testConvertFSConfigurationClusterResourceInvalid2()
      throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource("vcores=20, memmmm=240")
        .build();

    expectedException.expect(ConversionException.class);
    expectedException.expectMessage("Error while parsing resource");

    converter.convert(params);
  }

  @Test
  public void testConvertFSConfigurationRulesFile() throws Exception {
    ruleHandler = new FSConfigToCSConfigRuleHandler(
        createDefaultConversionOptions());
    createConverter();

    FSConfigToCSConfigConverterParams params =
        createDefaultParamsBuilder()
            .withConversionRulesConfig(CONVERSION_RULES_FILE)
            .withClusterResource("vcores=20, memory-mb=2400")
            .build();

    try {
      converter.convert(params);
      fail("Should have thrown UnsupportedPropertyException!");
    } catch (UnsupportedPropertyException e) {
      //need to catch exception so we can check the rules
    }

    ruleHandler = converter.getRuleHandler();
    Map<String, FSConfigToCSConfigRuleHandler.RuleAction> actions =
        ruleHandler.getActions();

    assertEquals("maxCapacityPercentage",
        ABORT, actions.get(MAX_CAPACITY_PERCENTAGE));
    assertEquals("maxChildCapacity",
        ABORT, actions.get(MAX_CHILD_CAPACITY));
    assertEquals("userMaxRunningApps",
        ABORT, actions.get(USER_MAX_RUNNING_APPS));
    assertEquals("userMaxAppsDefault",
        ABORT, actions.get(USER_MAX_APPS_DEFAULT));
    assertEquals("dynamicMaxAssign",
        ABORT, actions.get(DYNAMIC_MAX_ASSIGN));
    assertEquals("specifiedNotFirstRule",
        ABORT, actions.get(SPECIFIED_NOT_FIRST));
    assertEquals("reservationSystem",
        ABORT, actions.get(RESERVATION_SYSTEM));
    assertEquals("queueAutoCreate",
        ABORT, actions.get(QUEUE_AUTO_CREATE));
  }

  @Test
  public void testConvertFSConfigurationWithoutRulesFile() throws Exception {
    ruleHandler = new FSConfigToCSConfigRuleHandler(
        createDefaultConversionOptions());
    createConverter();

    FSConfigToCSConfigConverterParams params =
        createDefaultParamsBuilder()
            .withClusterResource(CLUSTER_RESOURCE_STRING)
            .build();

    converter.convert(params);

    ruleHandler = converter.getRuleHandler();
    Map<String, FSConfigToCSConfigRuleHandler.RuleAction> actions =
        ruleHandler.getActions();

    assertEquals("maxCapacityPercentage",
        WARNING, actions.get(MAX_CAPACITY_PERCENTAGE));
    assertEquals("maxChildCapacity",
        WARNING, actions.get(MAX_CHILD_CAPACITY));
    assertEquals("userMaxRunningApps",
        WARNING, actions.get(USER_MAX_RUNNING_APPS));
    assertEquals("userMaxAppsDefault",
        WARNING, actions.get(USER_MAX_APPS_DEFAULT));
    assertEquals("dynamicMaxAssign",
        WARNING, actions.get(DYNAMIC_MAX_ASSIGN));
    assertEquals("specifiedNotFirstRule",
        WARNING, actions.get(SPECIFIED_NOT_FIRST));
    assertEquals("reservationSystem",
        WARNING, actions.get(RESERVATION_SYSTEM));
    assertEquals("queueAutoCreate",
        WARNING, actions.get(QUEUE_AUTO_CREATE));
  }

  @Test
  public void testConvertFSConfigurationUndefinedYarnSiteConfig()
      throws Exception {
    FSConfigToCSConfigConverterParams params =
        FSConfigToCSConfigConverterParams.Builder.create()
            .withYarnSiteXmlConfig(null)
            .withOutputDirectory(FSConfigConverterTestCommons.OUTPUT_DIR)
            .build();

    expectedException.expect(PreconditionException.class);
    expectedException.expectMessage(
        "yarn-site.xml configuration is not defined");

    converter.convert(params);
  }

  @Test
  public void testConvertCheckOutputDir() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .build();

    converter.convert(params);

    Configuration conf =
        getConvertedCSConfig(FSConfigConverterTestCommons.OUTPUT_DIR);

    File capacityFile = new File(FSConfigConverterTestCommons.OUTPUT_DIR,
        "capacity-scheduler.xml");
    assertTrue("Capacity file exists", capacityFile.exists());
    assertTrue("Capacity file length > 0", capacityFile.length() > 0);
    assertTrue("No. of configuration elements > 0", conf.size() > 0);

    File yarnSiteFile = new File(FSConfigConverterTestCommons.OUTPUT_DIR,
        "yarn-site.xml");
    assertTrue("Yarn site exists", yarnSiteFile.exists());
    assertTrue("Yarn site length > 0", yarnSiteFile.length() > 0);
  }

  @Test
  public void testFairSchedulerXmlIsNotDefinedNeitherDirectlyNorInYarnSiteXml()
      throws Exception {
    FSConfigToCSConfigConverterParams params =
        createParamsBuilder(YARN_SITE_XML_NO_REF_TO_FS_XML)
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .build();

    expectedException.expect(PreconditionException.class);
    expectedException.expectMessage("fair-scheduler.xml is not defined");
    converter.convert(params);
  }

  @Test
  public void testInvalidFairSchedulerXml() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .withFairSchedulerXmlConfig(FAIR_SCHEDULER_XML_INVALID)
        .build();

    expectedException.expect(RuntimeException.class);
    converter.convert(params);
  }

  @Test
  public void testInvalidYarnSiteXml() throws Exception {
    FSConfigToCSConfigConverterParams params =
        createParamsBuilder(YARN_SITE_XML_INVALID)
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .build();

    expectedException.expect(RuntimeException.class);
    converter.convert(params);
  }

  @Test
  public void testConversionWithInvalidPlacementRules() throws Exception {
    config = new Configuration(false);
    config.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        FS_INVALID_PLACEMENT_RULES_XML);
    config.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
    expectedException.expect(ServiceStateException.class);

    converter.convert(config);
  }

  @Test
  public void testConversionWhenInvalidPlacementRulesIgnored()
      throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .withFairSchedulerXmlConfig(FS_INVALID_PLACEMENT_RULES_XML)
        .build();

    ConversionOptions conversionOptions = createDefaultConversionOptions();
    conversionOptions.setNoTerminalRuleCheck(true);

    converter = new FSConfigToCSConfigConverter(ruleHandler,
        conversionOptions);

    converter.convert(params);

    // expected: no exception
  }

  @Test
  public void testConversionWhenOnlyFairPolicyIsUsed() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .withFairSchedulerXmlConfig(FS_ONLY_FAIR_POLICY_XML)
        .build();

    converter.convert(params);

    Configuration convertedConfig = converter.getYarnSiteConfig();

    assertEquals("Resource calculator class shouldn't be set", null,
        convertedConfig.getClass(
            CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS, null));
  }

  @Test
  public void testConversionWhenMixedPolicyIsUsed() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .withFairSchedulerXmlConfig(FS_MIXED_POLICY_XML)
        .build();

    converter.convert(params);

    Configuration convertedConfig = converter.getYarnSiteConfig();

    assertEquals("Resource calculator type", DominantResourceCalculator.class,
        convertedConfig.getClass(
            CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS, null));
  }

  @SuppressWarnings("checkstyle:linelength")
  public void testUserAsDefaultQueueWithPlacementRules() throws Exception {
    config = new Configuration(false);
    config.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
    config.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        FAIR_SCHEDULER_XML);

    converter.convert(config);

    Configuration convertedConf = converter.getCapacitySchedulerConfig();

    String expectedMappingRules =
        "u:%user:root.admins.devs.%user,u:%user:root.users.%user,u:%user:root.default";
    String mappingRules =
        convertedConf.get(CapacitySchedulerConfiguration.QUEUE_MAPPING);
    assertEquals("Mapping rules", expectedMappingRules, mappingRules);
  }

  @Test
  public void testUserAsDefaultQueueTrueWithoutPlacementRules()
      throws Exception {
    testUserAsDefaultQueueWithoutPlacementRules(true);
  }

  @Test
  public void testUserAsDefaultQueueFalseWithoutPlacementRules()
      throws Exception {
    testUserAsDefaultQueueWithoutPlacementRules(false);
  }

  private void testUserAsDefaultQueueWithoutPlacementRules(boolean
      userAsDefaultQueue) throws Exception {
    config = new Configuration(false);
    config.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
    config.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        FS_NO_PLACEMENT_RULES_XML);
    config.setBoolean(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE,
        userAsDefaultQueue);

    converter.setConvertPlacementRules(true);
    converter.convert(config);

    Configuration convertedConf = converter.getCapacitySchedulerConfig();
    String mappingRules =
        convertedConf.get(CapacitySchedulerConfiguration.QUEUE_MAPPING);

    if (userAsDefaultQueue) {
      assertEquals("Mapping rules", "u:%user:%user", mappingRules);
    } else {
      assertEquals("Mapping rules", "u:%user:root.default", mappingRules);
    }
  }

  @Test
  public void testAutoCreateChildQueuesWithPlacementRules() throws Exception {
    config = new Configuration(false);
    config.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
    config.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        FAIR_SCHEDULER_XML);
    config.setBoolean(FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS,
        true);

    converter.convert(config);

    Configuration convertedConf = converter.getCapacitySchedulerConfig();
    String property =
        "yarn.scheduler.capacity.root.auto-create-child-queue.enabled";
    assertNull("Auto-create queue shouldn't be set",
        convertedConf.get(property));
  }

  @Test
  public void testAutoCreateChildQueuesTrueWithoutPlacementRules()
      throws Exception {
    testAutoCreateChildQueuesWithoutPlacementRules(true);
  }

  @Test
  public void testAutoCreateChildQueuesFalseWithoutPlacementRules()
      throws Exception {
    testAutoCreateChildQueuesWithoutPlacementRules(false);
  }

  @SuppressWarnings("checkstyle:linelength")
  private void testAutoCreateChildQueuesWithoutPlacementRules(
      boolean allowUndeclaredPools) throws Exception {
    config = new Configuration(false);
    config.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
    config.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        FS_NO_PLACEMENT_RULES_XML);
    config.setBoolean(FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS,
        allowUndeclaredPools);

    converter.convert(config);

    Configuration convertedConf = converter.getCapacitySchedulerConfig();
    String rootUserAutoCreate =
        "yarn.scheduler.capacity.root.users.auto-create-child-queue.enabled";
    String rootAutoCreate =
        "yarn.scheduler.capacity.root.auto-create-child-queue.enabled";
    String leafQueueAutoCreate =
        "yarn.scheduler.capacity.root.users.joe.auto-create-child-queue.enabled";

    if (allowUndeclaredPools) {
      assertEquals("Auto-create queue wasn't enabled for root.users", true,
          convertedConf.getBoolean(rootUserAutoCreate, false));
      assertNull("Auto-create queue shouldn't be set for root",
          convertedConf.get(rootAutoCreate));
      assertNull("Auto-create queue shouldn't be set for leaf",
          convertedConf.get(leafQueueAutoCreate));
    } else {
      assertNull("Auto-create queue shouldn't be set for root.users",
          convertedConf.get(rootUserAutoCreate));
      assertNull("Auto-create queue shouldn't be set for root",
          convertedConf.get(rootAutoCreate));
      assertNull("Auto-create queue shouldn't be set for leaf",
          convertedConf.get(leafQueueAutoCreate));
    }
  }

  @Test
  public void testPlacementRulesConversionDisabled() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .withFairSchedulerXmlConfig(FAIR_SCHEDULER_XML)
        .withConvertPlacementRules(false)
        .build();

    converter.setPlacementConverter(placementConverter);
    converter.convert(params);

    verifyZeroInteractions(placementConverter);
  }

  @Test
  public void testPlacementRulesConversionEnabled() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .withFairSchedulerXmlConfig(FAIR_SCHEDULER_XML)
        .withConvertPlacementRules(true)
        .build();

    converter.setPlacementConverter(placementConverter);
    converter.convert(params);

    verify(placementConverter).convertPlacementPolicy(
        any(PlacementManager.class),
        any(FSConfigToCSConfigRuleHandler.class),
        any(Boolean.class));
  }

  @Test
  public void testConversionWhenAsyncSchedulingIsEnabled()
          throws Exception {
    boolean schedulingEnabledValue =  testConversionWithAsyncSchedulingOption(true);
    assertTrue("Asynchronous scheduling should be true", schedulingEnabledValue);
  }

  @Test
  public void testConversionWhenAsyncSchedulingIsDisabled() throws Exception {
    boolean schedulingEnabledValue =  testConversionWithAsyncSchedulingOption(false);
    assertEquals("Asynchronous scheduling should be the default value",
            CapacitySchedulerConfiguration.DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE,
            schedulingEnabledValue);
  }

  private boolean testConversionWithAsyncSchedulingOption(boolean enabled) throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
            .withClusterResource(CLUSTER_RESOURCE_STRING)
            .withFairSchedulerXmlConfig(FAIR_SCHEDULER_XML)
            .build();

    ConversionOptions conversionOptions = createDefaultConversionOptions();
    conversionOptions.setEnableAsyncScheduler(enabled);

    converter = new FSConfigToCSConfigConverter(ruleHandler,
            conversionOptions);

    converter.convert(params);

    Configuration convertedConfig = converter.getYarnSiteConfig();

    return convertedConfig.getBoolean(CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE,
            CapacitySchedulerConfiguration.DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE);
  }

  private Configuration getConvertedCSConfig(String dir) throws IOException {
    File capacityFile = new File(dir, "capacity-scheduler.xml");
    ByteArrayInputStream input =
        new ByteArrayInputStream(FileUtils.readFileToByteArray(capacityFile));
    Configuration conf = new Configuration(false);
    conf.addResource(input);

    return conf;
  }
}
