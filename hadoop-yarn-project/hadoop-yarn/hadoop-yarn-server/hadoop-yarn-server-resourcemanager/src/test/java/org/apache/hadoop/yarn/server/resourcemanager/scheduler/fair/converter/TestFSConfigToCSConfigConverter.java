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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.DYNAMIC_MAX_ASSIGN;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.MAX_CAPACITY_PERCENTAGE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.MAX_CHILD_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.QUEUE_AUTO_CREATE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.RESERVATION_SYSTEM;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.CHILD_STATIC_DYNAMIC_CONFLICT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.PARENT_CHILD_CREATE_DIFFERS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.FAIR_AS_DRF;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.MAX_RESOURCES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.MIN_RESOURCES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.PARENT_DYNAMIC_CREATE;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.RuleAction.ABORT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.RuleAction.WARNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.MappingRulesDescription;
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

import com.fasterxml.jackson.databind.ObjectMapper;


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
    config.setBoolean(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, true);
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
    converter.setConvertPlacementRules(false);
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

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();
    Float maxAmShare =
        conf.getMaximumApplicationMasterResourcePercent();

    assertEquals("Default max AM share", 0.16f, maxAmShare, 0.0f);

    assertEquals("root.admins.alice max-am-resource-percent", 0.15f,
        conf.getMaximumApplicationMasterResourcePerQueuePercent("root.admins.alice"),
            0.0f);

    //root.users.joe don’t have maximum-am-resource-percent set
    // so falling back to the global value
    assertEquals("root.users.joe maximum-am-resource-percent", 0.16f,
        conf.getMaximumApplicationMasterResourcePerQueuePercent("root.users.joe"),
            0.0f);
  }

  @Test
  public void testDefaultUserLimitFactor() throws Exception {
    converter.convert(config);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();

    assertEquals("root.users user-limit-factor", 1.0f,
            conf.getUserLimitFactor("root.users"), 0.0f);
    assertEquals("root.users auto-queue-creation-v2.enabled", true,
            conf.isAutoQueueCreationV2Enabled("root.users"));

    assertEquals("root.default user-limit-factor", -1.0f,
            conf.getUserLimitFactor("root.default"), 0.0f);

    assertEquals("root.users.joe user-limit-factor", -1.0f,
            conf.getUserLimitFactor("root.users.joe"), 0.0f);

    assertEquals("root.admins.bob user-limit-factor", -1.0f,
            conf.getUserLimitFactor("root.admins.bob"), 0.0f);
    assertEquals("root.admin.bob auto-queue-creation-v2.enabled", false,
            conf.isAutoQueueCreationV2Enabled("root.admin.bob"));
  }

  @Test
  public void testDefaultMaxAMShareDisabled() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .withFairSchedulerXmlConfig(FS_MAX_AM_SHARE_DISABLED_XML)
        .build();

    converter.convert(params);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();

    // -1.0 means disabled ==> 1.0 in CS
    assertEquals("Default max-am-resource-percent", 1.0f,
        conf.getMaximumApplicationMasterResourcePercent(), 0.0f);

    // root.admins.bob is unset,so falling back to the global value
    assertEquals("root.admins.bob maximum-am-resource-percent", 1.0f,
        conf.getMaximumApplicationMasterResourcePerQueuePercent("root.admins.bob"), 0.0f);

    // root.admins.alice 0.15 != -1.0
    assertEquals("root.admins.alice max-am-resource-percent", 0.15f,
        conf.getMaximumApplicationMasterResourcePerQueuePercent("root.admins.alice"), 0.0f);

    // root.users.joe is unset,so falling back to the global value
    assertEquals("root.users.joe maximum-am-resource-percent", 1.0f,
        conf.getMaximumApplicationMasterResourcePerQueuePercent("root.users.joe"), 0.0f);
  }

  @Test
  public void testConvertACLs() throws Exception {
    converter.convert(config);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();

    // root
    assertEquals("root submit ACL", "alice,bob,joe,john hadoop_users",
        conf.getAcl("root", QueueACL.SUBMIT_APPLICATIONS).getAclString());
    assertEquals("root admin ACL", "alice,bob,joe,john hadoop_users",
        conf.getAcl("root", QueueACL.ADMINISTER_QUEUE).getAclString());

    // root.admins.bob
    assertEquals("root.admins.bob submit ACL", "bob ",
        conf.getAcl("root.admins.bob", QueueACL.SUBMIT_APPLICATIONS).getAclString());
    assertEquals("root.admins.bob admin ACL", "bob ",
        conf.getAcl("root.admins.bob", QueueACL.ADMINISTER_QUEUE).getAclString());

    // root.admins.alice
    assertEquals("root.admins.alice submit ACL", "alice ",
        conf.getAcl("root.admins.alice", QueueACL.SUBMIT_APPLICATIONS).getAclString());
    assertEquals("root.admins.alice admin ACL", "alice ",
        conf.getAcl("root.admins.alice", QueueACL.ADMINISTER_QUEUE).getAclString());

    // root.users.john
    assertEquals("root.users.john submit ACL", "*",
        conf.getAcl("root.users.john", QueueACL.SUBMIT_APPLICATIONS).getAclString());
    assertEquals("root.users.john admin ACL", "*",
        conf.getAcl("root.users.john", QueueACL.ADMINISTER_QUEUE).getAclString());

    // root.users.joe
    assertEquals("root.users.joe submit ACL", "joe ",
        conf.getAcl("root.users.joe", QueueACL.SUBMIT_APPLICATIONS).getAclString());
    assertEquals("root.users.joe admin ACL", "joe ",
        conf.getAcl("root.users.joe", QueueACL.ADMINISTER_QUEUE).getAclString());
  }

  @Test
  public void testDefaultQueueMaxParallelApps() throws Exception {
    converter.convert(config);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();

    assertEquals("Default max parallel apps", 15,
        conf.getDefaultMaxParallelApps(), 0);
  }

  @Test
  public void testSpecificQueueMaxParallelApps() throws Exception {
    converter.convert(config);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();

    assertEquals("root.admins.alice max parallel apps", 2,
        conf.getMaxParallelAppsForQueue("root.admins.alice"), 0);
  }

  @Test
  public void testDefaultUserMaxParallelApps() throws Exception {
    converter.convert(config);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();

    assertEquals("Default user max parallel apps", 10,
        conf.getDefaultMaxParallelAppsPerUser(), 0);
  }

  @Test
  public void testSpecificUserMaxParallelApps() throws Exception {
    converter.convert(config);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();

    assertEquals("Max parallel apps for alice", 30,
        conf.getMaxParallelAppsForUser("alice"), 0);

    //users.bob, user.joe, user.john  don’t have max-parallel-app set
    // so falling back to the global value for .user to 10
    assertEquals("Max parallel apps for user bob", 10,
        conf.getMaxParallelAppsForUser("bob"), 0);
    assertEquals("Max parallel apps for user joe", 10,
        conf.getMaxParallelAppsForUser("joe"), 0);
    assertEquals("Max parallel apps for user john", 10,
        conf.getMaxParallelAppsForUser("john"), 0);
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
    assertEquals("dynamicMaxAssign",
        ABORT, actions.get(DYNAMIC_MAX_ASSIGN));
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
    assertEquals("dynamicMaxAssign",
        WARNING, actions.get(DYNAMIC_MAX_ASSIGN));
    assertEquals("reservationSystem",
        WARNING, actions.get(RESERVATION_SYSTEM));
    assertEquals("queueAutoCreate",
        WARNING, actions.get(QUEUE_AUTO_CREATE));
    assertEquals("childStaticDynamicConflict",
        WARNING, actions.get(CHILD_STATIC_DYNAMIC_CONFLICT));
    assertEquals("parentChildCreateDiffers",
        WARNING, actions.get(PARENT_CHILD_CREATE_DIFFERS));
    assertEquals("fairAsDrf",
        WARNING, actions.get(FAIR_AS_DRF));
    assertEquals("maxResources",
        WARNING, actions.get(MAX_RESOURCES));
    assertEquals("minResources",
        WARNING, actions.get(MIN_RESOURCES));
    assertEquals("parentDynamicCreate",
        WARNING, actions.get(PARENT_DYNAMIC_CREATE));
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
        .withConvertPlacementRules(true)
        .withPlacementRulesToFile(true)
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

    File mappingRulesFile = new File(FSConfigConverterTestCommons.OUTPUT_DIR,
        "mapping-rules.json");
    assertTrue("Mapping rules file exists", mappingRulesFile.exists());
    assertTrue("Mapping rules file length > 0", mappingRulesFile.length() > 0);
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

  @Test
  public void testUserAsDefaultQueueWithPlacementRules()
      throws Exception {
    testUserAsDefaultQueueAndPlacementRules(true);
  }

  @Test
  public void testUserAsDefaultQueueWithoutPlacementRules()
      throws Exception {
    testUserAsDefaultQueueAndPlacementRules(false);
  }

  private void testUserAsDefaultQueueAndPlacementRules(
      boolean hasPlacementRules) throws Exception {
    config = new Configuration(false);
    config.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);

    if (hasPlacementRules) {
      config.set(FairSchedulerConfiguration.ALLOCATION_FILE,
          FAIR_SCHEDULER_XML);
    } else {
      config.set(FairSchedulerConfiguration.ALLOCATION_FILE,
          FS_NO_PLACEMENT_RULES_XML);
    }

    config.setBoolean(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE,
        true);

    converter.setConvertPlacementRules(true);
    converter.setConsoleMode(true);
    converter.convert(config);
    String json = converter.getCapacitySchedulerConfig()
        .get(CapacitySchedulerConfiguration.MAPPING_RULE_JSON);

    MappingRulesDescription description =
        new ObjectMapper()
          .reader()
          .forType(MappingRulesDescription.class)
          .readValue(json);

    if (hasPlacementRules) {
      // fs.xml defines 5 rules
      assertEquals("Number of rules", 5, description.getRules().size());
    } else {
      // by default, FS internally creates 2 rules
      assertEquals("Number of rules", 2, description.getRules().size());
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
        any(CapacitySchedulerConfiguration.class),
        anyBoolean());
    assertTrue(converter.getCapacitySchedulerConfig().getBoolean(
        CapacitySchedulerConfiguration.ENABLE_QUEUE_MAPPING_OVERRIDE, false));
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

  @Test
  public void testSiteDisabledPreemptionWithObserveOnlyConversion()
      throws Exception{
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withDisablePreemption(FSConfigToCSConfigConverterParams.
            PreemptionMode.OBSERVE_ONLY)
        .build();

    converter.convert(params);
    assertTrue("The observe only should be true",
        converter.getCapacitySchedulerConfig().
            getPreemptionObserveOnly());
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
