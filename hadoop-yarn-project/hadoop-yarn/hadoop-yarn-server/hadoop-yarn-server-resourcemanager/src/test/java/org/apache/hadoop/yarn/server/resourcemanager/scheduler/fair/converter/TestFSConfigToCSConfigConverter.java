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
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.CHILD_STATIC_DYNAMIC_CONFLICT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.PARENT_CHILD_CREATE_DIFFERS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.FAIR_AS_DRF;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.MAX_RESOURCES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.MIN_RESOURCES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.PARENT_DYNAMIC_CREATE;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.RuleAction.ABORT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.RuleAction.WARNING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
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
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.MappingRulesDescription;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.mockito.junit.jupiter.MockitoExtension;


/**
 * Unit tests for FSConfigToCSConfigConverter.
 *
 */
@ExtendWith(MockitoExtension.class)
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

  @BeforeEach
  public void setup() throws IOException {
    config = new Configuration(false);
    config.set(FairSchedulerConfiguration.ALLOCATION_FILE, FAIR_SCHEDULER_XML);
    config.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
    config.setBoolean(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, true);
    createConverter();
    converterTestCommons = new FSConfigConverterTestCommons();
    converterTestCommons.setUp();
  }

  @AfterEach
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
  void testDefaultMaxAMShare() throws Exception {
    converter.convert(config);

    Configuration conf = converter.getCapacitySchedulerConfig();
    String maxAmShare =
        conf.get(CapacitySchedulerConfiguration.
            MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT);

    assertEquals("0.16", maxAmShare, "Default max AM share");

    assertEquals("0.15", conf.get(PREFIX + "root.admins.alice.maximum-am-resource-percent"),
        "root.admins.alice max-am-resource-percent");

    assertNull(conf.get(PREFIX + "root.users.joe maximum-am-resource-percent"),
        "root.users.joe maximum-am-resource-percent should be null");
  }

  @Test
  void testDefaultMaxAMShareDisabled() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .withFairSchedulerXmlConfig(FS_MAX_AM_SHARE_DISABLED_XML)
        .build();

    converter.convert(params);

    Configuration conf = converter.getCapacitySchedulerConfig();

    // -1.0 means disabled ==> 1.0 in CS
    assertEquals("1.0", conf.get(CapacitySchedulerConfiguration.
            MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT),
        "Default max-am-resource-percent");

    // root.admins.bob -1.0 equals to the default -1.0
    assertNull(conf.get(PREFIX + "root.admins.bob.maximum-am-resource-percent"),
        "root.admins.bob maximum-am-resource-percent should be null");

    // root.admins.alice 0.15 != -1.0
    assertEquals("0.15", conf.get(PREFIX + "root.admins.alice.maximum-am-resource-percent"),
        "root.admins.alice max-am-resource-percent");

    // root.users.joe is unset, inherits -1.0
    assertNull(conf.get(PREFIX + "root.users.joe.maximum-am-resource-percent"),
        "root.users.joe maximum-am-resource-percent should be null");
  }

  @Test
  void testConvertACLs() throws Exception {
    converter.convert(config);

    Configuration conf = converter.getCapacitySchedulerConfig();

    // root
    assertEquals(
        "alice,bob,joe,john hadoop_users",
        conf.get(PREFIX + "root.acl_submit_applications"),
        "root submit ACL"
    );
    assertEquals(
        "alice,bob,joe,john hadoop_users",
        conf.get(PREFIX + "root.acl_administer_queue"),
        "root admin ACL"
    );

    // root.admins.bob
    assertEquals("bob ", conf.get(PREFIX + "root.admins.bob.acl_submit_applications"),
        "root.admins.bob submit ACL");
    assertEquals("bob ", conf.get(PREFIX + "root.admins.bob.acl_administer_queue"),
        "root.admins.bob admin ACL");

    // root.admins.alice
    assertEquals("alice ", conf.get(PREFIX + "root.admins.alice.acl_submit_applications"),
        "root.admins.alice submit ACL");
    assertEquals("alice ", conf.get(PREFIX + "root.admins.alice.acl_administer_queue"),
        "root.admins.alice admin ACL");

    // root.users.john
    assertEquals("john ", conf.get(PREFIX + "root.users.john.acl_submit_applications"),
        "root.users.john submit ACL");
    assertEquals("john ", conf.get(PREFIX + "root.users.john.acl_administer_queue"),
        "root.users.john admin ACL");

    // root.users.joe
    assertEquals("joe ", conf.get(PREFIX + "root.users.joe.acl_submit_applications"),
        "root.users.joe submit ACL");
    assertEquals("joe ", conf.get(PREFIX + "root.users.joe.acl_administer_queue"),
        "root.users.joe admin ACL");
  }

  @Test
  void testDefaultQueueMaxParallelApps() throws Exception {
    converter.convert(config);

    Configuration conf = converter.getCapacitySchedulerConfig();

    assertEquals(15, conf.getInt(PREFIX + "max-parallel-apps", -1),
        "Default max parallel apps");
  }

  @Test
  void testSpecificQueueMaxParallelApps() throws Exception {
    converter.convert(config);

    Configuration conf = converter.getCapacitySchedulerConfig();

    assertEquals(2, conf.getInt(PREFIX + "root.admins.alice.max-parallel-apps", -1),
        "root.admins.alice max parallel apps");
  }

  @Test
  void testDefaultUserMaxParallelApps() throws Exception {
    converter.convert(config);

    Configuration conf = converter.getCapacitySchedulerConfig();
    int userMaxParallelApps =
        conf.getInt(
            PREFIX + "user.max-parallel-apps", -1);

    assertEquals(10, userMaxParallelApps,
        "Default user max parallel apps");
  }

  @Test
  void testSpecificUserMaxParallelApps() throws Exception {
    converter.convert(config);

    Configuration conf = converter.getCapacitySchedulerConfig();

    assertEquals(30, conf.getInt(PREFIX + "user.alice.max-parallel-apps", -1),
        "Max parallel apps for alice");
    assertNull(conf.get(PREFIX + "user.bob.max-parallel-apps"),
        "Max parallel apps should be undefined for user bob");
    assertNull(conf.get(PREFIX + "user.joe.max-parallel-apps"),
        "Max parallel apps should be undefined for user joe");
    assertNull(conf.get(PREFIX + "user.john.max-parallel-apps"),
        "Max parallel apps should be undefined for user john");
  }

  @Test
  void testQueueMaxChildCapacityNotSupported() throws Exception {

    Mockito.doThrow(new UnsupportedPropertyException("test"))
      .when(ruleHandler).handleMaxChildCapacity();
    final Throwable thrown = Assertions.assertThrows(
        UnsupportedPropertyException.class,
        () -> converter.convert(config)
    );
    Assertions.assertEquals(thrown.getMessage(), "test");
  }

  @Test
  void testReservationSystemNotSupported() throws Exception {

    Mockito.doThrow(new UnsupportedPropertyException("maxCapacity"))
      .when(ruleHandler).handleMaxChildCapacity();
    config.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
    final Throwable thrown = Assertions.assertThrows(
        UnsupportedPropertyException.class,
        () -> converter.convert(config)
    );
    Assertions.assertEquals(thrown.getMessage(), "maxCapacity");
  }

  @Test
  void testConvertFSConfigurationClusterResource() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .build();
    converter.convert(params);
    assertEquals(
        Resource.newInstance(240, 20),
        converter.getClusterResource(),
        "Resource"
    );
  }

  @Test
  void testConvertFSConfigPctModeUsedAndClusterResourceDefined()
      throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
            .withClusterResource(CLUSTER_RESOURCE_STRING)
            .build();
    converter.convert(params);
    assertEquals(Resource.newInstance(240, 20),
        converter.getClusterResource(), "Resource");
  }

  @Test
  void testConvertFSConfigurationClusterResourceInvalid()
      throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
            .withClusterResource("vcores=20, memory-mb=240G")
            .build();

    final Throwable thrown = Assertions.assertThrows(
        ConversionException.class,
        () -> converter.convert(params)
    );
    Assertions.assertEquals(
        thrown.getMessage(),
        "Error while parsing resource."
    );
  }

  @Test
  void testConvertFSConfigurationClusterResourceInvalid2()
      throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource("vcores=20, memmmm=240")
        .build();

    final Throwable thrown = Assertions.assertThrows(
        ConversionException.class,
        () -> converter.convert(params)
    );
    Assertions.assertEquals(
        thrown.getMessage(),
        "Error while parsing resource."
    );
  }

  @Test
  void testConvertFSConfigurationRulesFile() throws Exception {
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

    assertEquals(ABORT,
        actions.get(MAX_CAPACITY_PERCENTAGE), "maxCapacityPercentage");
    assertEquals(ABORT,
        actions.get(MAX_CHILD_CAPACITY), "maxChildCapacity");
    assertEquals(ABORT,
        actions.get(DYNAMIC_MAX_ASSIGN), "dynamicMaxAssign");
    assertEquals(ABORT,
        actions.get(RESERVATION_SYSTEM), "reservationSystem");
    assertEquals(ABORT,
        actions.get(QUEUE_AUTO_CREATE), "queueAutoCreate");
  }

  @Test
  void testConvertFSConfigurationWithoutRulesFile() throws Exception {
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

    assertEquals(WARNING,
        actions.get(MAX_CAPACITY_PERCENTAGE), "maxCapacityPercentage");
    assertEquals(WARNING,
        actions.get(MAX_CHILD_CAPACITY), "maxChildCapacity");
    assertEquals(WARNING,
        actions.get(DYNAMIC_MAX_ASSIGN), "dynamicMaxAssign");
    assertEquals(WARNING,
        actions.get(RESERVATION_SYSTEM), "reservationSystem");
    assertEquals(WARNING,
        actions.get(QUEUE_AUTO_CREATE), "queueAutoCreate");
    assertEquals(WARNING,
        actions.get(CHILD_STATIC_DYNAMIC_CONFLICT), "childStaticDynamicConflict");
    assertEquals(WARNING,
        actions.get(PARENT_CHILD_CREATE_DIFFERS), "parentChildCreateDiffers");
    assertEquals(WARNING,
        actions.get(FAIR_AS_DRF), "fairAsDrf");
    assertEquals(WARNING,
        actions.get(MAX_RESOURCES), "maxResources");
    assertEquals(WARNING,
        actions.get(MIN_RESOURCES), "minResources");
    assertEquals(WARNING,
        actions.get(PARENT_DYNAMIC_CREATE), "parentDynamicCreate");
  }

  @Test
  void testConvertFSConfigurationUndefinedYarnSiteConfig()
      throws Exception {
    FSConfigToCSConfigConverterParams params =
        FSConfigToCSConfigConverterParams.Builder.create()
            .withYarnSiteXmlConfig(null)
            .withOutputDirectory(FSConfigConverterTestCommons.OUTPUT_DIR)
            .build();
    final Throwable thrown = Assertions.assertThrows(
        PreconditionException.class,
        () -> converter.convert(params)
    );
    Assertions.assertTrue(
        thrown.getMessage().contains(
            "yarn-site.xml configuration is not defined"
        )
    );
  }

  @Test
  void testConvertCheckOutputDir() throws Exception {
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
    assertTrue(capacityFile.exists(), "Capacity file exists");
    assertTrue(capacityFile.length() > 0, "Capacity file length > 0");
    assertTrue(conf.size() > 0, "No. of configuration elements > 0");

    File yarnSiteFile = new File(FSConfigConverterTestCommons.OUTPUT_DIR,
        "yarn-site.xml");
    assertTrue(yarnSiteFile.exists(), "Yarn site exists");
    assertTrue(yarnSiteFile.length() > 0, "Yarn site length > 0");

    File mappingRulesFile = new File(FSConfigConverterTestCommons.OUTPUT_DIR,
        "mapping-rules.json");
    assertTrue(mappingRulesFile.exists(), "Mapping rules file exists");
    assertTrue(mappingRulesFile.length() > 0, "Mapping rules file length > 0");
  }

  @Test
  void testFairSchedulerXmlIsNotDefinedNeitherDirectlyNorInYarnSiteXml()
      throws Exception {
    FSConfigToCSConfigConverterParams params =
        createParamsBuilder(YARN_SITE_XML_NO_REF_TO_FS_XML)
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .build();

    final Exception ex = Assertions.assertThrows(
        PreconditionException.class,
        () -> converter.convert(params)
    );
    Assertions.assertTrue(ex.getMessage().contains("fair-scheduler.xml is not defined"));
  }

  @Test
  void testInvalidFairSchedulerXml() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .withFairSchedulerXmlConfig(FAIR_SCHEDULER_XML_INVALID)
        .build();
    Assertions.assertThrows(
        RuntimeException.class,
        () -> converter.convert(params)
    );
  }

  @Test
  void testInvalidYarnSiteXml() throws Exception {
    FSConfigToCSConfigConverterParams params =
        createParamsBuilder(YARN_SITE_XML_INVALID)
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .build();
    Assertions.assertThrows(
        RuntimeException.class,
        () -> converter.convert(params)
    );
  }

  @Test
  void testConversionWithInvalidPlacementRules() throws Exception {
    config = new Configuration(false);
    config.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        FS_INVALID_PLACEMENT_RULES_XML);
    config.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
    Assertions.assertThrows(
        ServiceStateException.class,
        () -> converter.convert(config)
    );
  }

  @Test
  void testConversionWhenInvalidPlacementRulesIgnored()
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
  void testConversionWhenOnlyFairPolicyIsUsed() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .withFairSchedulerXmlConfig(FS_ONLY_FAIR_POLICY_XML)
        .build();

    converter.convert(params);

    Configuration convertedConfig = converter.getYarnSiteConfig();

    assertEquals(null, convertedConfig.getClass(
            CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS, null),
        "Resource calculator class shouldn't be set");
  }

  @Test
  void testConversionWhenMixedPolicyIsUsed() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .withFairSchedulerXmlConfig(FS_MIXED_POLICY_XML)
        .build();

    converter.convert(params);

    Configuration convertedConfig = converter.getYarnSiteConfig();

    assertEquals(DominantResourceCalculator.class, convertedConfig.getClass(
            CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS, null),
        "Resource calculator type");
  }

  @Test
  void testUserAsDefaultQueueWithPlacementRules()
      throws Exception {
    testUserAsDefaultQueueAndPlacementRules(true);
  }

  @Test
  void testUserAsDefaultQueueWithoutPlacementRules()
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
      assertEquals(5, description.getRules().size(), "Number of rules");
    } else {
      // by default, FS internally creates 2 rules
      assertEquals(2, description.getRules().size(), "Number of rules");
    }
  }

  @Test
  void testPlacementRulesConversionDisabled() throws Exception {
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
  void testPlacementRulesConversionEnabled() throws Exception {
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
  void testConversionWhenAsyncSchedulingIsEnabled()
          throws Exception {
    boolean schedulingEnabledValue =  testConversionWithAsyncSchedulingOption(true);
    assertTrue(schedulingEnabledValue, "Asynchronous scheduling should be true");
  }

  @Test
  void testConversionWhenAsyncSchedulingIsDisabled() throws Exception {
    boolean schedulingEnabledValue =  testConversionWithAsyncSchedulingOption(false);
    assertEquals(CapacitySchedulerConfiguration.DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE,
            schedulingEnabledValue,
            "Asynchronous scheduling should be the default value");
  }

  @Test
  void testSiteDisabledPreemptionWithObserveOnlyConversion()
      throws Exception{
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withDisablePreemption(FSConfigToCSConfigConverterParams.
            PreemptionMode.OBSERVE_ONLY)
        .build();

    converter.convert(params);
    assertTrue(converter.getCapacitySchedulerConfig().
            getBoolean(CapacitySchedulerConfiguration.
                PREEMPTION_OBSERVE_ONLY, false),
        "The observe only should be true");
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
