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

import static org.apache.hadoop.test.MockitoUtil.verifyZeroInteractions;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.MappingRulesDescription;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;


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

  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath DEFAULT = new QueuePath("root.default");
  private static final QueuePath USERS = new QueuePath("root.users");
  private static final QueuePath USERS_JOE = new QueuePath("root.users.joe");
  private static final QueuePath USERS_JOHN = new QueuePath("root.users.john");
  private static final QueuePath ADMINS_ALICE = new QueuePath("root.admins.alice");
  private static final QueuePath ADMINS_BOB = new QueuePath("root.admins.bob");

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
  public void testDefaultMaxAMShare() throws Exception {
    converter.convert(config);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();
    Float maxAmShare =
        conf.getMaximumApplicationMasterResourcePercent();

    assertEquals(0.16f, maxAmShare, 0.0f, "Default max AM share");

    assertEquals(0.15f,
        conf.getMaximumApplicationMasterResourcePerQueuePercent(ADMINS_ALICE),
        0.0f, "root.admins.alice max-am-resource-percent");

    //root.users.joe don’t have maximum-am-resource-percent set
    // so falling back to the global value
    assertEquals(0.16f,
        conf.getMaximumApplicationMasterResourcePerQueuePercent(USERS_JOE),
        0.0f, "root.users.joe maximum-am-resource-percent");
  }

  @Test
  public void testDefaultUserLimitFactor() throws Exception {
    converter.convert(config);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();

    assertEquals(1.0f, conf.getUserLimitFactor(USERS), 0.0f,
        "root.users user-limit-factor");
    assertEquals(true, conf.isAutoQueueCreationV2Enabled(USERS),
        "root.users auto-queue-creation-v2.enabled");

    assertEquals(-1.0f, conf.getUserLimitFactor(DEFAULT), 0.0f,
        "root.default user-limit-factor");

    assertEquals(-1.0f, conf.getUserLimitFactor(USERS_JOE), 0.0f,
        "root.users.joe user-limit-factor");

    assertEquals(-1.0f, conf.getUserLimitFactor(ADMINS_BOB), 0.0f,
        "root.admins.bob user-limit-factor");
    assertEquals(false, conf.isAutoQueueCreationV2Enabled(ADMINS_BOB),
        "root.admin.bob auto-queue-creation-v2.enabled");
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
    assertEquals(1.0f, conf.getMaximumApplicationMasterResourcePercent(), 0.0f,
        "Default max-am-resource-percent");

    // root.admins.bob is unset,so falling back to the global value
    assertEquals(1.0f, conf.getMaximumApplicationMasterResourcePerQueuePercent(ADMINS_BOB), 0.0f,
        "root.admins.bob maximum-am-resource-percent");

    // root.admins.alice 0.15 != -1.0
    assertEquals(0.15f, conf.getMaximumApplicationMasterResourcePerQueuePercent(ADMINS_ALICE), 0.0f,
        "root.admins.alice max-am-resource-percent");

    // root.users.joe is unset,so falling back to the global value
    assertEquals(1.0f, conf.getMaximumApplicationMasterResourcePerQueuePercent(USERS_JOE), 0.0f,
        "root.users.joe maximum-am-resource-percent");
  }

  @Test
  public void testConvertACLs() throws Exception {
    converter.convert(config);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();

    // root
    assertEquals("alice,bob,joe,john hadoop_users",
        conf.getAcl(ROOT, QueueACL.SUBMIT_APPLICATIONS).getAclString(), "root submit ACL");
    assertEquals("alice,bob,joe,john hadoop_users",
        conf.getAcl(ROOT, QueueACL.ADMINISTER_QUEUE).getAclString(), "root admin ACL");

    // root.admins.bob
    assertEquals("bob ", conf.getAcl(ADMINS_BOB, QueueACL.SUBMIT_APPLICATIONS).getAclString(),
        "root.admins.bob submit ACL");
    assertEquals("bob ", conf.getAcl(ADMINS_BOB, QueueACL.ADMINISTER_QUEUE).getAclString(),
        "root.admins.bob admin ACL");

    // root.admins.alice
    assertEquals("alice ", conf.getAcl(ADMINS_ALICE, QueueACL.SUBMIT_APPLICATIONS).getAclString(),
        "root.admins.alice submit ACL");
    assertEquals("alice ", conf.getAcl(ADMINS_ALICE, QueueACL.ADMINISTER_QUEUE).getAclString(),
        "root.admins.alice admin ACL");

    // root.users.john
    assertEquals("*", conf.getAcl(USERS_JOHN, QueueACL.SUBMIT_APPLICATIONS).getAclString(),
        "root.users.john submit ACL");
    assertEquals("*", conf.getAcl(USERS_JOHN, QueueACL.ADMINISTER_QUEUE).getAclString(),
        "root.users.john admin ACL");

    // root.users.joe
    assertEquals("joe ", conf.getAcl(USERS_JOE, QueueACL.SUBMIT_APPLICATIONS).getAclString(),
        "root.users.joe submit ACL");
    assertEquals("joe ", conf.getAcl(USERS_JOE, QueueACL.ADMINISTER_QUEUE).getAclString(),
        "root.users.joe admin ACL");
  }

  @Test
  public void testDefaultQueueMaxParallelApps() throws Exception {
    converter.convert(config);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();

    assertEquals(15, conf.getDefaultMaxParallelApps(), 0, "Default max parallel apps");
  }

  @Test
  public void testSpecificQueueMaxParallelApps() throws Exception {
    converter.convert(config);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();

    assertEquals(2, conf.getMaxParallelAppsForQueue(ADMINS_ALICE), 0,
        "root.admins.alice max parallel apps");
  }

  @Test
  public void testDefaultUserMaxParallelApps() throws Exception {
    converter.convert(config);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();

    assertEquals(10, conf.getDefaultMaxParallelAppsPerUser(), 0,
        "Default user max parallel apps");
  }

  @Test
  public void testSpecificUserMaxParallelApps() throws Exception {
    converter.convert(config);

    CapacitySchedulerConfiguration conf = converter.getCapacitySchedulerConfig();

    assertEquals(30, conf.getMaxParallelAppsForUser("alice"), 0,
        "Max parallel apps for alice");

    //users.bob, user.joe, user.john  don’t have max-parallel-app set
    // so falling back to the global value for .user to 10
    assertEquals(10, conf.getMaxParallelAppsForUser("bob"), 0,
        "Max parallel apps for user bob");
    assertEquals(10, conf.getMaxParallelAppsForUser("joe"), 0,
        "Max parallel apps for user joe");
    assertEquals(10, conf.getMaxParallelAppsForUser("john"), 0,
        "Max parallel apps for user john");
  }

  @Test
  public void testQueueMaxChildCapacityNotSupported() throws Exception {
    UnsupportedPropertyException exception =
        assertThrows(UnsupportedPropertyException.class, () -> {
          doThrow(new UnsupportedPropertyException("test"))
              .when(ruleHandler).handleMaxChildCapacity();
          converter.convert(config);
        });
    assertThat(exception.getMessage()).contains("test");
  }

  @Test
  public void testReservationSystemNotSupported() throws Exception {
    UnsupportedPropertyException exception =
        assertThrows(UnsupportedPropertyException.class, () -> {
          doThrow(new UnsupportedPropertyException("maxCapacity"))
              .when(ruleHandler).handleMaxChildCapacity();
          config.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
          converter.convert(config);
        });
    assertThat(exception.getMessage()).contains("maxCapacity");
  }

  @Test
  public void testConvertFSConfigurationClusterResource() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .build();
    converter.convert(params);
    assertEquals(Resource.newInstance(240, 20),
        converter.getClusterResource(), "Resource");
  }

  @Test
  public void testConvertFSConfigPctModeUsedAndClusterResourceDefined()
      throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
            .withClusterResource(CLUSTER_RESOURCE_STRING)
            .build();
    converter.convert(params);
    assertEquals(Resource.newInstance(240, 20),
        converter.getClusterResource(), "Resource");
  }

  @Test
  public void testConvertFSConfigurationClusterResourceInvalid()
      throws Exception {
    ConversionException exception =
        assertThrows(ConversionException.class, () -> {
          FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
              .withClusterResource("vcores=20, memory-mb=240G")
              .build();
          converter.convert(params);
        });
    assertThat(exception.getMessage()).contains("Error while parsing resource");
  }

  @Test
  public void testConvertFSConfigurationClusterResourceInvalid2()
      throws Exception {
    ConversionException exception =
        assertThrows(ConversionException.class, () -> {
          FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
              .withClusterResource("vcores=20, memmmm=240")
              .build();
          converter.convert(params);
        });
    assertThat(exception.getMessage()).contains("Error while parsing resource");
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

    assertEquals(ABORT, actions.get(MAX_CAPACITY_PERCENTAGE), "maxCapacityPercentage");
    assertEquals(ABORT, actions.get(MAX_CHILD_CAPACITY), "maxChildCapacity");
    assertEquals(ABORT, actions.get(DYNAMIC_MAX_ASSIGN), "dynamicMaxAssign");
    assertEquals(ABORT, actions.get(RESERVATION_SYSTEM), "reservationSystem");
    assertEquals(ABORT, actions.get(QUEUE_AUTO_CREATE), "queueAutoCreate");
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

    assertEquals(WARNING, actions.get(MAX_CAPACITY_PERCENTAGE), "maxCapacityPercentage");
    assertEquals(WARNING, actions.get(MAX_CHILD_CAPACITY), "maxChildCapacity");
    assertEquals(WARNING, actions.get(DYNAMIC_MAX_ASSIGN), "dynamicMaxAssign");
    assertEquals(WARNING, actions.get(RESERVATION_SYSTEM), "reservationSystem");
    assertEquals(WARNING, actions.get(QUEUE_AUTO_CREATE), "queueAutoCreate");
    assertEquals(WARNING, actions.get(CHILD_STATIC_DYNAMIC_CONFLICT),
        "childStaticDynamicConflict");
    assertEquals(WARNING, actions.get(PARENT_CHILD_CREATE_DIFFERS), "parentChildCreateDiffers");
    assertEquals(WARNING, actions.get(FAIR_AS_DRF), "fairAsDrf");
    assertEquals(WARNING, actions.get(MAX_RESOURCES), "maxResources");
    assertEquals(WARNING, actions.get(MIN_RESOURCES), "minResources");
    assertEquals(WARNING, actions.get(PARENT_DYNAMIC_CREATE), "parentDynamicCreate");
  }

  @Test
  public void testConvertFSConfigurationUndefinedYarnSiteConfig()
      throws Exception {
    PreconditionException exception =
        assertThrows(PreconditionException.class, () -> {
          FSConfigToCSConfigConverterParams params =
              FSConfigToCSConfigConverterParams.Builder.create()
              .withYarnSiteXmlConfig(null)
              .withOutputDirectory(FSConfigConverterTestCommons.OUTPUT_DIR)
              .build();
          converter.convert(params);
        });
    assertThat(exception.getMessage()).contains("yarn-site.xml configuration is not defined");
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
  public void testFairSchedulerXmlIsNotDefinedNeitherDirectlyNorInYarnSiteXml()
      throws Exception {
    PreconditionException exception =
        assertThrows(PreconditionException.class, () -> {
          FSConfigToCSConfigConverterParams params =
              createParamsBuilder(YARN_SITE_XML_NO_REF_TO_FS_XML)
              .withClusterResource(CLUSTER_RESOURCE_STRING)
              .build();
          converter.convert(params);
        });
    assertThat(exception.getMessage()).contains("fair-scheduler.xml is not defined");
  }

  @Test
  public void testInvalidFairSchedulerXml() throws Exception {
    assertThrows(RuntimeException.class, () -> {
      FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
          .withClusterResource(CLUSTER_RESOURCE_STRING)
          .withFairSchedulerXmlConfig(FAIR_SCHEDULER_XML_INVALID)
          .build();
      converter.convert(params);
    });
  }

  @Test
  public void testInvalidYarnSiteXml() throws Exception {
    assertThrows(RuntimeException.class, () -> {
      FSConfigToCSConfigConverterParams params =
          createParamsBuilder(YARN_SITE_XML_INVALID)
          .withClusterResource(CLUSTER_RESOURCE_STRING)
          .build();
      converter.convert(params);
    });
  }

  @Test
  public void testConversionWithInvalidPlacementRules() throws Exception {
    assertThrows(ServiceStateException.class, () -> {
      config = new Configuration(false);
      config.set(FairSchedulerConfiguration.ALLOCATION_FILE,
          FS_INVALID_PLACEMENT_RULES_XML);
      config.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
      converter.convert(config);
    });
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

    assertEquals(null, convertedConfig.getClass(
        CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS, null),
        "Resource calculator class shouldn't be set");
  }

  @Test
  public void testConversionWhenMixedPolicyIsUsed() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource(CLUSTER_RESOURCE_STRING)
        .withFairSchedulerXmlConfig(FS_MIXED_POLICY_XML)
        .build();

    converter.convert(params);

    Configuration convertedConfig = converter.getYarnSiteConfig();

    assertEquals(DominantResourceCalculator.class,
        convertedConfig.getClass(
        CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS, null),
        "Resource calculator type");
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
      assertEquals(5, description.getRules().size(), "Number of rules");
    } else {
      // by default, FS internally creates 2 rules
      assertEquals(2, description.getRules().size(), "Number of rules");
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
    assertTrue(schedulingEnabledValue, "Asynchronous scheduling should be true");
  }

  @Test
  public void testConversionWhenAsyncSchedulingIsDisabled() throws Exception {
    boolean schedulingEnabledValue =  testConversionWithAsyncSchedulingOption(false);
    assertEquals(CapacitySchedulerConfiguration.DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE,
        schedulingEnabledValue, "Asynchronous scheduling should be the default value");
  }

  @Test
  public void testSiteDisabledPreemptionWithObserveOnlyConversion()
      throws Exception{
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withDisablePreemption(FSConfigToCSConfigConverterParams.
            PreemptionMode.OBSERVE_ONLY)
        .build();

    converter.convert(params);
    assertTrue(converter.getCapacitySchedulerConfig().
        getPreemptionObserveOnly(), "The observe only should be true");
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
