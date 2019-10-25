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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
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
  private static final Resource CLUSTER_RESOURCE =
      Resource.newInstance(16384, 16);
  private static final String FILE_PREFIX = "file:";
  private static final String FAIR_SCHEDULER_XML =
      prepareFileName("fair-scheduler-conversion.xml");

  @Mock
  private FSConfigToCSConfigRuleHandler ruleHandler;

  private FSConfigToCSConfigConverter converter;
  private Configuration config;

  private ByteArrayOutputStream csConfigOut;

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
    converter = new FSConfigToCSConfigConverter(ruleHandler);
    converter.setClusterResource(CLUSTER_RESOURCE);
    ByteArrayOutputStream yarnSiteOut = new ByteArrayOutputStream();
    csConfigOut = new ByteArrayOutputStream();

    converter.setCapacitySchedulerConfigOutputStream(csConfigOut);
    converter.setYarnSiteOutputStream(yarnSiteOut);
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
  public void testDefaultMaxApplications() throws Exception {
    converter.convert(config);

    Configuration conf = getConvertedCSConfig();
    int maxApps =
        conf.getInt(
            CapacitySchedulerConfiguration.MAXIMUM_SYSTEM_APPLICATIONS, -1);

    assertEquals("Default max apps", 15, maxApps);
  }

  @Test
  public void testDefaultMaxAMShare() throws Exception {
    converter.convert(config);

    Configuration conf = getConvertedCSConfig();
    String maxAmShare =
        conf.get(CapacitySchedulerConfiguration.
            MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT);

    assertEquals("Default max AM share", "0.16", maxAmShare);
  }

  @Test
  public void testConvertACLs() throws Exception {
    converter.convert(config);

    Configuration conf = getConvertedCSConfig();

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
  public void testDefaultMaxRunningApps() throws Exception {
    converter.convert(config);

    Configuration conf = getConvertedCSConfig();

    // default setting
    assertEquals("Default max apps", 15,
        conf.getInt(PREFIX + "maximum-applications", -1));
  }

  @Test
  public void testMixedQueueOrderingPolicy() throws Exception {
    expectedException.expect(ConversionException.class);
    expectedException.expectMessage(
        "DRF ordering policy cannot be used together with fifo/fair");
    String absolutePath =
        new File("src/test/resources/fair-scheduler-orderingpolicy-mixed.xml")
          .getAbsolutePath();
    config.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        FILE_PREFIX + absolutePath);

    converter.convert(config);
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
  public void testUserMaxAppsNotSupported() throws Exception {
    expectedException.expect(UnsupportedPropertyException.class);
    expectedException.expectMessage("userMaxApps");

    Mockito.doThrow(new UnsupportedPropertyException("userMaxApps"))
      .when(ruleHandler).handleUserMaxApps();

    converter.convert(config);
  }

  @Test
  public void testUserMaxAppsDefaultNotSupported() throws Exception {
    expectedException.expect(UnsupportedPropertyException.class);
    expectedException.expectMessage("userMaxAppsDefault");

    Mockito.doThrow(new UnsupportedPropertyException("userMaxAppsDefault"))
      .when(ruleHandler).handleUserMaxAppsDefault();

    converter.convert(config);
  }

  @Test
  public void testConvertFSConfigurationClusterResource() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource("vcores=20, memory-mb=240")
        .build();
    converter.convert(params);
    assertEquals("Resource", Resource.newInstance(240, 20),
        converter.getClusterResource());
  }

  @Test
  public void testConvertFSConfigPctModeUsedAndClusterResourceDefined()
      throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
            .withClusterResource("vcores=20, memory-mb=240")
            .build();
    converter.convert(params);
    assertEquals("Resource", Resource.newInstance(240, 20),
        converter.getClusterResource());
  }

  @Test
  public void testConvertFSConfigPctModeUsedAndClusterResourceNotDefined()
      throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
            .build();

    expectedException.expect(ConversionException.class);
    expectedException.expectMessage("cluster resource parameter" +
        " is not defined via CLI");

    converter.convert(params);
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
    ruleHandler = new FSConfigToCSConfigRuleHandler();
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
        .withClusterResource("vcores=20, memory-mb=240")
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
        .withClusterResource("vcores=20, memory-mb=240")
        .build();

    expectedException.expect(PreconditionException.class);
    expectedException.expectMessage("fair-scheduler.xml is not defined");
    converter.convert(params);
  }

  @Test
  public void testInvalidFairSchedulerXml() throws Exception {
    FSConfigToCSConfigConverterParams params = createDefaultParamsBuilder()
        .withClusterResource("vcores=20, memory-mb=240")
        .withFairSchedulerXmlConfig(FAIR_SCHEDULER_XML_INVALID)
        .build();

    expectedException.expect(RuntimeException.class);
    converter.convert(params);
  }

  @Test
  public void testInvalidYarnSiteXml() throws Exception {
    FSConfigToCSConfigConverterParams params =
        createParamsBuilder(YARN_SITE_XML_INVALID)
        .withClusterResource("vcores=20, memory-mb=240")
        .build();

    expectedException.expect(RuntimeException.class);
    converter.convert(params);
  }

  private Configuration getConvertedCSConfig() {
    ByteArrayInputStream input =
        new ByteArrayInputStream(csConfigOut.toByteArray());
    assertTrue("CS config output has length of 0!",
        csConfigOut.toByteArray().length > 0);
    Configuration conf = new Configuration(false);
    conf.addResource(input);

    return conf;
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
