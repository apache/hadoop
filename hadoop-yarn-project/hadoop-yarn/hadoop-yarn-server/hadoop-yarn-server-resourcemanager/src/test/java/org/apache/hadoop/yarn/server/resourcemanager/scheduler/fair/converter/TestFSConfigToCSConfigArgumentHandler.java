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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

/**
 * Unit tests for FSConfigToCSConfigArgumentHandler.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class TestFSConfigToCSConfigArgumentHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFSConfigToCSConfigArgumentHandler.class);

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  private FSConfigToCSConfigConverter mockConverter;

  @Mock
  private ConvertedConfigValidator mockValidator;

  private DryRunResultHolder dryRunResultHolder;
  private ConversionOptions conversionOptions;

  private FSConfigConverterTestCommons fsTestCommons;

  @Before
  public void setUp() throws IOException {
    fsTestCommons = new FSConfigConverterTestCommons();
    fsTestCommons.setUp();
    dryRunResultHolder = new DryRunResultHolder();
    conversionOptions = new ConversionOptions(dryRunResultHolder, false);
  }

  @After
  public void tearDown() {
    QueueMetrics.clearQueueMetrics();
    fsTestCommons.tearDown();
  }

  private void setupFSConfigConversionFiles(boolean defineAllocationFile)
      throws IOException {
    FSConfigConverterTestCommons.configureFairSchedulerXml();

    if (defineAllocationFile) {
      FSConfigConverterTestCommons.configureYarnSiteXmlWithFsAllocFileDefined();
    } else {
      FSConfigConverterTestCommons.configureEmptyYarnSiteXml();
    }
    FSConfigConverterTestCommons.configureDummyConversionRulesFile();
  }


  private FSConfigToCSConfigArgumentHandler createArgumentHandler() {
    FSConfigToCSConfigArgumentHandler argumentHandler =
        new FSConfigToCSConfigArgumentHandler();
    argumentHandler.setConverterSupplier(this::getMockConverter);

    return argumentHandler;
  }

  private FSConfigToCSConfigConverter getMockConverter() {
    return mockConverter;
  }

  private static String[] getDefaultArgumentsAsArray() {
    List<String> args = getDefaultArguments();
    return args.toArray(new String[0]);
  }

  private static List<String> getDefaultArguments() {
    return Lists.newArrayList("-y", FSConfigConverterTestCommons.YARN_SITE_XML,
        "-o", FSConfigConverterTestCommons.OUTPUT_DIR);
  }

  private static List<String> getDefaultArgumentsWithNoOutput() {
    return Lists.newArrayList("-y", FSConfigConverterTestCommons.YARN_SITE_XML);
  }

  private String[] getArgumentsAsArrayWithDefaults(String... args) {
    List<String> result = getDefaultArguments();
    result.addAll(Arrays.asList(args));
    return result.toArray(new String[0]);
  }

  private String[] getArgumentsAsArrayWithDefaultsNoOutput(
      String... args) {
    List<String> result = getDefaultArgumentsWithNoOutput();
    result.addAll(Arrays.asList(args));
    return result.toArray(new String[0]);
  }

  private String[] getArgumentsAsArray(String... args) {
    List<String> result = Lists.newArrayList();
    result.addAll(Arrays.asList(args));
    return result.toArray(new String[0]);
  }

  @Test
  public void testMissingYarnSiteXmlArgument() throws Exception {
    setupFSConfigConversionFiles(true);

    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    String[] args = new String[] {"-o",
        FSConfigConverterTestCommons.OUTPUT_DIR};

    int retVal = argumentHandler.parseAndConvert(args);
    assertEquals("Return value", -1, retVal);

    assertTrue("Error content missing", fsTestCommons.getErrContent()
        .toString().contains("Missing yarn-site.xml parameter"));
  }

  @Test
  public void testMissingFairSchedulerXmlArgument() throws Exception {
    setupFSConfigConversionFiles(true);
    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    argumentHandler.parseAndConvert(getDefaultArgumentsAsArray());
  }

  @Test
  public void testMissingOutputDirArgument() throws Exception {
    setupFSConfigConversionFiles(true);

    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    String[] args = new String[] {"-y",
        FSConfigConverterTestCommons.YARN_SITE_XML};

    int retVal = argumentHandler.parseAndConvert(args);
    assertEquals("Return value", -1, retVal);

    assertTrue("Error content missing", fsTestCommons.getErrContent()
        .toString()
        .contains("Output directory or console mode was not defined"));
  }

  @Test
  public void testMissingRulesConfiguration() throws Exception {
    setupFSConfigConversionFiles(true);
    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    argumentHandler.parseAndConvert(getDefaultArgumentsAsArray());
  }

  @Test
  public void testInvalidRulesConfigFile() throws Exception {
    FSConfigConverterTestCommons.configureYarnSiteXmlWithFsAllocFileDefined();
    FSConfigConverterTestCommons.configureFairSchedulerXml();
    FSConfigConverterTestCommons.configureInvalidConversionRulesFile();

    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    String[] args = getArgumentsAsArrayWithDefaults();
    argumentHandler.parseAndConvert(args);
  }

  @Test
  public void testInvalidOutputDir() throws Exception {
    FSConfigConverterTestCommons.configureYarnSiteXmlWithFsAllocFileDefined();
    FSConfigConverterTestCommons.configureFairSchedulerXml();
    FSConfigConverterTestCommons.configureDummyConversionRulesFile();

    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    String[] args = getArgumentsAsArray("-y",
        FSConfigConverterTestCommons.YARN_SITE_XML, "-o",
        FSConfigConverterTestCommons.YARN_SITE_XML);

    int retVal = argumentHandler.parseAndConvert(args);
    assertEquals("Return value", -1, retVal);
    assertTrue("Error content missing", fsTestCommons.getErrContent()
        .toString()
        .contains("Cannot start FS config conversion due to the following " +
            "precondition error"));
  }

  @Test
  public void testFairSchedulerXmlIsNotDefinedIfItsDefinedInYarnSiteXml()
      throws Exception {
    setupFSConfigConversionFiles(true);
    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();
    argumentHandler.parseAndConvert(getDefaultArgumentsAsArray());
  }

  @Test
  public void testEmptyYarnSiteXmlSpecified() throws Exception {
    FSConfigConverterTestCommons.configureFairSchedulerXml();
    FSConfigConverterTestCommons.configureEmptyYarnSiteXml();
    FSConfigConverterTestCommons.configureDummyConversionRulesFile();

    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE);
    argumentHandler.parseAndConvert(args);
  }

  @Test
  public void testEmptyFairSchedulerXmlSpecified() throws Exception {
    FSConfigConverterTestCommons.configureEmptyFairSchedulerXml();
    FSConfigConverterTestCommons.configureEmptyYarnSiteXml();
    FSConfigConverterTestCommons.configureDummyConversionRulesFile();

    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE);
    argumentHandler.parseAndConvert(args);
  }

  @Test
  public void testEmptyRulesConfigurationSpecified() throws Exception {
    FSConfigConverterTestCommons.configureEmptyFairSchedulerXml();
    FSConfigConverterTestCommons.configureEmptyYarnSiteXml();
    FSConfigConverterTestCommons.configureEmptyConversionRulesFile();

    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE,
        "-r", FSConfigConverterTestCommons.CONVERSION_RULES_FILE);
    argumentHandler.parseAndConvert(args);
  }

  @Test
  public void testConvertFSConfigurationDefaults() throws Exception {
    setupFSConfigConversionFiles(true);

    ArgumentCaptor<FSConfigToCSConfigConverterParams> conversionParams =
        ArgumentCaptor.forClass(FSConfigToCSConfigConverterParams.class);

    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE,
        "-r", FSConfigConverterTestCommons.CONVERSION_RULES_FILE);
    argumentHandler.parseAndConvert(args);

    // validate params
    verify(mockConverter).convert(conversionParams.capture());
    FSConfigToCSConfigConverterParams params = conversionParams.getValue();
    LOG.info("FS config converter parameters: " + params);

    assertEquals("Yarn site config",
        FSConfigConverterTestCommons.YARN_SITE_XML,
        params.getYarnSiteXmlConfig());
    assertEquals("FS xml", FSConfigConverterTestCommons.FS_ALLOC_FILE,
        params.getFairSchedulerXmlConfig());
    assertEquals("Conversion rules config",
        FSConfigConverterTestCommons.CONVERSION_RULES_FILE,
        params.getConversionRulesConfig());
    assertFalse("Console mode", params.isConsole());
  }

  @Test
  public void testConvertFSConfigurationWithConsoleParam()
      throws Exception {
    setupFSConfigConversionFiles(true);

    ArgumentCaptor<FSConfigToCSConfigConverterParams> conversionParams =
        ArgumentCaptor.forClass(FSConfigToCSConfigConverterParams.class);

    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE,
        "-r", FSConfigConverterTestCommons.CONVERSION_RULES_FILE, "-p");
    argumentHandler.parseAndConvert(args);

    // validate params
    verify(mockConverter).convert(conversionParams.capture());
    FSConfigToCSConfigConverterParams params = conversionParams.getValue();
    LOG.info("FS config converter parameters: " + params);

    assertEquals("Yarn site config",
        FSConfigConverterTestCommons.YARN_SITE_XML,
        params.getYarnSiteXmlConfig());
    assertEquals("FS xml", FSConfigConverterTestCommons.FS_ALLOC_FILE,
        params.getFairSchedulerXmlConfig());
    assertEquals("Conversion rules config",
        FSConfigConverterTestCommons.CONVERSION_RULES_FILE,
        params.getConversionRulesConfig());
    assertTrue("Console mode", params.isConsole());
  }

  @Test
  public void testConvertFSConfigurationClusterResource()
      throws Exception {
    setupFSConfigConversionFiles(true);

    ArgumentCaptor<FSConfigToCSConfigConverterParams> conversionParams =
        ArgumentCaptor.forClass(FSConfigToCSConfigConverterParams.class);

    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE,
        "-r", FSConfigConverterTestCommons.CONVERSION_RULES_FILE,
        "-p", "-c", "vcores=20, memory-mb=240");
    argumentHandler.parseAndConvert(args);

    // validate params
    verify(mockConverter).convert(conversionParams.capture());
    FSConfigToCSConfigConverterParams params = conversionParams.getValue();
    LOG.info("FS config converter parameters: " + params);

    assertEquals("Yarn site config",
        FSConfigConverterTestCommons.YARN_SITE_XML,
        params.getYarnSiteXmlConfig());
    assertEquals("FS xml",
        FSConfigConverterTestCommons.FS_ALLOC_FILE,
        params.getFairSchedulerXmlConfig());
    assertEquals("Conversion rules config",
        FSConfigConverterTestCommons.CONVERSION_RULES_FILE,
        params.getConversionRulesConfig());
    assertEquals("Cluster resource", "vcores=20, memory-mb=240",
        params.getClusterResource());
    assertTrue("Console mode", params.isConsole());
  }

  @Test
  public void testConvertFSConfigurationErrorHandling() throws Exception {
    setupFSConfigConversionFiles(true);

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE,
        "-r", FSConfigConverterTestCommons.CONVERSION_RULES_FILE, "-p");
    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    Mockito.doThrow(UnsupportedPropertyException.class)
      .when(mockConverter)
      .convert(ArgumentMatchers.any(FSConfigToCSConfigConverterParams.class));
    int retVal = argumentHandler.parseAndConvert(args);
    assertEquals("Return value", -1, retVal);
    assertTrue("Error content missing", fsTestCommons.getErrContent()
        .toString().contains("Unsupported property/setting encountered"));
  }

  @Test
  public void testConvertFSConfigurationErrorHandling2() throws Exception {
    setupFSConfigConversionFiles(true);

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE,
        "-r", FSConfigConverterTestCommons.CONVERSION_RULES_FILE, "-p");
    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    Mockito.doThrow(ConversionException.class).when(mockConverter)
      .convert(ArgumentMatchers.any(FSConfigToCSConfigConverterParams.class));
    int retVal = argumentHandler.parseAndConvert(args);
    assertEquals("Return value", -1, retVal);
    assertTrue("Error content missing", fsTestCommons.getErrContent()
        .toString().contains("Fatal error during FS config conversion"));
  }

  @Test
  public void testDryRunWhenPreconditionExceptionOccurs() throws Exception {
    testDryRunWithException(new PreconditionException("test"),
        "Cannot start FS config conversion");
  }

  @Test
  public void testDryRunWhenUnsupportedPropertyExceptionExceptionOccurs()
      throws Exception {
    testDryRunWithException(new UnsupportedPropertyException("test"),
        "Unsupported property/setting encountered");
  }

  @Test
  public void testDryRunWhenConversionExceptionExceptionOccurs()
      throws Exception {
    testDryRunWithException(new ConversionException("test"),
        "Fatal error during FS config conversion");
  }

  @Test
  public void testDryRunWhenIllegalArgumentExceptionExceptionOccurs()
      throws Exception {
    testDryRunWithException(new IllegalArgumentException("test"),
        "Fatal error during FS config conversion");
  }

  private void testDryRunWithException(Exception exception,
      String expectedErrorMessage) throws Exception {
    setupFSConfigConversionFiles(true);

    String[] args = getArgumentsAsArrayWithDefaultsNoOutput("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE,
        "-r", FSConfigConverterTestCommons.CONVERSION_RULES_FILE,
        "-d");
    FSConfigToCSConfigArgumentHandler argumentHandler =
        new FSConfigToCSConfigArgumentHandler(conversionOptions, mockValidator);
    argumentHandler.setConverterSupplier(this::getMockConverter);

    Mockito.doThrow(exception).when(mockConverter)
      .convert(ArgumentMatchers.any(FSConfigToCSConfigConverterParams.class));

    int retVal = argumentHandler.parseAndConvert(args);
    assertEquals("Return value", -1, retVal);
    assertEquals("Number of errors", 1, dryRunResultHolder.getErrors().size());
    String error = dryRunResultHolder.getErrors().iterator().next();
    assertTrue("Unexpected error message",
        error.contains(expectedErrorMessage));
  }

  @Test
  public void testDisabledTerminalRuleCheck() throws Exception {
    setupFSConfigConversionFiles(true);

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE,
        "-r", FSConfigConverterTestCommons.CONVERSION_RULES_FILE, "-p",
        "-t");

    FSConfigToCSConfigArgumentHandler argumentHandler =
        new FSConfigToCSConfigArgumentHandler(conversionOptions, mockValidator);
    argumentHandler.setConverterSupplier(this::getMockConverter);

    argumentHandler.parseAndConvert(args);

    assertTrue("-t switch had no effect",
        conversionOptions.isNoRuleTerminalCheck());
  }

  @Test
  public void testEnabledTerminalRuleCheck() throws Exception {
    setupFSConfigConversionFiles(true);

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE,
        "-r", FSConfigConverterTestCommons.CONVERSION_RULES_FILE, "-p");

    FSConfigToCSConfigArgumentHandler argumentHandler =
        new FSConfigToCSConfigArgumentHandler(conversionOptions, mockValidator);
    argumentHandler.setConverterSupplier(this::getMockConverter);

    argumentHandler.parseAndConvert(args);

    assertFalse("No terminal rule check was enabled",
        conversionOptions.isNoRuleTerminalCheck());
  }

  @Test
  public void testYarnSiteOptionInOutputFolder() throws Exception {
    setupFSConfigConversionFiles(true);

    FSConfigToCSConfigArgumentHandler argumentHandler =
        createArgumentHandler();

    String[] args = new String[] {
        "-y", FSConfigConverterTestCommons.YARN_SITE_XML,
        "-o", FSConfigConverterTestCommons.TEST_DIR};

    int retVal = argumentHandler.parseAndConvert(args);
    assertEquals("Return value", -1, retVal);

    assertTrue(fsTestCommons.getErrContent()
        .toString().contains("contains the yarn-site.xml"));
  }

  private void testFileExistsInOutputFolder(String file) throws Exception {
    File testFile = new File(FSConfigConverterTestCommons.OUTPUT_DIR, file);
    try {
      FileUtils.touch(testFile);

      setupFSConfigConversionFiles(true);

      FSConfigToCSConfigArgumentHandler argumentHandler =
          createArgumentHandler();

      String[] args = new String[] {
          "-y", FSConfigConverterTestCommons.YARN_SITE_XML,
          "-o", FSConfigConverterTestCommons.OUTPUT_DIR};

      int retVal = argumentHandler.parseAndConvert(args);
      assertEquals("Return value", -1, retVal);

      String expectedMessage = String.format(
          "already contains a file or directory named %s", file);

      assertTrue(fsTestCommons.getErrContent()
          .toString().contains(expectedMessage));
    } finally {
      if (testFile.exists()) {
        testFile.delete();
      }
    }
  }

  @Test
  public void testYarnSiteExistsInOutputFolder() throws Exception {
    testFileExistsInOutputFolder(
        YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
  }

  @Test
  public void testCapacitySchedulerXmlExistsInOutputFolder()
      throws Exception {
    testFileExistsInOutputFolder(
        YarnConfiguration.CS_CONFIGURATION_FILE);
  }

  @Test
  public void testPlacementRulesConversionEnabled() throws Exception {
    testPlacementRuleConversion(true);
  }

  @Test
  public void testPlacementRulesConversionDisabled() throws Exception {
    testPlacementRuleConversion(false);
  }

  private void testPlacementRuleConversion(boolean enabled) throws Exception {
    setupFSConfigConversionFiles(true);

    String[] args = null;
    if (enabled) {
      args = getArgumentsAsArrayWithDefaults("-f",
          FSConfigConverterTestCommons.FS_ALLOC_FILE,
          "-p", "-m");
    } else {
      args = getArgumentsAsArrayWithDefaults("-f",
          FSConfigConverterTestCommons.FS_ALLOC_FILE,
          "-p");
    }
    FSConfigToCSConfigArgumentHandler argumentHandler =
        new FSConfigToCSConfigArgumentHandler(conversionOptions,
            mockValidator);
    argumentHandler.setConverterSupplier(this::getMockConverter);

    argumentHandler.parseAndConvert(args);

    ArgumentCaptor<FSConfigToCSConfigConverterParams> captor =
        ArgumentCaptor.forClass(FSConfigToCSConfigConverterParams.class);
    verify(mockConverter).convert(captor.capture());
    FSConfigToCSConfigConverterParams params = captor.getValue();

    if (enabled) {
      assertTrue("-m switch had no effect", params.isConvertPlacementRules());
    } else {
      assertFalse("Placement rule conversion was enabled",
          params.isConvertPlacementRules());
    }
  }

  public void testValidatorInvocation() throws Exception {
    setupFSConfigConversionFiles(true);

    FSConfigToCSConfigArgumentHandler argumentHandler =
        new FSConfigToCSConfigArgumentHandler(conversionOptions,
            mockValidator);

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE);
    argumentHandler.parseAndConvert(args);

    verify(mockValidator).validateConvertedConfig(anyString());
  }

  @Test
  public void testValidationSkippedWhenCmdLineSwitchIsDefined()
      throws Exception {
    setupFSConfigConversionFiles(true);

    FSConfigToCSConfigArgumentHandler argumentHandler =
        new FSConfigToCSConfigArgumentHandler(conversionOptions,
            mockValidator);

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE, "-s");
    argumentHandler.parseAndConvert(args);

    verifyZeroInteractions(mockValidator);
  }

  @Test
  public void testValidationSkippedWhenOutputIsConsole() throws Exception {
    setupFSConfigConversionFiles(true);

    FSConfigToCSConfigArgumentHandler argumentHandler =
        new FSConfigToCSConfigArgumentHandler(conversionOptions,
            mockValidator);

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE, "-s", "-p");
    argumentHandler.parseAndConvert(args);

    verifyZeroInteractions(mockValidator);
  }

  @Test
  public void testEnabledAsyncScheduling() throws Exception {
    setupFSConfigConversionFiles(true);

    FSConfigToCSConfigArgumentHandler argumentHandler =
            new FSConfigToCSConfigArgumentHandler(conversionOptions, mockValidator);

    String[] args = getArgumentsAsArrayWithDefaults("-f",
            FSConfigConverterTestCommons.FS_ALLOC_FILE, "-p",
            "-a");
    argumentHandler.parseAndConvert(args);

    assertTrue("-a switch had no effect",
            conversionOptions.isEnableAsyncScheduler());
  }

  @Test
  public void testDisabledAsyncScheduling() throws Exception {
    setupFSConfigConversionFiles(true);

    FSConfigToCSConfigArgumentHandler argumentHandler =
            new FSConfigToCSConfigArgumentHandler(conversionOptions, mockValidator);

    String[] args = getArgumentsAsArrayWithDefaults("-f",
            FSConfigConverterTestCommons.FS_ALLOC_FILE, "-p");
    argumentHandler.parseAndConvert(args);

    assertFalse("-a switch wasn't provided but async scheduling option is true",
            conversionOptions.isEnableAsyncScheduler());
  }
}