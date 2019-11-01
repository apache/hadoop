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

import com.google.common.collect.Lists;
import org.apache.commons.cli.MissingOptionException;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
  private FSConfigConverterTestCommons fsTestCommons;

  @Before
  public void setUp() throws IOException {
    fsTestCommons = new FSConfigConverterTestCommons();
    fsTestCommons.setUp();
  }

  @After
  public void tearDown() {
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
    return new FSConfigToCSConfigArgumentHandler(mockConverter);
  }

  private static String[] getDefaultArgumentsAsArray() {
    List<String> args = getDefaultArguments();
    return args.toArray(new String[0]);
  }

  private static List<String> getDefaultArguments() {
    return Lists.newArrayList("-y", FSConfigConverterTestCommons.YARN_SITE_XML,
        "-o", FSConfigConverterTestCommons.OUTPUT_DIR);
  }

  private String[] getArgumentsAsArrayWithDefaults(String... args) {
    List<String> result = getDefaultArguments();
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

    expectedException.expect(MissingOptionException.class);
    expectedException.expectMessage("Missing required option: y");

    argumentHandler.parseAndConvert(args);
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

    expectedException.expect(MissingOptionException.class);
    expectedException.expectMessage("Missing required option: o");

    argumentHandler.parseAndConvert(args);
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

    argumentHandler.parseAndConvert(args);
    System.out.println(fsTestCommons.getErrContent());
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
        new FSConfigToCSConfigArgumentHandler(mockConverter);

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE,
        "-r", FSConfigConverterTestCommons.CONVERSION_RULES_FILE);
    argumentHandler.parseAndConvert(args);

    // validate params
    Mockito.verify(mockConverter).convert(conversionParams.capture());
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
        new FSConfigToCSConfigArgumentHandler(mockConverter);

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE,
        "-r", FSConfigConverterTestCommons.CONVERSION_RULES_FILE, "-p");
    argumentHandler.parseAndConvert(args);

    // validate params
    Mockito.verify(mockConverter).convert(conversionParams.capture());
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
        new FSConfigToCSConfigArgumentHandler(mockConverter);

    String[] args = getArgumentsAsArrayWithDefaults("-f",
        FSConfigConverterTestCommons.FS_ALLOC_FILE,
        "-r", FSConfigConverterTestCommons.CONVERSION_RULES_FILE,
        "-p", "-c", "vcores=20, memory-mb=240");
    argumentHandler.parseAndConvert(args);

    // validate params
    Mockito.verify(mockConverter).convert(conversionParams.capture());
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
    argumentHandler.parseAndConvert(args);
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
    argumentHandler.parseAndConvert(args);
    assertTrue("Error content missing", fsTestCommons.getErrContent()
        .toString().contains("Fatal error during FS config conversion"));
  }
}