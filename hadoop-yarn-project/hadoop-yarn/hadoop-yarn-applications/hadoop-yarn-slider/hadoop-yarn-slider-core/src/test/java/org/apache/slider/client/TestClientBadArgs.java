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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.common.params.Arguments;
import org.apache.slider.common.params.SliderActions;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.UsageException;
import org.apache.slider.utils.SliderTestBase;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Test the argument parsing/validation logic.
 */
public class TestClientBadArgs extends SliderTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestClientBadArgs.class);

  @Test
  public void testNoAction() throws Throwable {
    launchExpectingException(SliderClient.class,
                             createTestConfig(),
                             "Usage: slider COMMAND",
                             EMPTY_LIST);

  }

  @Test
  public void testUnknownAction() throws Throwable {
    launchExpectingException(SliderClient.class,
                             createTestConfig(),
                             "not-a-known-action",
                             Arrays.asList("not-a-known-action"));
  }

  @Test
  public void testActionWithoutOptions() throws Throwable {
    launchExpectingException(SliderClient.class,
                             createTestConfig(),
                             "Usage: slider build <application>",
                             Arrays.asList(SliderActions.ACTION_BUILD));
  }

  @Test
  public void testActionWithoutEnoughArgs() throws Throwable {
    launchExpectingException(SliderClient.class,
                             createTestConfig(),
                             ErrorStrings.ERROR_NOT_ENOUGH_ARGUMENTS,
                             Arrays.asList(SliderActions.ACTION_START));
  }

  @Test
  public void testActionWithTooManyArgs() throws Throwable {
    launchExpectingException(SliderClient.class,
                             createTestConfig(),
                             ErrorStrings.ERROR_TOO_MANY_ARGUMENTS,
                             Arrays.asList(SliderActions.ACTION_HELP,
                             "hello, world"));
  }

  @Test
  public void testBadImageArg() throws Throwable {
    launchExpectingException(SliderClient.class,
                             createTestConfig(),
                             "Unknown option: --image",
                            Arrays.asList(SliderActions.ACTION_HELP,
                             Arguments.ARG_IMAGE));
  }

  @Test
  public void testRegistryUsage() throws Throwable {
    Throwable exception = launchExpectingException(SliderClient.class,
        createTestConfig(),
        "org.apache.slider.core.exceptions.UsageException: Argument --name " +
            "missing",
        Arrays.asList(SliderActions.ACTION_REGISTRY));
    assertTrue(exception instanceof UsageException);
    LOG.info(exception.toString());
  }

  @Test
  public void testRegistryExportBadUsage1() throws Throwable {
    Throwable exception = launchExpectingException(SliderClient.class,
        createTestConfig(),
        "Expected a value after parameter --getexp",
        Arrays.asList(SliderActions.ACTION_REGISTRY,
            Arguments.ARG_NAME,
            "cl1",
            Arguments.ARG_GETEXP));
    assertTrue(exception instanceof BadCommandArgumentsException);
    LOG.info(exception.toString());
  }

  @Test
  public void testRegistryExportBadUsage2() throws Throwable {
    Throwable exception = launchExpectingException(SliderClient.class,
        createTestConfig(),
        "Expected a value after parameter --getexp",
        Arrays.asList(SliderActions.ACTION_REGISTRY,
            Arguments.ARG_NAME,
            "cl1",
            Arguments.ARG_LISTEXP,
        Arguments.ARG_GETEXP));
    assertTrue(exception instanceof BadCommandArgumentsException);
    LOG.info(exception.toString());
  }

  @Test
  public void testRegistryExportBadUsage3() throws Throwable {
    Throwable exception = launchExpectingException(SliderClient.class,
        createTestConfig(),
        "Usage: registry",
        Arrays.asList(SliderActions.ACTION_REGISTRY,
            Arguments.ARG_NAME,
            "cl1",
            Arguments.ARG_LISTEXP,
            Arguments.ARG_GETEXP,
            "export1"));
    assertTrue(exception instanceof UsageException);
    LOG.info(exception.toString());
  }

  @Test
  public void testUpgradeUsage() throws Throwable {
    Throwable exception = launchExpectingException(SliderClient.class,
        createTestConfig(),
        "org.apache.slider.core.exceptions.BadCommandArgumentsException: Not " +
            "enough arguments for action: upgrade Expected minimum 1 but got 0",
        Arrays.asList(SliderActions.ACTION_UPGRADE));
    assertTrue(exception instanceof BadCommandArgumentsException);
    LOG.info(exception.toString());
  }

  public Configuration createTestConfig() {
    Configuration configuration = new Configuration();
    configuration.set(YarnConfiguration.RM_ADDRESS,  "127.0.0.1:8032");
    return configuration;
  }

  @Ignore
  @Test
  public void testUpgradeWithTemplateResourcesAndContainersOption() throws
      Throwable {
    //TODO test upgrade args
    String appName = "test_hbase";
    Throwable exception = launchExpectingException(SliderClient.class,
        createTestConfig(),
        "BadCommandArgumentsException: Option --containers cannot be "
        + "specified with --appdef",
        Arrays.asList(SliderActions.ACTION_UPGRADE,
            appName,
            Arguments.ARG_APPDEF,
            "/tmp/app.json",
            Arguments.ARG_CONTAINERS,
            "container_1"
        ));
    assertTrue(exception instanceof BadCommandArgumentsException);
    LOG.info(exception.toString());
  }

  @Ignore
  @Test
  public void testUpgradeWithTemplateResourcesAndComponentsOption() throws
      Throwable {
    //TODO test upgrade args
    String appName = "test_hbase";
    Throwable exception = launchExpectingException(SliderClient.class,
        createTestConfig(),
        "BadCommandArgumentsException: Option --components cannot be "
        + "specified with --appdef",
        Arrays.asList(SliderActions.ACTION_UPGRADE,
            appName,
            Arguments.ARG_APPDEF,
            "/tmp/app.json",
            Arguments.ARG_COMPONENTS,
            "HBASE_MASTER"
        ));
    assertTrue(exception instanceof BadCommandArgumentsException);
    LOG.info(exception.toString());
  }

  @Test
  public void testNodesMissingFile() throws Throwable {
    Throwable exception = launchExpectingException(SliderClient.class,
        createTestConfig(),
        "after parameter --out",
        Arrays.asList(SliderActions.ACTION_NODES, Arguments.ARG_OUTPUT));
    assertTrue(exception instanceof BadCommandArgumentsException);
  }

  @Test
  public void testFlexWithNoComponents() throws Throwable {
    Throwable exception = launchExpectingException(SliderClient.class,
        new Configuration(),
        "Usage: slider flex <application>",
        Arrays.asList(
            SliderActions.ACTION_FLEX,
            "flex1",
            Arguments.ARG_DEFINE,
            YarnConfiguration.RM_ADDRESS + "=127.0.0.1:8032"
        ));
    assertTrue(exception instanceof UsageException);
    LOG.info(exception.toString());
  }
}
