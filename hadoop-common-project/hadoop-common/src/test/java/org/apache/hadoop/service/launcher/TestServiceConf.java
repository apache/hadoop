/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.service.launcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.launcher.testservices.LaunchableRunningService;
import org.apache.hadoop.service.launcher.testservices.RunningService;
import static org.apache.hadoop.service.launcher.LauncherArguments.*;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

/**
 * Test how configuration files are loaded off the command line.
 */
public class TestServiceConf
    extends AbstractServiceLauncherTestBase {

  @Test
  public void testRunService() throws Throwable {
    assertRuns(LaunchableRunningService.NAME);
  }

  @Test
  public void testConfPropagationOverInitBindings() throws Throwable {
    Configuration conf = newConf(RunningService.FAIL_IN_RUN, "true");
    assertLaunchOutcome(EXIT_FAIL,
        "failed",
        LaunchableRunningService.NAME,
        ARG_CONF_PREFIXED,
        configFile(conf));
  }

  @Test
  public void testUnbalancedConfArg() throws Throwable {
    assertLaunchOutcome(EXIT_COMMAND_ARGUMENT_ERROR,
        E_PARSE_FAILED,
        LaunchableRunningService.NAME,
        ARG_CONF_PREFIXED);
  }

  @Test
  public void testConfArgMissingFile() throws Throwable {
    assertLaunchOutcome(EXIT_COMMAND_ARGUMENT_ERROR,
        E_PARSE_FAILED,
        LaunchableRunningService.NAME,
        ARG_CONF_PREFIXED,
        "no-file.xml");
  }

  @Test
  public void testConfPropagation() throws Throwable {
    Configuration conf = newConf(RunningService.FAIL_IN_RUN, "true");
    assertLaunchOutcome(EXIT_EXCEPTION_THROWN,
        RunningService.FAILURE_MESSAGE,
        RunningService.NAME,
        ARG_CONF_PREFIXED,
        configFile(conf));
  }

  /**
   * Low level conf value extraction test...just to make sure
   * that all works at the lower level.
   * @throws Throwable
   */
  @Test
  public void testConfExtraction() throws Throwable {
    ExitTrackingServiceLauncher<Service> launcher =
      new ExitTrackingServiceLauncher<>(RunningService.NAME);
    launcher.bindCommandOptions();
    Configuration conf = newConf("propagated", "true");
    assertEquals("true", conf.get("propagated", "unset"));

    Configuration extracted = new Configuration(false);

    List<String> argsList =
        asList("Name", ARG_CONF_PREFIXED, configFile(conf));
    List<String> args = launcher.extractCommandOptions(extracted,
        argsList);
    if (!args.isEmpty()) {
      assertEquals("args beginning with " + args.get(0),
          0, args.size());
    }
    assertEquals("true", extracted.get("propagated", "unset"));
  }

  @Test
  public void testDualConfArgs() throws Throwable {
    ExitTrackingServiceLauncher<Service> launcher =
        new ExitTrackingServiceLauncher<>(RunningService.NAME);
    launcher.bindCommandOptions();
    String key1 = "key1";
    Configuration conf1 = newConf(key1, "true");
    String key2 = "file2";
    Configuration conf2 = newConf(key2, "7");
    Configuration extracted = new Configuration(false);

    List<String> argsList =
        asList("Name",
            ARG_CONF_PREFIXED, configFile(conf1),
            ARG_CONF_PREFIXED, configFile(conf2));

    List<String> args = launcher.extractCommandOptions(extracted, argsList);
    if (!args.isEmpty()) {
      assertEquals("args beginning with " + args.get(0),
          0, args.size());
    }
    assertTrue(extracted.getBoolean(key1, false));
    assertEquals(7, extracted.getInt(key2, -1));
  }

  @Test
  public void testConfArgWrongFiletype() throws Throwable {
    new File(CONF_FILE_DIR).mkdirs();
    File file = new File(CONF_FILE_DIR, methodName.getMethodName());
    try (FileWriter fileWriter = new FileWriter(file)) {
      fileWriter.write("not-a-conf-file");
      fileWriter.close();
    }
    assertLaunchOutcome(EXIT_COMMAND_ARGUMENT_ERROR,
        "",
        RunningService.NAME,
        ARG_CONF_PREFIXED,
        file.getAbsolutePath());
  }

}
