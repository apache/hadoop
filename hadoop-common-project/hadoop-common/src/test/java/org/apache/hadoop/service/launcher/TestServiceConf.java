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
import org.apache.hadoop.service.launcher.testservices.LaunchableRunningService;
import org.apache.hadoop.service.launcher.testservices.RunningService;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

public class TestServiceConf extends AbstractServiceLauncherTestBase {

  @Test
  public void testRunService() throws Throwable {
    assertRuns(LaunchableRunningService.NAME);
  }

  @Test
  public void testConfPropagationOverInitBindings() throws Throwable {
    Configuration conf = newConf(LaunchableRunningService.FAIL_IN_RUN, "true");
    assertLaunchOutcome(EXIT_FAIL,
        "failed",
        LaunchableRunningService.NAME,
        ServiceLauncher.ARG_CONF,
        configFile(conf));
  }

  @Test
  public void testUnbalancedConfArg() throws Throwable {
    Configuration conf = newConf(RunningService.FAIL_IN_RUN, "true");
    assertLaunchOutcome(EXIT_COMMAND_ARGUMENT_ERROR,
        "missing",
        RunningService.NAME,
        ServiceLauncher.ARG_CONF);
  }

  @Test
  public void testConfArgMissingFile() throws Throwable {
    Configuration conf = newConf(RunningService.FAIL_IN_RUN, "true");
    assertLaunchOutcome(EXIT_COMMAND_ARGUMENT_ERROR,
        "not found",
        RunningService.NAME,
        ServiceLauncher.ARG_CONF,
        "no-file.xml");
  }

  @Test
  public void testConfPropagation() throws Throwable {
    Configuration conf = newConf(RunningService.FAIL_IN_RUN, "true");
    assertLaunchOutcome(EXIT_EXCEPTION_THROWN,
        RunningService.FAILURE_MESSAGE,
        RunningService.NAME,
        ServiceLauncher.ARG_CONF,
        configFile(conf));
  }

  /**
   * Low level conf value extraction test...just to make sure
   * that all works at the lower level
   * @throws Throwable
   */
  @Test
  public void testConfExtraction() throws Throwable {
    ServiceLauncher<RunningService> launcher =
        new ServiceLauncher<RunningService>(RunningService.NAME);
    Configuration conf = newConf("propagated", "true");
    assertEquals("true", conf.get("propagated", "unset"));

    Configuration extracted = new Configuration(false);

    List<String> argsList =
        asList("Name", ServiceLauncher.ARG_CONF, configFile(conf));
    List<String> args = launcher.extractConfigurationArgs(extracted,
        argsList);
    if (!args.isEmpty()) {
      assertEquals("args beginning with " + args.get(0),
          0, args.size());
    }
    assertEquals("true", extracted.get("propagated", "unset"));
  }

  /**
   * Low level conf value extraction test...just to make sure
   * that all works at the lower level
   * @throws Throwable
   */
  @Test
  public void testDualConfArgs() throws Throwable {
    ServiceLauncher<RunningService> launcher =
        new ServiceLauncher<RunningService>(RunningService.NAME);
    String key1 = "key1";
    Configuration conf1 = newConf(key1, "true");
    String key2 = "file2";
    Configuration conf2 = newConf(key2, "7");
    Configuration extracted = new Configuration(false);

    List<String> argsList =
        asList("Name",
            ServiceLauncher.ARG_CONF, configFile(conf1),
            ServiceLauncher.ARG_CONF, configFile(conf2))
        ;
    List<String> args = launcher.extractConfigurationArgs(extracted,
        argsList);
    if (!args.isEmpty()) {
      assertEquals("args beginning with " + args.get(0),
          0, args.size());
    }
    assertTrue(extracted.getBoolean(key1, false));
    assertEquals(7, extracted.getInt(key2, -1));
  }


  @Test
  public void testConfArgWrongMissingFiletype() throws Throwable {
    File file = new File(CONF_FILE_DIR, methodName.getMethodName());
    FileWriter fileWriter = new FileWriter(file);
    fileWriter.write("not-a-conf-file");
    fileWriter.close();
    assertLaunchOutcome(EXIT_COMMAND_ARGUMENT_ERROR,
        "not loadable",
        RunningService.NAME,
        ServiceLauncher.ARG_CONF,
        file.getAbsolutePath());
  }


}
