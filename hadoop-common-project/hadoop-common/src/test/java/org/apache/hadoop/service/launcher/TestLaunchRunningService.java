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
import org.apache.hadoop.service.launcher.testservices.FailInStartService;
import org.apache.hadoop.service.launcher.testservices.RunningService;
import org.junit.Test;

import java.util.List;

public class TestLaunchRunningService extends AbstractServiceLauncherTestBase {

  @Test
  public void testRunService() throws Throwable {
    assertRuns(RunningService.NAME);
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

    List<String> argsList = asList("Name",ServiceLauncher.ARG_CONF, configFile(conf));
    List<String> args = launcher.extractConfigurationArgs(extracted,
        argsList);
    if (!args.isEmpty()) {
      assertEquals("args beginning with " + args.get(0),
          0, args.size());
    }
    assertEquals("true", extracted.get("propagated","unset"));
  }
}
