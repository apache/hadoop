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
import org.apache.hadoop.service.launcher.testservices.LaunchedRunningService;
import org.apache.hadoop.service.launcher.testservices.RunningService;
import org.junit.Test;

import java.util.List;

public class TestLaunchedRunningService extends AbstractServiceLauncherTestBase {

  @Test
  public void testRunService() throws Throwable {
    assertRuns(LaunchedRunningService.NAME);
  }

  @Test
  public void testConfPropagation() throws Throwable {
    Configuration conf = newConf(LaunchedRunningService.FAIL_IN_RUN, "true");
    assertLaunchOutcome(LauncherExitCodes.EXIT_FAIL,
        "failed",
        LaunchedRunningService.NAME,
        ServiceLauncher.ARG_CONF,
        configFile(conf));
  }

  @Test
  public void testArgBinding() throws Throwable {
    Configuration conf = newConf(LaunchedRunningService.FAIL_IN_RUN, "true");
    assertLaunchOutcome(LauncherExitCodes.EXIT_FAIL,
        "",
        LaunchedRunningService.NAME,
        LaunchedRunningService.ARG_FAILING);
  }

}
