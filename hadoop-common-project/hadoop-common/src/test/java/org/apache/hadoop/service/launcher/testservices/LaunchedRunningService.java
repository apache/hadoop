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

package org.apache.hadoop.service.launcher.testservices;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.launcher.LaunchedService;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A service which implements {@link LaunchedService} and
 * <ol>
 *   <li>does nothing in its {@link #serviceStart()}</li>
 *   <li>does its sleep+ maybe fail operation in its {@link #execute()} method</li>
 *   <li>gets the failing flag from the argument {@link #ARG_FAILING}</li>
 *   <li>returns 0 for a succesful execute</li>
 *   <li>returns {@link LauncherExitCodes#EXIT_FAIL} for a failing execute</li>
 * </ol>
 */
public class LaunchedRunningService extends RunningService implements
    LaunchedService {
  private static final Logger LOG =
      LoggerFactory.getLogger(RunningService.class);
  public static final String NAME =
      "org.apache.hadoop.service.launcher.testservices.LaunchedRunningService";
  public static final String ARG_FAILING = "--failing";

  @Override
  public Configuration bindArgs(Configuration config, List<String> args) throws
      Exception {
    if (args.contains(ARG_FAILING)) {
      LOG.info("CLI contains " + ARG_FAILING);
      failInRun = true;
    }
    return config;
  }

  @Override
  protected void serviceStart() throws Exception {
    // no-op
  }

  @Override
  public int execute() throws Throwable {
      Thread.sleep(delayTime);
      if (failInRun) {
        return LauncherExitCodes.EXIT_FAIL;
      }
    return 0;
  }
}
