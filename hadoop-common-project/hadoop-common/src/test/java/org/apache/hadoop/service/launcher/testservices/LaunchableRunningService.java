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
import org.apache.hadoop.service.launcher.LaunchableService;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A service which implements {@link LaunchableService}.
 * It
 * <ol>
 *   <li>does nothing in its {@link #serviceStart()}</li>
 *   <li>does its sleep+ maybe fail operation in its {@link #execute()}
 *   method</li>
 *   <li>gets the failing flag from the argument {@link #ARG_FAILING} first,
 *   the config file second.</li>
 *   <li>returns 0 for a successful execute</li>
 *   <li>returns a configurable exit code for a failing execute</li>
 *   <li>generates a new configuration in {@link #bindArgs(Configuration, List)}
 *   to verify that these propagate.</li>
 * </ol>
 */
public class LaunchableRunningService extends RunningService implements
    LaunchableService {
  public static final String NAME =
      "org.apache.hadoop.service.launcher.testservices.LaunchableRunningService";
  public static final String ARG_FAILING = "--failing";
  public static final String EXIT_CODE_PROP = "exit.code";
  private static final Logger LOG =
      LoggerFactory.getLogger(LaunchableRunningService.class);
  private int exitCode = 0;

  public LaunchableRunningService() {
    this("LaunchableRunningService");
  }

  public LaunchableRunningService(String name) {
    super(name);
  }

  @Override
  public Configuration bindArgs(Configuration config, List<String> args) throws
      Exception {
    Assert.assertEquals(STATE.NOTINITED, getServiceState());
    for (String arg : args) {
      LOG.info(arg);
    }
    Configuration newConf = new Configuration(config);
    if (args.contains(ARG_FAILING)) {
      LOG.info("CLI contains " + ARG_FAILING);
      failInRun = true;
      newConf.setInt(EXIT_CODE_PROP, LauncherExitCodes.EXIT_OTHER_FAILURE);
    }
    return newConf;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    if (conf.getBoolean(FAIL_IN_RUN, false)) {
      //if the conf value says fail, the exit code goes to it too
      exitCode = LauncherExitCodes.EXIT_FAIL;
    }
    // the exit code can be read off the property
    exitCode = conf.getInt(EXIT_CODE_PROP, exitCode);
  }

  @Override
  protected void serviceStart() throws Exception {
    // no-op
  }

  @Override
  public int execute() throws Exception {
    Thread.sleep(delayTime);
    if (failInRun) {
      return exitCode;
    }
    return 0;
  }

  public int getExitCode() {
    return exitCode;
  }

  public void setExitCode(int exitCode) {
    this.exitCode = exitCode;
  }
}
