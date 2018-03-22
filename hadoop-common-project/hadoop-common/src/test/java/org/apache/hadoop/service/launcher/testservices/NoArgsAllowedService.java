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
import org.apache.hadoop.service.launcher.AbstractLaunchableService;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.service.launcher.ServiceLaunchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * service that does not allow any arguments.
 */
public class NoArgsAllowedService extends AbstractLaunchableService {

  private static final Logger LOG =
      LoggerFactory.getLogger(NoArgsAllowedService.class);

  public NoArgsAllowedService() {
    super("NoArgsAllowedService");
  }

  public static final String NAME =
      "org.apache.hadoop.service.launcher.testservices.NoArgsAllowedService";
  
  @Override
  public Configuration bindArgs(Configuration config, List<String> args)
      throws Exception {
    Configuration configuration = super.bindArgs(config, args);
    if (!args.isEmpty()) {
      StringBuilder argsList = new StringBuilder();
      for (String arg : args) {
        argsList.append('"').append(arg).append("\" ");
      }
      LOG.error("Got {} arguments: {}", args.size(), argsList);
      throw new ServiceLaunchException(
          LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR,
          "Expected 0 arguments but got %d: %s",
          args.size(),
          argsList);
    }
    return configuration;
  }
}
