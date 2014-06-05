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
import org.apache.hadoop.service.launcher.AbstractLaunchedService;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.service.launcher.ServiceLaunchException;

import java.util.List;

/**
 * service that does not allow any arguments
 */
public class NoArgsAllowedService extends AbstractLaunchedService {
  public NoArgsAllowedService() {
    super("NoArgsAllowedService");
  }

  public static final String NAME =
      "org.apache.hadoop.service.launcher.testservices.NoArgsAllowedService";
  
  @Override
  public Configuration bindArgs(Configuration config, List<String> args) throws
      Exception {
    Configuration configuration = super.bindArgs(config, args);
    if (!args.isEmpty()) {
      throw new ServiceLaunchException(LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR,
          "Expected 0 arguments but got %d",
          args.size());
    }
    return configuration;
  }
}
