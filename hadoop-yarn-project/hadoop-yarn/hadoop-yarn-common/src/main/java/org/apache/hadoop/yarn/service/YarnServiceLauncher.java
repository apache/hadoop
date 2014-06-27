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

package org.apache.hadoop.yarn.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.launcher.ServiceLauncher;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.Arrays;
import java.util.List;

/**
 * This an extension of the base {@link ServiceLauncher} class that creates
 * a {@link YarnConfiguration} instance as the configuration class to use.
 * Services which prefer an argument of this type should use it
 * @param <S> service class to cast the generated service to.
 */
public class YarnServiceLauncher<S extends Service> extends ServiceLauncher<S> {

  public YarnServiceLauncher(String serviceClassName) {
    super(serviceClassName);
  }

  @Override
  protected Configuration createConfiguration() {
    return new YarnConfiguration();
  }

  /**
   * This is the JVM entry point for the service launcher.
   * <p>
   * Converts the arguments to a list, then invokes {@link #serviceMain(List)}
   * @param args command line arguments.
   */
  public static void main(String[] args) {
    serviceMain(Arrays.asList(args));
  }
    
  /**
   * The real main function, which takes the arguments as a list.
   * <p>
   * Argument 0 MUST be the service classname
   * @param argsList the list of arguments
   */
  public static void serviceMain(List<String> argsList) {
    if (argsList.isEmpty()) {
      // no arguments: usage message
      exitWithUsageMessage();
    } else {
      YarnServiceLauncher<Service> serviceLauncher =
          new YarnServiceLauncher<Service>(argsList.get(0));
      serviceLauncher.launchServiceAndExit(argsList);
    }
  }
}
