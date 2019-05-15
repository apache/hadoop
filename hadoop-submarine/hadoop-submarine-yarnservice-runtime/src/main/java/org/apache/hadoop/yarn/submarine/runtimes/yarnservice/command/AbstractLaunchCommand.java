/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command;

import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup;
import java.io.IOException;

/**
 * Abstract base class for Launch command implementations for Services.
 * Currently we have launch command implementations
 * for TensorFlow PS, worker and Tensorboard instances.
 */
public abstract class AbstractLaunchCommand {
  private final LaunchScriptBuilder builder;

  public AbstractLaunchCommand(HadoopEnvironmentSetup hadoopEnvSetup,
      Component component, RunJobParameters parameters,
      String launchCommandPrefix) throws IOException {
    this.builder = new LaunchScriptBuilder(launchCommandPrefix, hadoopEnvSetup,
        parameters, component);
  }

  protected LaunchScriptBuilder getBuilder() {
    return builder;
  }

  /**
   * Subclasses need to defined this method and return a valid launch script.
   * Implementors can utilize the {@link LaunchScriptBuilder} using
   * the getBuilder method of this class.
   * @return The contents of a script.
   * @throws IOException If any IO issue happens.
   */
  public abstract String generateLaunchScript() throws IOException;

  /**
   * Subclasses need to provide a service-specific launch command
   * of the service.
   * Please note that this method should only return the launch command
   * but not the whole script.
   * @return The launch command
   */
  public abstract String createLaunchCommand();

}
