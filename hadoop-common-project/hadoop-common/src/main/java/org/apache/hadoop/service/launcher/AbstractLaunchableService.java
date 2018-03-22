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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;

/**
 * Subclass of {@link AbstractService} that provides basic implementations
 * of the {@link LaunchableService} methods.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AbstractLaunchableService extends AbstractService
    implements LaunchableService {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractLaunchableService.class);

  /**
   * Construct an instance with the given name.
   */
  protected AbstractLaunchableService(String name) {
    super(name);
  }

  /**
   * {@inheritDoc}
   * <p>
   * The base implementation logs all arguments at the debug level,
   * then returns the passed in config unchanged.
   */
  
  @Override
  public Configuration bindArgs(Configuration config, List<String> args) throws
      Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Service {} passed in {} arguments:", getName(), args.size());
      for (String arg : args) {
        LOG.debug(arg);
      }
    }
    return config;
  }

  /**
   * {@inheritDoc}
   * <p>
   * The action is to signal success by returning the exit code 0.
   */
  @Override
  public int execute() throws Exception {
    return LauncherExitCodes.EXIT_SUCCESS;
  }
}
