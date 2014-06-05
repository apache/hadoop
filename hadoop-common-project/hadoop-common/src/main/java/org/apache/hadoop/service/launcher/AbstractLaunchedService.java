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
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Subclass of {@link AbstractService} that provides basic implementations
 * of the new methods
 */
public abstract class AbstractLaunchedService extends AbstractService
    implements LaunchedService {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractLaunchedService.class);

  /**
   * {@inheritDoc}
   */
  public AbstractLaunchedService(String name) {
    super(name);
  }

  /**
   * Implementation logs all arguments @ debug, then returns the passed
   * in config unchanged.
   * {@inheritDoc}
   */
  
  @Override
  public Configuration bindArgs(Configuration config, List<String> args) throws
      Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Service {} passed in {} arguments:", getName(),args.size());
      for (String arg : args) {
        LOG.debug(arg);
      }
    }
    return config;
  }

  /**
   * Default execution action is to succeed.
   * {@inheritDoc}
   */
  @Override
  public int execute() throws Exception {
    return LauncherExitCodes.EXIT_SUCCESS;
  }
}
