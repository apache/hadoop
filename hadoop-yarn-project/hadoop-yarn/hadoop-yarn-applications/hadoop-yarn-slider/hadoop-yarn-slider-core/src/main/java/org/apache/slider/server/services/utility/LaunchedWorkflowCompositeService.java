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

package org.apache.slider.server.services.utility;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.slider.core.main.LauncherExitCodes;
import org.apache.slider.core.main.RunService;
import org.apache.slider.server.services.workflow.WorkflowCompositeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a workflow compositoe service which can be launched from the CLI
 * ... catches the arguments and implements a stub runService operation.
 */
public class LaunchedWorkflowCompositeService extends WorkflowCompositeService
    implements RunService {
  private static final Logger log = LoggerFactory.getLogger(
      LaunchedWorkflowCompositeService.class);
  private String[] argv;
  
  public LaunchedWorkflowCompositeService(String name) {
    super(name);
  }

  public LaunchedWorkflowCompositeService(String name, Service... children) {
    super(name, children);
  }

  /**
   * Implementation of set-ness, groovy definition of true/false for a string
   * @param s
   * @return true iff the string is non-null and non-empty
   */
  protected static boolean isUnset(String s) {
    return StringUtils.isEmpty(s);
  }

  protected static boolean isSet(String s) {
    return StringUtils.isNotEmpty(s);
  }

  protected String[] getArgv() {
    return argv;
  }

  /**
   * Pre-init argument binding
   * @param config the initial configuration build up by the
   * service launcher.
   * @param args argument list list of arguments passed to the command line
   * after any launcher-specific commands have been stripped.
   * @return the configuration
   * @throws Exception
   */
  @Override
  public Configuration bindArgs(Configuration config, String... args) throws
                                                                      Exception {
    this.argv = args;
    if (log.isDebugEnabled()) {
      log.debug("Binding {} Arguments:", args.length);

      StringBuilder builder = new StringBuilder();
      for (String arg : args) {
        builder.append('"').append(arg).append("\" ");
      }
      log.debug(builder.toString());
    }
    return config;
  }

  @Override
  public int runService() throws Throwable {
    return LauncherExitCodes.EXIT_SUCCESS;
  }

  @Override
  public synchronized void addService(Service service) {
    Preconditions.checkArgument(service != null, "null service argument");
    super.addService(service);
  }

  /**
   * Run a child service -initing and starting it if this
   * service has already passed those parts of its own lifecycle
   * @param service the service to start
   */
  protected boolean deployChildService(Service service) {
    service.init(getConfig());
    addService(service);
    if (isInState(STATE.STARTED)) {
      service.start();
      return true;
    }
    return false;
  }

}
