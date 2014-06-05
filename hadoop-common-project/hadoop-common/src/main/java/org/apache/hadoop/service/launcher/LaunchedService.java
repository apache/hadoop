/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.service.launcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;

import java.util.List;

/**
 * An interface which services can implement to have their
 * execution managed by the ServiceLauncher.
 * The command line options will be passed down before the 
 * {@link Service#init(Configuration)} operation is invoked via an
 * invocation of {@link LaunchedService#bindArgs(Configuration, List)}
 * After the service has been successfully started via {@link Service#start()}
 * the {@link LaunchedService#execute()} method is called to execute the 
 * service. When this method returns, the service launcher will exit, using
 * the return code from the method as its exit option.
 */
public interface LaunchedService extends Service {

  /**
   * Propagate the command line arguments.
   * This method is called before {@link #init(Configuration)};
   * Any non-null configuration that is returned from this operation
   * becomes the one that is passed on to that {@link #init(Configuration) operation.
   * 
   * This permits implementations to change the configuration before
   * the init operation. As the ServiceLauncher only creates
   * an instance of the base {@link Configuration} class, it is
   * recommended to instantiate any subclass (such as YarnConfiguration)
   * that injects new resources.
   * 
   * @param config the initial configuration build up by the
   * service launcher.
   * @param args list of arguments passed to the command line
   * after any launcher-specific commands have been stripped.
   * @return the configuration to init the service with.
   * Recommended: pass down the config parameter with any changes
   * @throws Exception any problem
   */
  Configuration bindArgs(Configuration config, List<String> args) throws Exception;
  
  /**
   * Run a service. This called after {@link Service#start()}
   * @return the exit code
   * @throws Throwable any exception to report
   */
  int execute() throws Throwable ;
}
