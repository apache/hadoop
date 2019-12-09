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

package org.apache.hadoop.yarn.service.conf;

import org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes;

public interface SliderExitCodes extends LauncherExitCodes {

  /**
   * starting point for exit codes; not an exception itself
   */
  int _EXIT_CODE_BASE =           64;

  /**
   * service entered the failed state: {@value}
   */
  int EXIT_YARN_SERVICE_FAILED =  65;

  /**
   * service was killed: {@value}
   */
  int EXIT_YARN_SERVICE_KILLED =  66;

  /**
   * timeout on monitoring client: {@value}
   */
  int EXIT_TIMED_OUT =            67;

  /**
   * service finished with an error: {@value}
   */
  int EXIT_YARN_SERVICE_FINISHED_WITH_ERROR = 68;

  /**
   * the service instance is unknown: {@value}
   */
  int EXIT_UNKNOWN_INSTANCE =     69;

  /**
   * the service instance is in the wrong state for that operation: {@value}
   */
  int EXIT_BAD_STATE =            70;

  /**
   * A spawned master process failed 
   */
  int EXIT_PROCESS_FAILED =       71;

  /**
   * The instance failed -too many containers were
   * failing or some other threshold was reached
   */
  int EXIT_DEPLOYMENT_FAILED =    72;

  /**
   * The service is live -and the requested operation
   * does not work if the cluster is running
   */
  int EXIT_APPLICATION_IN_USE =   73;

  /**
   * There already is an service instance of that name
   * when an attempt is made to create a new instance
   */
  int EXIT_INSTANCE_EXISTS =      75;

  /**
   * Exit code when the configurations in valid/incomplete: {@value}
   */
  int EXIT_BAD_CONFIGURATION =    77;

}
