/**
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

package org.apache.hadoop.yarn.api;

import org.apache.hadoop.security.UserGroupInformation;

/**
 * This is the API for the applications comprising of constants that YARN sets
 * up for the applications and the containers.
 * 
 * TODO: Should also be defined in avro/pb IDLs
 * TODO: Investigate the semantics and security of each cross-boundary refs.
 */
public interface ApplicationConstants {

  // TODO: They say tokens via env isn't good.
  public static final String APPLICATION_MASTER_TOKEN_ENV_NAME =
    "AppMasterTokenEnv";

  // TODO: They say tokens via env isn't good.
  public static final String APPLICATION_CLIENT_SECRET_ENV_NAME =
    "AppClientTokenEnv";

  // TODO: Weird. This is part of AM command line. Instead it should be a env.
  public static final String AM_FAIL_COUNT_STRING = "<FAILCOUNT>";

  public static final String CONTAINER_TOKEN_FILE_ENV_NAME =
      UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

  public static final String LOCAL_DIR_ENV = "YARN_LOCAL_DIRS";

  public static final String LOG_DIR_EXPANSION_VAR = "<LOG_DIR>";
}
