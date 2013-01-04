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
 * TODO: Investigate the semantics and security of each cross-boundary refs.
 */
public interface ApplicationConstants {

  // TODO: They say tokens via env isn't good.
  public static final String APPLICATION_MASTER_TOKEN_ENV_NAME =
    "AppMasterTokenEnv";

  // TODO: They say tokens via env isn't good.
  public static final String APPLICATION_CLIENT_SECRET_ENV_NAME =
    "AppClientSecretEnv";
  
  /**
   * The environment variable for CONTAINER_ID. Set in AppMaster environment
   * only
   */
  public static final String AM_CONTAINER_ID_ENV = "AM_CONTAINER_ID";
  
  /**
   * The environment variable for APPLICATION_ATTEMPT_ID. Set in AppMaster
   * environment only
   */
  public static final String AM_APP_ATTEMPT_ID_ENV = "AM_APP_ATTEMPT_ID";

  /**
   * The environment variable for the NM_HOST. Set in the AppMaster environment
   * only
   */
  public static final String NM_HOST_ENV = "NM_HOST";
  
  /**
   * The environment variable for the NM_PORT. Set in the AppMaster environment
   * only
   */
  public static final String NM_PORT_ENV = "NM_PORT";
  
  /**
   * The environment variable for the NM_HTTP_PORT. Set in the AppMaster environment
   * only
   */
  public static final String NM_HTTP_PORT_ENV = "NM_HTTP_PORT";
  
  /**
   * The environment variable for APP_SUBMIT_TIME. Set in AppMaster environment
   * only
   */
  public static final String APP_SUBMIT_TIME_ENV = "APP_SUBMIT_TIME_ENV";

  public static final String CONTAINER_TOKEN_FILE_ENV_NAME =
      UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

  public static final String LOCAL_DIR_ENV = "YARN_LOCAL_DIRS";

  /**
   * The environmental variable for APPLICATION_WEB_PROXY_BASE. Set in 
   * ApplicationMaster's environment only. This states that for all non-relative
   * web URLs in the app masters web UI what base should they have.
   */
  public static final String APPLICATION_WEB_PROXY_BASE_ENV = 
    "APPLICATION_WEB_PROXY_BASE";

  public static final String LOG_DIR_EXPANSION_VAR = "<LOG_DIR>";

  public static final String STDERR = "stderr";

  public static final String STDOUT = "stdout";

  /**
   * Environment for Applications.
   * 
   * Some of the environment variables for applications are <em>final</em> 
   * i.e. they cannot be modified by the applications.
   */
  public enum Environment {
    /**
     * $USER
     * Final, non-modifiable.
     */
    USER("USER"),
    
    /**
     * $LOGNAME
     * Final, non-modifiable.
     */
    LOGNAME("LOGNAME"),
    
    /**
     * $HOME
     * Final, non-modifiable.
     */
    HOME("HOME"),
    
    /**
     * $PWD
     * Final, non-modifiable.
     */
    PWD("PWD"),
    
    /**
     * $PATH
     */
    PATH("PATH"),
    
    /**
     * $SHELL
     */
    SHELL("SHELL"),
    
    /**
     * $JAVA_HOME
     */
    JAVA_HOME("JAVA_HOME"),
    
    /**
     * $CLASSPATH
     */
    CLASSPATH("CLASSPATH"),
    
    /**
     * $APP_CLASSPATH
     */
    APP_CLASSPATH("APP_CLASSPATH"),
    
    /**
     * $LD_LIBRARY_PATH
     */
    LD_LIBRARY_PATH("LD_LIBRARY_PATH"),
    
    /**
     * $HADOOP_CONF_DIR
     * Final, non-modifiable.
     */
    HADOOP_CONF_DIR("HADOOP_CONF_DIR"),
    
    /**
     * $HADOOP_COMMON_HOME
     */
    HADOOP_COMMON_HOME("HADOOP_COMMON_HOME"),
    
    /**
     * $HADOOP_HDFS_HOME
     */
    HADOOP_HDFS_HOME("HADOOP_HDFS_HOME"),
    
    /**
     * $MALLOC_ARENA_MAX
     */
    MALLOC_ARENA_MAX("MALLOC_ARENA_MAX"),
    
    /**
     * $HADOOP_YARN_HOME
     */
    HADOOP_YARN_HOME("HADOOP_YARN_HOME");

    private final String variable;
    private Environment(String variable) {
      this.variable = variable;
    }
    
    public String key() {
      return variable;
    }
    
    public String toString() {
      return variable;
    }
    
    public String $() {
      return "$" + variable;
    }
  }
}
