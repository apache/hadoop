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

public interface YarnServiceConstants {

  /**
   * The path under which cluster and temp data are stored
   */
  String SERVICE_BASE_DIRECTORY = ".yarn";

  /**
   * The paths under which Service AM dependency libraries are stored
   */
  String DEPENDENCY_LOCALIZED_DIR_LINK = "service_dep";
  String DEPENDENCY_DIR = "/yarn-services/%s/";
  String DEPENDENCY_TAR_GZ_FILE_NAME = "service-dep";
  String DEPENDENCY_TAR_GZ_FILE_EXT = ".tar.gz";
  String DEPENDENCY_DIR_PERMISSIONS = "755";

  /**
   * Service type for YARN service
   */
  String APP_TYPE = "yarn-service";

  String KEYTAB_DIR = "keytabs";
  String KEYTAB_LOCATION = KEYTAB_DIR + "/%s" + ".keytab";

  String RESOURCE_DIR = "resources";


  String SERVICES_DIRECTORY = "services";

  /**
   * JVM property to define the service lib directory;
   * this is set by the yarn.sh script
   */
  String PROPERTY_LIB_DIR = "service.libdir";

  /**
   * name of generated dir for this conf
   */
  String SUBMITTED_CONF_DIR = "conf";

  /**
   * Service AM log4j file name
   */
  String YARN_SERVICE_LOG4J_FILENAME = "yarnservice-log4j.properties";

  /**
   * Log4j sysprop to name the resource
   */
  String SYSPROP_LOG4J_CONFIGURATION = "log4j.configuration";

  /**
   * sysprop for Service AM log4j directory
   */
  String SYSPROP_LOG_DIR = "LOG_DIR";

  String TMP_DIR_PREFIX = "tmp";


  String SERVICE_CORE_JAR = "yarn-service-core.jar";

  String STDOUT_AM = "serviceam-out.txt";
  String STDERR_AM = "serviceam-err.txt";

  String HADOOP_USER_NAME = "HADOOP_USER_NAME";

  String APP_CONF_DIR = "conf";
  String APP_RESOURCES_DIR = "resources";

  String APP_LIB_DIR = "lib";

  String OUT_FILE = "stdout.txt";
  String ERR_FILE = "stderr.txt";

  String CONTENT = "content";
  String PRINCIPAL = "yarn.service.am.principal";

  String UPGRADE_DIR = "upgrade";
  String CONTAINER_STATE_REPORT_AS_SERVICE_STATE =
      "yarn.service.container-state-report-as-service-state";
}
