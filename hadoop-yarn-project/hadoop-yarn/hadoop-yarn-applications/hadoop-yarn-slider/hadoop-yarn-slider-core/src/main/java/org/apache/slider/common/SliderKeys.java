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

package org.apache.slider.common;

/**
 * Keys and various constants for Slider
 */
public interface SliderKeys extends SliderXmlConfKeys {

  /**
   * This is the name of the slider appmaster in configurations :{@value}
   */
  String COMPONENT_AM = "slider-appmaster";
  
  /**
   * Slider role is "special":{@value}
   */
  int ROLE_AM_PRIORITY_INDEX = 0;
  
  
  /**
   * The path under which cluster and temp data are stored
   * {@value}
   */
  String SLIDER_BASE_DIRECTORY = ".slider";

  /**
   * The paths under which Slider AM dependency libraries are stored
   */
  String SLIDER_DEPENDENCY_LOCALIZED_DIR_LINK = "slider_dep";
  String SLIDER_DEPENDENCY_HDP_PARENT_DIR = "/hdp";
  String SLIDER_DEPENDENCY_DIR = "/apps/%s/slider";
  String SLIDER_DEPENDENCY_TAR_GZ_FILE_NAME = "slider-dep";
  String SLIDER_DEPENDENCY_TAR_GZ_FILE_EXT = ".tar.gz";
  String SLIDER_DEPENDENCY_DIR_PERMISSIONS = "755";

  /**
   * 
   */
  String HDP_VERSION_PROP_NAME = "HDP_VERSION";

  /**
   *  name of the relative path to expaned an image into:  {@value}.
   *  The title of this path is to help people understand it when
   *  they see it in their error messages
   */
  String LOCAL_TARBALL_INSTALL_SUBDIR = "expandedarchive";


  /**
   * Application type for YARN  {@value}
   */
  String APP_TYPE = "org-apache-slider";

  /**
   * A component type for an external app that has been predefined using the
   * slider build command
   */
  String COMPONENT_SEPARATOR = "-";

  /**
   * A component type for a client component
   */
  String COMPONENT_TYPE_CLIENT = "client";

  /**
   * Key for application version.
   */
  String APP_VERSION_UNKNOWN = "awaiting heartbeat...";

  /**
   * Keys for application container specific properties, like release timeout
   */
  String APP_CONTAINER_RELEASE_TIMEOUT = "site.global.app_container.release_timeout_secs";

  /**
   * Subdirectories of HDFS cluster dir.
   */
  String DATA_DIR_NAME = "data";
  String HISTORY_DIR_NAME = "history";
  String HISTORY_FILENAME_SUFFIX = "json";
  String HISTORY_FILENAME_PREFIX = "rolehistory-";
  String KEYTAB_DIR = "keytabs";
  String RESOURCE_DIR = "resources";

  /**
   * Filename pattern is required to save in strict temporal order.
   * Important: older files must sort less-than newer files when using
   * case-sensitive name sort.
   */
  String HISTORY_FILENAME_CREATION_PATTERN = HISTORY_FILENAME_PREFIX +"%016x."+
                                    HISTORY_FILENAME_SUFFIX;
  /**
   * The posix regexp used to locate this 
   */
  String HISTORY_FILENAME_MATCH_PATTERN = HISTORY_FILENAME_PREFIX +"[0-9a-f]+\\."+
                                    HISTORY_FILENAME_SUFFIX;
    /**
   * The posix regexp used to locate this 
   */
  String HISTORY_FILENAME_GLOB_PATTERN = HISTORY_FILENAME_PREFIX +"*."+
                                    HISTORY_FILENAME_SUFFIX;

  String CLUSTER_DIRECTORY = "cluster";

  /**
   * JVM property to define the slider lib directory;
   * this is set by the slider script: {@value}
   */
  String PROPERTY_LIB_DIR = "slider.libdir";

  /**
   * name of generated dir for this conf: {@value}
   */
  String SUBMITTED_CONF_DIR = "conf";

  /**
   * Slider AM log4j file name : {@value}
   */
  String LOG4J_SERVER_PROP_FILENAME = "slideram-log4j.properties";

  /**
   * Log4j sysprop to name the resource :{@value}
   */
  String SYSPROP_LOG4J_CONFIGURATION = "log4j.configuration";

  /**
   * sysprop for Slider AM log4j directory :{@value}
   */
  String SYSPROP_LOG_DIR = "LOG_DIR";

  /**
   * name of the Slider client resource
   * loaded when the service is loaded.
   */
  String SLIDER_CLIENT_XML = "slider-client.xml";

  /**
   * The name of the resource to put on the classpath
   */
  String SLIDER_SERVER_XML = "slider-server.xml";

  String TMP_DIR_PREFIX = "tmp";

  /**
   * Store the default app definition, e.g. metainfo file or content of a folder
   */
  String APP_DEF_DIR = "appdef";
  /**
   * Store additional app defs - co-processors
   */
  String ADDONS_DIR = "addons";

  String SLIDER_JAR = "slider-core.jar";

  String STDOUT_AM = "slider-out.txt";
  String STDERR_AM = "slider-err.txt";

  String HADOOP_USER_NAME = "HADOOP_USER_NAME";

  /**
   * Name of the AM filter to use: {@value}
   */
  String AM_FILTER_NAME =
      "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer";

  String YARN_CONTAINER_PATH = "/node/container/";

  String APP_CONF_DIR = "conf";

  String APP_LIB_DIR = "lib";

  String OUT_FILE = "stdout.txt";
  String ERR_FILE = "stderr.txt";

  String QUICK_LINKS = "quicklinks";

  String KEY_CONTAINER_LAUNCH_DELAY = "container.launch.delay.sec";
}
