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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
  String SLIDER_DEPENDENCY_TAR_GZ_FILE_NAME = "slider";
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
   * Key for component type. This MUST NOT be set in app_config/global {@value}
   */
  String COMPONENT_TYPE_KEY = "site.global.component_type";
  /**
   * A component type for an external app that has been predefined using the
   * slider build command
   */
  String COMPONENT_TYPE_EXTERNAL_APP = "external_app";
  String COMPONENT_SEPARATOR = "-";
  List<String> COMPONENT_KEYS_TO_SKIP = Collections.unmodifiableList(Arrays
      .asList("zookeeper.", "env.MALLOC_ARENA_MAX", "site.fs.", "site.dfs."));

  /**
   * A component type for a client component
   */
  String COMPONENT_TYPE_CLIENT = "client";

  /**
   * Key for application version. This must be set in app_config/global {@value}
   */
  String APP_VERSION = "site.global.app_version";
  String APP_VERSION_UNKNOWN = "awaiting heartbeat...";

  /**
   * Keys for application container specific properties, like release timeout
   */
  String APP_CONTAINER_RELEASE_TIMEOUT = "site.global.app_container.release_timeout_secs";
  int APP_CONTAINER_HEARTBEAT_INTERVAL_SEC = 10; // look for HEARTBEAT_IDDLE_INTERVAL_SEC

  /**
   * JVM arg to force IPv4  {@value}
   */
  String JVM_ENABLE_ASSERTIONS = "-ea";
  
  /**
   * JVM arg enable JVM system/runtime {@value}
   */
  String JVM_ENABLE_SYSTEM_ASSERTIONS = "-esa";

  /**
   * JVM arg to force IPv4  {@value}
   */
  String JVM_FORCE_IPV4 = "-Djava.net.preferIPv4Stack=true";

  /**
   * JVM arg to go headless  {@value}
   */

  String JVM_JAVA_HEADLESS = "-Djava.awt.headless=true";

  /**
   * This is the name of the dir/subdir containing
   * the hbase conf that is propagated via YARN
   *  {@value}
   */
  String PROPAGATED_CONF_DIR_NAME = "propagatedconf";
  String INFRA_DIR_NAME = "infra";
  String GENERATED_CONF_DIR_NAME = "generated";
  String SNAPSHOT_CONF_DIR_NAME = "snapshot";
  String DATA_DIR_NAME = "database";
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
  /**
   * XML resource listing the standard Slider providers
   * {@value}
   */
  String SLIDER_XML = "org/apache/slider/slider.xml";
  
  String CLUSTER_DIRECTORY = "cluster";

  String PACKAGE_DIRECTORY = "package";

  /**
   * JVM property to define the slider configuration directory;
   * this is set by the slider script: {@value}
   */
  String PROPERTY_CONF_DIR = "slider.confdir";

  /**
   * JVM property to define the slider lib directory;
   * this is set by the slider script: {@value}
   */
  String PROPERTY_LIB_DIR = "slider.libdir";

  /**
   * name of generated dir for this conf: {@value}
   */
  String SUBMITTED_CONF_DIR = "confdir";

  /**
   * Slider AM log4j file name : {@value}
   */
  String LOG4J_SERVER_PROP_FILENAME = "slideram-log4j.properties";

  /**
   * Standard log4j file name  : {@value}
   */
  String LOG4J_PROP_FILENAME = "log4j.properties";

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

  String TMP_LOGDIR_PREFIX = "/tmp/slider-";
  String TMP_DIR_PREFIX = "tmp";
  String AM_DIR_PREFIX = "appmaster";

  /**
   * Store the default app definition, e.g. metainfo file or content of a folder
   */
  String APP_DEF_DIR = "appdef";
  /**
   * Store additional app defs - co-processors
   */
  String ADDONS_DIR = "addons";

  String SLIDER_JAR = "slider.jar";
  String JCOMMANDER_JAR = "jcommander.jar";
  String GSON_JAR = "gson.jar";
  String DEFAULT_APP_PKG = "appPkg.zip";

  String DEFAULT_JVM_HEAP = "256M";
  int DEFAULT_YARN_MEMORY = 256;
  String STDOUT_AM = "slider-out.txt";
  String STDERR_AM = "slider-err.txt";
  String DEFAULT_GC_OPTS = "";

  String HADOOP_USER_NAME = "HADOOP_USER_NAME";
  String HADOOP_PROXY_USER = "HADOOP_PROXY_USER";
  String SLIDER_PASSPHRASE = "SLIDER_PASSPHRASE";

  boolean PROPAGATE_RESOURCE_OPTION = true;

  /**
   * Security associated keys.
   */
  String SECURITY_DIR = "security";
  String CRT_FILE_NAME = "ca.crt";
  String CSR_FILE_NAME = "ca.csr";
  String KEY_FILE_NAME = "ca.key";
  String KEYSTORE_FILE_NAME = "keystore.p12";
  String CRT_PASS_FILE_NAME = "pass.txt";
  String PASS_LEN = "50";

  String COMP_STORES_REQUIRED_KEY =
      "slider.component.security.stores.required";
  String COMP_KEYSTORE_PASSWORD_PROPERTY_KEY =
      "slider.component.keystore.password.property";
  String COMP_KEYSTORE_PASSWORD_ALIAS_KEY =
      "slider.component.keystore.credential.alias.property";
  String COMP_KEYSTORE_PASSWORD_ALIAS_DEFAULT =
      "component.keystore.credential.alias";
  String COMP_TRUSTSTORE_PASSWORD_PROPERTY_KEY =
      "slider.component.truststore.password.property";
  String COMP_TRUSTSTORE_PASSWORD_ALIAS_KEY =
      "slider.component.truststore.credential.alias.property";
  String COMP_TRUSTSTORE_PASSWORD_ALIAS_DEFAULT =
      "component.truststore.credential.alias";

  /**
   * Python specific
   */
  String PYTHONPATH = "PYTHONPATH";


  /**
   * Name of the AM filter to use: {@value}
   */
  String AM_FILTER_NAME =
      "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer";

  /**
   * Allowed port range. This MUST be set in app_conf/global.
   * {@value}
   */
  String KEY_ALLOWED_PORT_RANGE = "site.global.slider.allowed.ports";

  /**
   * env var for custom JVM options.
   */
  String SLIDER_JVM_OPTS = "SLIDER_JVM_OPTS";

  String SLIDER_CLASSPATH_EXTRA = "SLIDER_CLASSPATH_EXTRA";
  String YARN_CONTAINER_PATH = "/node/container/";

  String GLOBAL_CONFIG_TAG = "global";
  String SYSTEM_CONFIGS = "system_configs";
  String JAVA_HOME = "java_home";
  String TWO_WAY_SSL_ENABLED = "ssl.server.client.auth";
  String INFRA_RUN_SECURITY_DIR = "infra/run/security/";
  String CERT_FILE_LOCALIZATION_PATH = INFRA_RUN_SECURITY_DIR + "ca.crt";

  String AM_CONFIG_GENERATION = "am.config.generation";
  String APP_CONF_DIR = "app/conf";

  String APP_RESOURCES = "application.resources";
  String APP_RESOURCES_DIR = "app/resources";

  String APP_PACKAGES_DIR = "app/packages";
}
