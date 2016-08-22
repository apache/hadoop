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

package org.apache.slider.providers.agent;

/*

 */
public interface AgentKeys {

  String PROVIDER_AGENT = "agent";
  String ROLE_NODE = "echo";

  /**
   * Template stored in the slider classpath -to use if there is
   * no site-specific template
   * {@value}
   */
  String CONF_RESOURCE = "org/apache/slider/providers/agent/conf/";
  /*  URL to talk back to Agent Controller*/
  String CONTROLLER_URL = "agent.controller.url";
  /**
   * The location of pre-installed agent path.
   * This can be also be dynamically computed based on Yarn installation of agent.
   */
  String PACKAGE_PATH = "agent.package.root";
  /**
   * The location of the script implementing the command.
   */
  String SCRIPT_PATH = "agent.script";
  /**
   * Execution home for the agent.
   */
  String APP_HOME = "app.home";
  String APP_ROOT = "site.global.app_root";
  String APP_CLIENT_ROOT = "client_root";
  /**
   * Runas user of the application
   */
  String RUNAS_USER = "site.global.app_user";
  /**
   * Name of the service.
   */
  String SERVICE_NAME = "app.name";
  String ARG_LABEL = "--label";
  String ARG_HOST = "--host";
  String ARG_PORT = "--port";
  String ARG_SECURED_PORT = "--secured_port";
  String ARG_ZOOKEEPER_QUORUM = "--zk-quorum";
  String ARG_ZOOKEEPER_REGISTRY_PATH = "--zk-reg-path";
  String ARG_DEBUG = "--debug";
  String AGENT_MAIN_SCRIPT_ROOT = "./infra/agent/slider-agent/";
  String AGENT_JINJA2_ROOT = "./infra/agent/slider-agent/jinja2";
  String AGENT_MAIN_SCRIPT = "agent/main.py";

  String APP_DEF = "application.def";
  String APP_DEF_ORIGINAL = "application.def.original";
  String ADDON_PREFIX = "application.addon.";
  String ADDONS = "application.addons";
  String AGENT_VERSION = "agent.version";
  String AGENT_CONF = "agent.conf";
  String ADDON_FOR_ALL_COMPONENTS = "ALL";

  String APP_RESOURCES = "application.resources";
  String APP_RESOURCES_DIR = "app/resources";

  String APP_CONF_DIR = "app/conf";

  String AGENT_INSTALL_DIR = "infra/agent";
  String APP_DEFINITION_DIR = "app/definition";
  String ADDON_DEFINITION_DIR = "addon/definition";
  String AGENT_CONFIG_FILE = "infra/conf/agent.ini";
  String AGENT_VERSION_FILE = "infra/version";
  String APP_PACKAGES_DIR = "app/packages";
  String PER_COMPONENT = "per.component";
  String PER_GROUP = "per.group";

  String JAVA_HOME = "java_home";
  String PACKAGE_LIST = "package_list";
  String SYSTEM_CONFIGS = "system_configs";
  String WAIT_HEARTBEAT = "wait.heartbeat";
  String PYTHON_EXE = "python";
  String CREATE_DEF_ZK_NODE = "create.default.zookeeper.node";
  String HEARTBEAT_MONITOR_INTERVAL = "heartbeat.monitor.interval";
  String AGENT_INSTANCE_DEBUG_DATA = "agent.instance.debug.data";
  String AGENT_OUT_FILE = "slider-agent.out";
  String KEY_AGENT_TWO_WAY_SSL_ENABLED = "ssl.server.client.auth";
  String INFRA_RUN_SECURITY_DIR = "infra/run/security/";
  String CERT_FILE_LOCALIZATION_PATH = INFRA_RUN_SECURITY_DIR + "ca.crt";
  String KEY_CONTAINER_LAUNCH_DELAY = "container.launch.delay.sec";
  String TEST_RELAX_VERIFICATION = "test.relax.validation";
  String AM_CONFIG_GENERATION = "am.config.generation";

  String DEFAULT_METAINFO_MAP_KEY = "DEFAULT_KEY";
}


