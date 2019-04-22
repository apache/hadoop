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

package org.apache.hadoop.applications.mawo.server.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;

/**
 * MaWo configuration class.
 */
@Singleton
public final class MawoConfiguration {
  /**
   * logger initialization for mawo config class.
   */
  static final Logger LOG = LoggerFactory.getLogger(MawoConfiguration.class);
  /**
   * Define comma separator.
   */
  static final String COMMA_SPLITTER = ",";
  /**
   * MaWo config file name.
   */
  public static final String CONFIG_FILE = "mawo.properties";

  /**
   * RPC server hostname.
   */
  private static final String RPC_SERVER_HOSTNAME = "rpc.server.hostname";
  /**
   * RPC server port.
   */
  private static final String RPC_SERVER_PORT = "rpc.server.port";

  // Default values
  /**
   * RPC server hostname default value.
   */
  private static final String RPC_SERVER_HOSTNAME_DEFAULT = "localhost";
  /**
   * RPC server port default value.
   */
  private static final String RPC_SERVER_PORT_DEFAULT = "5121";

  // Curator related Configurations
  /**
   * Config to check id Job Queue Storage is enabled.
   */
  private static final String JOB_QUEUE_STORAGE_ENABLED =
      "mawo.job-queue-storage.enabled";

  /**
   * ZooKeeper property prefix.
   */
  private static final String ZK_PREFIX = "zookeeper.";
  /**
   * Property for ZooKeeper address.
   */
  private static final String ZK_ADDRESS = ZK_PREFIX + "address";
  /**
   * Default value for ZooKeeper address.
   */
  private static final String ZK_ADDRESS_DEFAULT = "localhost:2181";

  /**
   * Property for ZooKeeper parent path.
   */
  private static final String ZK_PARENT_PATH = ZK_PREFIX + "parent.path";
  /**
   * Property for ZooKeeper parent path default value.
   */
  private static final String ZK_PARENT_PATH_DEFAULT = "/mawoRoot";

  /**
   * Property for ZooKeeper retry interval.
   */
  private static final String ZK_RETRY_INTERVAL_MS =
      ZK_PREFIX + "retry.interval.ms";
  /**
   * Default value for ZooKeeper retry interval.
   */
  private static final String ZK_RETRY_INTERVAL_MS_DEFAULT = "1000";

  /**
   * Property for Zookeeper session timeout.
   */
  private static final String ZK_SESSION_TIMEOUT_MS =
      ZK_PREFIX + "session.timeout.ms";
  /**
   * Default value for ZooKeeper session timeout.
   */
  private static final String ZK_SESSION_TIMEOUT_MS_DEFAULT = "10000";

  /**
   * Property for ZooKeeper retry number.
   */
  private static final String ZK_RETRIES_NUM = ZK_PREFIX + "retries.num";
  /**
   * Default value for ZooKeeper retry number.
   */
  private static final String ZK_RETRIES_NUM_DEFAULT = "1000";

  /**
   * Property for ZooKeeper acl.
   */
  private static final String ZK_ACL = ZK_PREFIX + "acl";
  /**
   * Default value for ZooKeeper acl.
   */
  private static final String ZK_ACL_DEFAULT = "world:anyone:rwcda";

  /**
   * Property for setting num of workers.
   */
  private static final String WORKER_NUM_TASKS = "worker.num.tasks";
  /**
   * Default value for num of workers.
   */
  private static final String WORKER_NUM_TASKS_DEFAULT = "10";

  /**
   * Property for setting job builder class.
   */
  public static final String JOB_BUILDER_CLASS = "mawo.job-builder.class";
  /**
   * Default value for job builder class = simpleTaskJobBuilder.
   */
  private static final String JOB_BUILDER_CLASS_DEFAULT =
      "org.apache.hadoop.applications.mawo.server.master.job."
          + "SimpleTaskJobBuilder";

  /**
   * Property for setting worker workspace.
   */
  private static final String WORKER_WORK_SPACE = "worker.workspace";
  /**
   * Default value for worker workspace.
   */
  private static final String WORKER_WORK_SPACE_DEFAULT = "/tmp";

  /**
   * Property for resource manager url.
   */
  public static final String CLUSTER_MANAGER_URL = "ycloud.url";
  /**
   * Default value for resource manager url.
   */
  private static final String DEFAULT_CLUSTER_MANAGER_URL = "0.0.0.0:9191";

  /**
   * Property for setting auto shutdown for worker.
   */
  public static final String AUTO_SHUTDOWN_WORKERS =
      "mawo.master.auto-shutdown-workers";
  /**
   * Set auto shutdown of workers to False by default.
   */
  private static final boolean DEFAULT_AUTO_SHUTDOWN_WORKERS = false;

  /**
   * Property for task status log path in master node.
   */
  public static final String MASTER_TASKS_STATUS_LOG_PATH
      = "master.tasks-status.log.path";
  /**
   * Default value for task status log path.
   */
  private static final String MASTER_TASKS_STATUS_LOG_PATH_DEFAULT = "/tmp";

  /**
   * Property for drain event timeout.
   */
  private static final String MASTER_DRAIN_EVENTS_TIMEOUT =
      "master.drain-events.timeout";
  /**
   * Default value for drain event timeout.
   */
  private static final long MASTER_DRAIN_EVENTS_TIMEOUT_DEFAULT = 60000;

  /**
   * Property for worker white list env.
   * This environment variables will be set for all tasks.
   */
  private static final String WORKER_WHITELIST_ENV = "worker.whitelist.env";
  /**
   * Default value for worker white list env.
   */
  private static final String WORKER_WHITELIST_ENV_DEFAULT = "";

  /**
   * Property for teardown worker validity.
   */
  private static final String MASTER_TEARDOWN_WORKER_VALIDITY_INTERVAL_MS =
      "master.teardown-worker.validity-interval.ms";
  /**
   * Default value for teardown worker validity.
   */
  private static final String
      MASTER_TEARDOWN_WORKER_VALIDITY_INTERVAL_MS_DEFAULT = "120000";

  /**
   * Map of MaWo Configs.
   */
  private Map<String, String> configsMap;

  /**
   * Mowo configuration initializer.
   */
  public MawoConfiguration() {
    this(readConfigFile());
  }

  /**
   * Set up MaWo properties.
   * @param properties : Map of properties
   */
  private MawoConfiguration(final Properties properties) {

    configsMap = new HashMap<String, String>();

    configsMap.put(RPC_SERVER_HOSTNAME, properties
        .getProperty(RPC_SERVER_HOSTNAME, RPC_SERVER_HOSTNAME_DEFAULT));
    configsMap.put(RPC_SERVER_PORT,
        properties.getProperty(RPC_SERVER_PORT, RPC_SERVER_PORT_DEFAULT));

    configsMap.put(ZK_ADDRESS,
        properties.getProperty(ZK_ADDRESS, ZK_ADDRESS_DEFAULT));
    configsMap.put(ZK_PARENT_PATH,
        properties.getProperty(ZK_PARENT_PATH, ZK_PARENT_PATH_DEFAULT));
    configsMap.put(ZK_RETRY_INTERVAL_MS, properties
        .getProperty(ZK_RETRY_INTERVAL_MS, ZK_RETRY_INTERVAL_MS_DEFAULT));
    configsMap.put(ZK_SESSION_TIMEOUT_MS, properties
        .getProperty(ZK_SESSION_TIMEOUT_MS, ZK_SESSION_TIMEOUT_MS_DEFAULT));
    configsMap.put(ZK_RETRIES_NUM,
        properties.getProperty(ZK_RETRIES_NUM, ZK_RETRIES_NUM_DEFAULT));
    configsMap.put(ZK_ACL, properties.getProperty(ZK_ACL, ZK_ACL_DEFAULT));

    configsMap.put(JOB_BUILDER_CLASS,
        properties.getProperty(JOB_BUILDER_CLASS, JOB_BUILDER_CLASS_DEFAULT));

    configsMap.put(JOB_QUEUE_STORAGE_ENABLED,
        properties.getProperty(JOB_QUEUE_STORAGE_ENABLED, "false"));

    configsMap.put(CLUSTER_MANAGER_URL, properties
        .getProperty(CLUSTER_MANAGER_URL, DEFAULT_CLUSTER_MANAGER_URL));

    configsMap.put(WORKER_NUM_TASKS,
        properties.getProperty(WORKER_NUM_TASKS, WORKER_NUM_TASKS_DEFAULT));

    configsMap.put(WORKER_WORK_SPACE,
        properties.getProperty(WORKER_WORK_SPACE, WORKER_WORK_SPACE_DEFAULT));

    configsMap.put(AUTO_SHUTDOWN_WORKERS, properties.getProperty(
        AUTO_SHUTDOWN_WORKERS, String.valueOf(DEFAULT_AUTO_SHUTDOWN_WORKERS)));

    configsMap.put(MASTER_TASKS_STATUS_LOG_PATH, properties.getProperty(
        MASTER_TASKS_STATUS_LOG_PATH,
        String.valueOf(MASTER_TASKS_STATUS_LOG_PATH_DEFAULT)));

    configsMap.put(MASTER_DRAIN_EVENTS_TIMEOUT,
        properties.getProperty(MASTER_DRAIN_EVENTS_TIMEOUT,
        String.valueOf(MASTER_DRAIN_EVENTS_TIMEOUT_DEFAULT)));

    configsMap.put(WORKER_WHITELIST_ENV, properties.getProperty(
        WORKER_WHITELIST_ENV, WORKER_WHITELIST_ENV_DEFAULT));

    configsMap.put(MASTER_TEARDOWN_WORKER_VALIDITY_INTERVAL_MS,
        properties.getProperty(MASTER_TEARDOWN_WORKER_VALIDITY_INTERVAL_MS,
            MASTER_TEARDOWN_WORKER_VALIDITY_INTERVAL_MS_DEFAULT));

  }

  /**
   * Get MaWo config map.
   * @return the config map for MaWo properties
   */

  public Map<String, String> getConfigsMap() {
    return configsMap;
  }

  /**
   * Find, read, and parse the configuration file.
   *
   * @return the properties that were found or empty if no file was found
   */
  private static Properties readConfigFile() {
    Properties properties = new Properties();

    // Get property file stream from classpath
    LOG.info("Configuration file being loaded: " + CONFIG_FILE
        + ". Found in classpath at "
        + MawoConfiguration.class.getClassLoader().getResource(CONFIG_FILE));
    InputStream inputStream = MawoConfiguration.class.getClassLoader()
        .getResourceAsStream(CONFIG_FILE);

    if (inputStream == null) {
      throw new RuntimeException(CONFIG_FILE + " not found in classpath");
    }

    // load the properties
    try {
      properties.load(inputStream);
      inputStream.close();
    } catch (FileNotFoundException fnf) {
      LOG.error(
          "No configuration file " + CONFIG_FILE + " found in classpath.");
    } catch (IOException ie) {
      throw new IllegalArgumentException(
          "Can't read configuration file " + CONFIG_FILE, ie);
    }

    return properties;
  }

  /**
   * Get MaWo RPC server Port.
   * @return value of rpc.server.port
   */
  public int getRpcServerPort() {
    return Integer.parseInt(configsMap.get(RPC_SERVER_PORT));
  }

  /**
   * Get RPC Host map.
   * @return value of rpc.server.hostname
   */
  public String getRpcHostName() {
    return configsMap.get(RPC_SERVER_HOSTNAME);
  }

  /**
   * Check if Job Queue Storage is Enabled.
   * @return True if Job queue storage is enabled otherwise False
   */
  public boolean getJobQueueStorageEnabled() {
    return Boolean.parseBoolean(configsMap.get(JOB_QUEUE_STORAGE_ENABLED));
  }

  /**
   * Get ZooKeeper Address.
   * @return value of ZooKeeper.address
   */
  public String getZKAddress() {
    return configsMap.get(ZK_ADDRESS);
  }

  /**
   * Get ZooKeeper parent Path.
   * @return value of ZooKeeper.parent.path
   */
  public String getZKParentPath() {
    return configsMap.get(ZK_PARENT_PATH);
  }

  /**
   * Get ZooKeeper retry interval value in milli seconds.
   * @return value of ZooKeeper.retry.interval.ms
   */
  public int getZKRetryIntervalMS() {
    return Integer.parseInt(configsMap.get(ZK_RETRY_INTERVAL_MS));
  }

  /**
   * Get ZooKeeper session timeout in milli seconds.
   * @return value of ZooKeeper.session.timeout.ms
   */
  public int getZKSessionTimeoutMS() {
    return Integer.parseInt(configsMap.get(ZK_SESSION_TIMEOUT_MS));
  }

  /**
   * Get ZooKeeper retries number.
   * @return value of ZooKeeper.retries.num
   */
  public int getZKRetriesNum() {
    return Integer.parseInt(configsMap.get(ZK_RETRIES_NUM));
  }

  /**
   * Get ZooKeeper Acls.
   * @return value of ZooKeeper.acl
   */
  public String getZKAcl() {
    return configsMap.get(ZK_ACL);
  }

  /**
   * Get number of tasks a worker can run in parallel.
   * @return value of worker.num.tasks
   */
  public int getWorkerConcurrentTasksLimit() {
    return Integer.parseInt(configsMap.get(WORKER_NUM_TASKS));
  }

  /**
   * Get job builder class.
   * @return value of mawo.job-builder.class
   */
  public String getJobBuilderClass() {
    return configsMap.get(JOB_BUILDER_CLASS);
  }

  /**
   * Get worker work space.
   * @return value of worker.workspace
   */
  public String getWorkerWorkSpace() {
    return configsMap.get(WORKER_WORK_SPACE);
  }

  /**
   * Get cluster manager URL.
   * @return value of ycloud.url
   */
  public String getClusterManagerURL() {
    return configsMap.get(CLUSTER_MANAGER_URL);
  }

  /**
   * Check if worker auto shutdown feature is enabled.
   * @return value of mawo.master.auto-shutdown-workers
   */
  public boolean getAutoShutdownWorkers() {
    return Boolean.parseBoolean(configsMap.get(AUTO_SHUTDOWN_WORKERS));
  }

  /**
   * Get Task status log file path on master host.
   * @return value of master.tasks-status.log.path
   */
  public String getMasterTasksStatusLogPath() {
    return configsMap.get(MASTER_TASKS_STATUS_LOG_PATH);
  }

  /**
   * Get Master drain event timeout.
   * @return value of master.drain-events.timeout
   */
  public long getMasterDrainEventsTimeout() {
    return Long.parseLong(configsMap.get(MASTER_DRAIN_EVENTS_TIMEOUT));
  }

  /**
   * Get Worker whitelist env params.
   * These params will be set in all tasks.
   * @return list of white list environment
   */
  public List<String> getWorkerWhiteListEnv() {
    List<String> whiteList = new ArrayList<String>();
    String env = configsMap.get(WORKER_WHITELIST_ENV);
    if (env != null && !env.isEmpty()) {
      String[] variables = env.split(COMMA_SPLITTER);
      for (String variable : variables) {
        variable = variable.trim();
        if (variable.startsWith("$")) {
          variable = variable.substring(1);
        }
        if (!variable.isEmpty()) {
          whiteList.add(variable);
        }
      }
    }
    return whiteList;
  }

  /**
   * Get Teardown worker validity interval.
   * @return value of master.teardown-worker.validity-interval.ms
   */
  public long getTeardownWorkerValidityInterval() {
    return Long.parseLong(configsMap.get(
        MASTER_TEARDOWN_WORKER_VALIDITY_INTERVAL_MS));
  }
}
