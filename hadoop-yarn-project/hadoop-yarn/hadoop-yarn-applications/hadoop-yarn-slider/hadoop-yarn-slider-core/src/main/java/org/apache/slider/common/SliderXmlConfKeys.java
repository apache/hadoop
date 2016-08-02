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

package org.apache.slider.common;

import org.apache.hadoop.registry.client.api.RegistryConstants;

/**
 * These are the keys that can be added to <code>conf/slider-client.xml</code>.
 */
public interface SliderXmlConfKeys {
  String PREFIX_PROVIDER = "slider.provider";
  /**
   * pattern to identify a provider
   * {@value}
   */
  String KEY_PROVIDER = PREFIX_PROVIDER + ".%s";

  /**
   * conf option set to point to where the config came from
   * {@value}
   */
  String KEY_TEMPLATE_ORIGIN = "slider.template.origin";

  /**
   * Original name for the default FS. This is still 
   * expected by applications deployed
   */
  String FS_DEFAULT_NAME_CLASSIC = "fs.default.name";

  /**
   * Slider principal
   */
  String KEY_KERBEROS_PRINCIPAL = "slider.kerberos.principal";

  /**
   * Name of the property for ACLs for Slider AM.
   * {@value}
   */
  String KEY_PROTOCOL_ACL = "slider.security.protocol.acl";

  /**
   * Limit on restarts for the AM
   * {@value}
   */
  String KEY_AM_RESTART_LIMIT = "slider.yarn.restart.limit";

  /**
   * queue name, by default let YARN pick the queue
   */
  String KEY_YARN_QUEUE = "slider.yarn.queue";
  String DEFAULT_YARN_QUEUE = null;

  /**
   * default priority
   */
  String KEY_YARN_QUEUE_PRIORITY = "slider.yarn.queue.priority";
  int DEFAULT_YARN_QUEUE_PRIORITY = 1;


  /**
   * The slider base path: {@value}
   * Defaults to HomeDir/.slider
   */
  String KEY_SLIDER_BASE_PATH = "slider.base.path";


  /**
   * Option for the permissions for the cluster directory itself: {@value}
   */
  String CLUSTER_DIRECTORY_PERMISSIONS =
    "slider.cluster.directory.permissions";

  /**
   * Default value for the permissions :{@value}
   */
  String DEFAULT_CLUSTER_DIRECTORY_PERMISSIONS = "750";

  /**
   * 
   * Option for the permissions for the data directory itself: {@value}
   */
  String DATA_DIRECTORY_PERMISSIONS = "slider.data.directory.permissions";

  /**
   * Default value for the data directory permissions: {@value}
   */
  String DEFAULT_DATA_DIRECTORY_PERMISSIONS = "750";

  /**
   *
   * Use {@link RegistryConstants#KEY_REGISTRY_ZK_ROOT}
   *
   */
  @Deprecated
  String REGISTRY_PATH = "slider.registry.path";

  /**
   * 
   * @Deprecated use {@link RegistryConstants#KEY_REGISTRY_ZK_QUORUM}
   * 
   */
  @Deprecated
  String REGISTRY_ZK_QUORUM = "slider.zookeeper.quorum";


  String IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH =
      "ipc.client.fallback-to-simple-auth-allowed";
  String HADOOP_HTTP_FILTER_INITIALIZERS =
      "hadoop.http.filter.initializers";
  String KEY_KEYSTORE_LOCATION = "ssl.server.keystore.location";
  String KEY_AM_LOGIN_KEYTAB_NAME = "slider.am.login.keytab.name";
  /** Declare that a keytab must be provided */
  String KEY_AM_LOGIN_KEYTAB_REQUIRED = "slider.am.login.keytab.required";
  String KEY_HDFS_KEYTAB_DIR = "slider.hdfs.keytab.dir";
  String KEY_AM_KEYTAB_LOCAL_PATH = "slider.am.keytab.local.path";
  String KEY_KEYTAB_PRINCIPAL = "slider.keytab.principal.name";
  String KEY_SECURITY_ENABLED = "site.global.security_enabled";

  /**
   * Set to disable server-side checks for python, openssl &c.
   * This should only be set for testing
   */
  String KEY_SLIDER_AM_DEPENDENCY_CHECKS_DISABLED =
      "slider.am.dependency.checks.disabled";

  /**
   * The path to the python executable utilized to launch the agent.
   */
  String PYTHON_EXECUTABLE_PATH = "agent.python.exec.path";

  /**
   * Flag to enable the insecure AM filter: {@value}
   */
  String X_DEV_INSECURE_WS = "slider.feature.ws.insecure";

  /**
   * Flag to indicate the insecure AM filter is enabled by default: {@value}.
   */
  boolean X_DEV_INSECURE_DEFAULT = false;


  /**
   * Flag to indicate the insecure AM filter is required for
   * complex REST Verbs: {@value}.
   * When Slider switches to being Hadoop 2.7+ only, this flag
   * can be set to false
   */
  boolean X_DEV_INSECURE_REQUIRED = true;

  /**
   *
   */
  String KEY_IPC_CLIENT_RETRY_POLICY_ENABLED =
      "slider.ipc.client.retry.enabled";
  boolean IPC_CLIENT_RETRY_POLICY_ENABLED_DEFAULT = true;
  String KEY_IPC_CLIENT_RETRY_POLICY_SPEC =
      "slider.ipc.client.retry.policy.spec";
  String IPC_CLIENT_RETRY_POLICY_SPEC_DEFAULT =
      "10000,6,60000,10"; //t1,n1,t2,n2,... 

  String KEY_AM_LAUNCH_ENV = "slider.am.launch.env";

  /**
   * From {@code DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY}
   */
  String DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY = "dfs.namenode.kerberos.principal";

  String DFS_DATANODE_KERBEROS_PRINCIPAL_KEY = "dfs.datanode.kerberos.principal";

  //Delegation token related keys
  String DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY
      = "dfs.namenode.delegation.key.update-interval";
  long DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT = 24 * 60 * 60 *
      1000; // 1 day
  String DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY
      = "dfs.namenode.delegation.token.renew-interval";
  long DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT = 24 * 60 * 60 *
      1000;  // 1 day
  String DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY
      = "dfs.namenode.delegation.token.max-lifetime";
  long DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT = 7 * 24 * 60 * 60 *
      1000; // 7 days
  String DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY
      = "dfs.namenode.delegation.token.always-use"; // for tests
  boolean DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_DEFAULT = false;
  String DFS_NAMENODE_KEYTAB_FILE_KEY = "dfs.namenode.keytab.file";
  String DFS_NAMENODE_DU_RESERVED_KEY = "dfs.namenode.resource.du.reserved";

  String MAPREDUCE_JOB_CREDENTIALS_BINARY = "mapreduce.job.credentials.binary";
}
