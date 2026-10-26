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
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Place holder for cluster level configuration keys.
 * 
 * The keys should have "mapreduce.cluster." as the prefix. 
 *
 */
@InterfaceAudience.Private
public interface MRConfig {

  // Cluster-level configuration parameters
  public static final String TEMP_DIR = "mapreduce.cluster.temp.dir";
  public static final String LOCAL_DIR = "mapreduce.cluster.local.dir";
  public static final String MAPMEMORY_MB = "mapreduce.cluster.mapmemory.mb";
  public static final String REDUCEMEMORY_MB = 
    "mapreduce.cluster.reducememory.mb";
  public static final String MR_ACLS_ENABLED = "mapreduce.cluster.acls.enabled";
  public static final String MR_ADMINS =
    "mapreduce.cluster.administrators";
  @Deprecated
  public static final String MR_SUPERGROUP =
    "mapreduce.cluster.permissions.supergroup";

  //Delegation token related keys
  public static final String  DELEGATION_KEY_UPDATE_INTERVAL_KEY = 
    "mapreduce.cluster.delegation.key.update-interval";
  public static final long    DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT = 
    24*60*60*1000; // 1 day
  public static final String  DELEGATION_TOKEN_RENEW_INTERVAL_KEY = 
    "mapreduce.cluster.delegation.token.renew-interval";
  public static final long    DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT = 
    24*60*60*1000;  // 1 day
  public static final String  DELEGATION_TOKEN_MAX_LIFETIME_KEY = 
    "mapreduce.cluster.delegation.token.max-lifetime";
  public static final long    DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT = 
    7*24*60*60*1000; // 7 days
  
  public static final String RESOURCE_CALCULATOR_PROCESS_TREE =
    "mapreduce.job.process-tree.class";
  public static final String STATIC_RESOLUTIONS = 
    "mapreduce.job.net.static.resolutions";

  public static final String MASTER_ADDRESS  = "mapreduce.jobtracker.address";
  public static final String MASTER_USER_NAME = 
    "mapreduce.jobtracker.kerberos.principal";

  public static final String FRAMEWORK_NAME  = "mapreduce.framework.name";
  public static final String CLASSIC_FRAMEWORK_NAME  = "classic";
  public static final String YARN_FRAMEWORK_NAME  = "yarn";
  public static final String LOCAL_FRAMEWORK_NAME = "local";

  public static final String TASK_LOCAL_OUTPUT_CLASS =
  "mapreduce.task.local.output.class";

  public static final String PROGRESS_STATUS_LEN_LIMIT_KEY =
    "mapreduce.task.max.status.length";
  public static final int PROGRESS_STATUS_LEN_LIMIT_DEFAULT = 512;

  public static final int MAX_BLOCK_LOCATIONS_DEFAULT = 15;
  public static final String MAX_BLOCK_LOCATIONS_KEY =
    "mapreduce.job.max.split.locations";

  public static final String SHUFFLE_SSL_ENABLED_KEY =
    "mapreduce.shuffle.ssl.enabled";

  public static final boolean SHUFFLE_SSL_ENABLED_DEFAULT = false;

  public static final String SHUFFLE_CONSUMER_PLUGIN =
    "mapreduce.job.reduce.shuffle.consumer.plugin.class";

  /**
   * Configuration key to enable/disable IFile readahead.
   */
  public static final String MAPRED_IFILE_READAHEAD =
    "mapreduce.ifile.readahead";

  public static final boolean DEFAULT_MAPRED_IFILE_READAHEAD = true;

  /**
   * Configuration key to set the IFile readahead length in bytes.
   */
  public static final String MAPRED_IFILE_READAHEAD_BYTES =
    "mapreduce.ifile.readahead.bytes";

  public static final int DEFAULT_MAPRED_IFILE_READAHEAD_BYTES =
    4 * 1024 * 1024;

  /**
   * Whether users are explicitly trying to control resource monitoring
   * configuration for the MiniMRCluster. Disabled by default.
   */
  public static final String MAPREDUCE_MINICLUSTER_CONTROL_RESOURCE_MONITORING
      = "mapreduce.minicluster.control-resource-monitoring";
  public static final boolean
      DEFAULT_MAPREDUCE_MINICLUSTER_CONTROL_RESOURCE_MONITORING = false;

  @Public
  @Unstable
  public static final String MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM =
      "mapreduce.app-submission.cross-platform";
  @Public
  @Unstable
  public static final boolean DEFAULT_MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM =
      false;

  /**
   * Enable application master webapp ui actions.
   */
  String MASTER_WEBAPP_UI_ACTIONS_ENABLED =
      "mapreduce.webapp.ui-actions.enabled";
  boolean DEFAULT_MASTER_WEBAPP_UI_ACTIONS_ENABLED = true;
  String MULTIPLE_OUTPUTS_CLOSE_THREAD_COUNT = "mapreduce.multiple-outputs-close-threads";
  int DEFAULT_MULTIPLE_OUTPUTS_CLOSE_THREAD_COUNT = 10;

  /**
   * Enables MapReduce Task-Level Security Enforcement.
   * <p>
   * When enabled, the Application Master performs validation of user-submitted
   * mapper, reducer, and other task-related classes before launching containers.
   * This mechanism protects the cluster from running disallowed or unsafe task
   * implementations as defined by administrator-controlled policies.
   * </p>
   * Property type: boolean
   * Default: {@value #DEFAULT_SECURITY_ENABLED}
   * Value: {@value}
   */
  String MAPREDUCE_TASK_SECURITY_ENABLED = "mapreduce.security.enabled";
  boolean DEFAULT_SECURITY_ENABLED = false;

  /**
   * MapReduce Task-Level Security Enforcement: Property Domain
   * <p>
   * Defines the set of MapReduce configuration keys that represent user-supplied
   * class names involved in task execution (e.g., mapper, reducer, partitioner).
   * The Application Master examines the values of these properties and checks
   * whether any referenced class is listed in {@link #SECURITY_DENIED_TASKS}.
   * Administrators may override this list to expand or restrict the validation
   * domain.
   * </p>
   * Property type: list of configuration keys
   * Default: all known task-level class properties (see list below)
   * Value: {@value}
   */
  String SECURITY_PROPERTY_DOMAIN = "mapreduce.security.property-domain";
  String[] DEFAULT_SECURITY_PROPERTY_DOMAIN = {
      "mapreduce.job.combine.class",
      "mapreduce.job.combiner.group.comparator.class",
      "mapreduce.job.end-notification.custom-notifier-class",
      "mapreduce.job.inputformat.class",
      "mapreduce.job.map.class",
      "mapreduce.job.map.output.collector.class",
      "mapreduce.job.output.group.comparator.class",
      "mapreduce.job.output.key.class",
      "mapreduce.job.output.key.comparator.class",
      "mapreduce.job.output.value.class",
      "mapreduce.job.outputformat.class",
      "mapreduce.job.partitioner.class",
      "mapreduce.job.reduce.class",
      "mapreduce.map.output.key.class",
      "mapreduce.map.output.value.class",
      "mapreduce.outputcommitter.factory.scheme.s3a",
      "mapreduce.outputcommitter.factory.scheme.abfs",
      "mapreduce.outputcommitter.factory.scheme.gs",
      "mapreduce.outputcommitter.factory.scheme.hdfs",
      "mapreduce.outputcommitter.named.classname",
      "mapred.mapper.class",
      "mapred.map.runner.class",
      "mapred.reducer.class"
  };

  /**
   * MapReduce Task-Level Security Enforcement: Denied Tasks
   * <p>
   * Specifies the list of disallowed task implementation classes or packages.
   * If a user submits a job whose mapper, reducer, or other task-related classes
   * match any entry in this blacklist.
   * </p>
   * Property type: list of class name or package patterns
   * Default: empty (no restrictions)
   * Example: org.apache.hadoop.streaming,org.apache.hadoop.examples.QuasiMonteCarlo
   * Value: {@value}
   */
  String SECURITY_DENIED_TASKS = "mapreduce.security.denied-tasks";
  String[] DEFAULT_SECURITY_DENIED_TASKS = {};

  /**
   * MapReduce Task-Level Security Enforcement: Allowed Users
   * <p>
   * Specifies users who may bypass the blacklist defined in
   * {@link #SECURITY_DENIED_TASKS}.
   * This whitelist is intended for trusted or system-level workflows that may
   * legitimately require the use of restricted task implementations.
   * If the submitting user is listed here, blacklist enforcement is skipped,
   * although standard Hadoop authentication and ACL checks still apply.
   * </p>
   * Property type: list of usernames
   * Default: empty (no bypass users)
   * Example: hue,hive
   * Value: {@value}
   */
  String SECURITY_ALLOWED_USERS = "mapreduce.security.allowed-users";
  String[] DEFAULT_SECURITY_ALLOWED_USERS = {};
}
  
