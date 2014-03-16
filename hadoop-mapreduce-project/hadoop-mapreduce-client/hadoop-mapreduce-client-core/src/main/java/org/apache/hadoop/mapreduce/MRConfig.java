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

  public static final int MAX_BLOCK_LOCATIONS_DEFAULT = 10;
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
}
  
