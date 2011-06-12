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
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.TaskTracker;

/**
 * Place holder for cluster level configuration keys.
 * 
 * These keys are used by both {@link JobTracker} and {@link TaskTracker}. The 
 * keys should have "mapreduce.cluster." as the prefix. 
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
  static final String JOB_LEVEL_AUTHORIZATION_ENABLING_FLAG = 
    "mapreduce.cluster.job-authorization-enabled";
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
}
