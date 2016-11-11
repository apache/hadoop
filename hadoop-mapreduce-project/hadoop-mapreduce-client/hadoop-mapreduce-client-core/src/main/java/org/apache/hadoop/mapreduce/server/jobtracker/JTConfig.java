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
package org.apache.hadoop.mapreduce.server.jobtracker;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.MRConfig;

/**
 * Place holder for JobTracker server-level configuration.
 * 
 * The keys should have "mapreduce.jobtracker." as the prefix
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface JTConfig extends MRConfig {
  // JobTracker configuration parameters
  public static final String JT_IPC_ADDRESS  = "mapreduce.jobtracker.address";
  public static final String JT_PERSIST_JOBSTATUS =
    "mapreduce.jobtracker.persist.jobstatus.active";

  public static final String JT_RETIREJOBS =
    "mapreduce.jobtracker.retirejobs";
  public static final String JT_TASKCACHE_LEVELS =
    "mapreduce.jobtracker.taskcache.levels";
  public static final String JT_SYSTEM_DIR = "mapreduce.jobtracker.system.dir";
  public static final String JT_STAGING_AREA_ROOT = 
    "mapreduce.jobtracker.staging.root.dir";
  public static final String JT_MAX_MAPMEMORY_MB =
    "mapreduce.jobtracker.maxmapmemory.mb";
  public static final String JT_MAX_REDUCEMEMORY_MB = 
    "mapreduce.jobtracker.maxreducememory.mb";
  public static final String JT_USER_NAME = "mapreduce.jobtracker.kerberos.principal";
}
