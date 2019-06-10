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
package org.apache.hadoop.mapreduce.server.tasktracker;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.MRConfig;

/**
 * Place holder for MapReduce server-level configuration.
 * (formerly TaskTracker configuration)
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface TTConfig extends MRConfig {

  public static final String TT_INDEX_CACHE = 
    "mapreduce.tasktracker.indexcache.mb";
  public static final String TT_MAP_SLOTS = 
    "mapreduce.tasktracker.map.tasks.maximum";
  public static final String TT_RESOURCE_CALCULATOR_PLUGIN = 
    "mapreduce.tasktracker.resourcecalculatorplugin";
}
