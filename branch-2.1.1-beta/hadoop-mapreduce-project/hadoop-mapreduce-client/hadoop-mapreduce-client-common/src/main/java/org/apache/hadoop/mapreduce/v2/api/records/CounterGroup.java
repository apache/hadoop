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

package org.apache.hadoop.mapreduce.v2.api.records;

import java.util.Map;

public interface CounterGroup {
  public abstract String getName();
  public abstract String getDisplayName();
  
  public abstract Map<String, Counter> getAllCounters();
  public abstract Counter getCounter(String key);
  
  public abstract void setName(String name);
  public abstract void setDisplayName(String displayName);
  
  public abstract void addAllCounters(Map<String, Counter> counters);
  public abstract void setCounter(String key, Counter value);
  public abstract void removeCounter(String key);
  public abstract void clearCounters();
}
