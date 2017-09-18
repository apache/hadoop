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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

import java.util.List;
import java.util.Map;

/**
 * A default implementation of {@link YarnConfigurationStore}. Doesn't offer
 * persistent configuration storage, just stores the configuration in memory.
 */
public class InMemoryConfigurationStore extends YarnConfigurationStore {

  private Configuration schedConf;
  private LogMutation pendingMutation;

  @Override
  public void initialize(Configuration conf, Configuration schedConf,
      RMContext rmContext) {
    this.schedConf = schedConf;
  }

  @Override
  public void logMutation(LogMutation logMutation) {
    pendingMutation = logMutation;
  }

  @Override
  public void confirmMutation(boolean isValid) {
    if (isValid) {
      for (Map.Entry<String, String> kv : pendingMutation.getUpdates()
          .entrySet()) {
        if (kv.getValue() == null) {
          schedConf.unset(kv.getKey());
        } else {
          schedConf.set(kv.getKey(), kv.getValue());
        }
      }
    }
    pendingMutation = null;
  }

  @Override
  public synchronized Configuration retrieve() {
    return schedConf;
  }

  @Override
  public List<LogMutation> getConfirmedConfHistory(long fromId) {
    // Unimplemented.
    return null;
  }

  @Override
  public Version getConfStoreVersion() throws Exception {
    // Does nothing.
    return null;
  }

  @Override
  public void storeVersion() throws Exception {
    // Does nothing.
  }

  @Override
  public Version getCurrentVersion() {
    // Does nothing.
    return null;
  }

  @Override
  public void checkVersion() {
    // Does nothing. (Version is always compatible since it's in memory)
  }
}
