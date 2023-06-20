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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A default implementation of {@link YarnConfigurationStore}. Doesn't offer
 * persistent configuration storage, just stores the configuration in memory.
 */
public class InMemoryConfigurationStore extends YarnConfigurationStore {

  private Configuration schedConf;
  private long configVersion;

  @Override
  public void initialize(Configuration conf, Configuration schedConf,
      RMContext rmContext) {
    this.schedConf = schedConf;
    this.configVersion = 1L;
  }

  /**
   * This method does not log as it does not support backing store.
   * The mutation to be applied on top of schedConf will be directly passed
   * in confirmMutation.
   */
  @Override
  public void logMutation(LogMutation logMutation) {
  }

  @Override
  public void confirmMutation(LogMutation pendingMutation, boolean isValid) {
    if (isValid) {
      for (Map.Entry<String, String> kv : pendingMutation.getUpdates()
          .entrySet()) {
        if (kv.getValue() == null) {
          schedConf.unset(kv.getKey());
        } else {
          schedConf.set(kv.getKey(), kv.getValue());
        }
      }
      this.configVersion = this.configVersion + 1L;
    }
  }

  @Override
  public void format() {
    this.schedConf = null;
  }

  @Override
  public synchronized Configuration retrieve() {
    return schedConf;
  }

  @Override
  public long getConfigVersion() {
    return configVersion;
  }

  /**
   * Configuration mutations not logged (i.e. not persisted) but directly
   * confirmed. As such, a list of persisted configuration mutations does not
   * exist.
   * @return null Configuration mutation list not applicable for this store.
   */
  @Override
  public List<LogMutation> getConfirmedConfHistory(long fromId) {
    // Unimplemented.
    return null;
  }

  /**
   * Configuration mutations not logged (i.e. not persisted) but directly
   * confirmed. As such, a list of persisted configuration mutations does not
   * exist.
   * @return null Configuration mutation list not applicable for this store.
   */
  @Override
  protected LinkedList<LogMutation> getLogs() {
    // Unimplemented.
    return null;
  }

  /**
   * Configuration mutations applied directly in-memory. As such, there is no
   * persistent configuration store.
   * As there is no configuration store for versioning purposes,
   * a conf store version is not applicable.
   * @return null Conf store version not applicable for this store.
   * @throws Exception if any exception occurs during getConfStoreVersion.
   */
  @Override
  public Version getConfStoreVersion() throws Exception {
    // Does nothing.
    return null;
  }

  /**
   * Configuration mutations not logged (i.e. not persisted). As such, they are
   * not persisted and not versioned. Hence, no version information to store.
   * @throws Exception if any exception occurs during store Version.
   */
  @Override
  public void storeVersion() throws Exception {
    // Does nothing.
  }

  /**
   * Configuration mutations not logged (i.e. not persisted). As such, they are
   * not persisted and not versioned. Hence, a current version is not
   * applicable.
   * @return null A current version not applicable for this store.
   */
  @Override
  public Version getCurrentVersion() {
    // Does nothing.
    return null;
  }

  /**
   * Configuration mutations not logged (i.e. not persisted). As such, they are
   * not persisted and not versioned. Hence, version is always compatible,
   * since it is in-memory.
   */
  @Override
  public void checkVersion() {
    // Does nothing. (Version is always compatible since it's in memory)
  }

  @Override
  public void close() throws IOException {
    // Does nothing.
  }
}
