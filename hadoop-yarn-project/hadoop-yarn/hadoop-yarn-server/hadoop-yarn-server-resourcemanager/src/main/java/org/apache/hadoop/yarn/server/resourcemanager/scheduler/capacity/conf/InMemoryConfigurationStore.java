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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A default implementation of {@link YarnConfigurationStore}. Doesn't offer
 * persistent configuration storage, just stores the configuration in memory.
 */
public class InMemoryConfigurationStore implements YarnConfigurationStore {

  private Configuration schedConf;
  private LinkedList<LogMutation> pendingMutations;
  private long pendingId;

  @Override
  public void initialize(Configuration conf, Configuration schedConf) {
    this.schedConf = schedConf;
    this.pendingMutations = new LinkedList<>();
    this.pendingId = 0;
  }

  @Override
  public synchronized long logMutation(LogMutation logMutation) {
    logMutation.setId(++pendingId);
    pendingMutations.add(logMutation);
    return pendingId;
  }

  @Override
  public synchronized boolean confirmMutation(long id, boolean isValid) {
    LogMutation mutation = pendingMutations.poll();
    // If confirmMutation is called out of order, discard mutations until id
    // is reached.
    while (mutation != null) {
      if (mutation.getId() == id) {
        if (isValid) {
          Map<String, String> mutations = mutation.getUpdates();
          for (Map.Entry<String, String> kv : mutations.entrySet()) {
            schedConf.set(kv.getKey(), kv.getValue());
          }
        }
        return true;
      }
      mutation = pendingMutations.poll();
    }
    return false;
  }

  @Override
  public synchronized Configuration retrieve() {
    return schedConf;
  }

  @Override
  public synchronized List<LogMutation> getPendingMutations() {
    return pendingMutations;
  }

  @Override
  public List<LogMutation> getConfirmedConfHistory(long fromId) {
    // Unimplemented.
    return null;
  }
}
