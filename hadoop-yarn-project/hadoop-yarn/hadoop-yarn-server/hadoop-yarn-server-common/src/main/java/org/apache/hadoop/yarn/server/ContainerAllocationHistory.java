/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server;

import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.scheduler.ResourceRequestSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Records the allocation history from YarnRM and provide aggregated insights.
 */
public class ContainerAllocationHistory {
  private static final Logger LOG = LoggerFactory.getLogger(AMRMClientRelayer.class);

  private int maxEntryCount;

  // Allocate timing history <AllocateTimeStamp, AllocateLatency>
  private Queue<Entry<Long, Long>> relaxableG = new LinkedList<>();

  public ContainerAllocationHistory(Configuration conf) {
    this.maxEntryCount = conf.getInt(
        YarnConfiguration.FEDERATION_ALLOCATION_HISTORY_MAX_ENTRY,
        YarnConfiguration.DEFAULT_FEDERATION_ALLOCATION_HISTORY_MAX_ENTRY);
  }

  /**
   * Record the allocation history for the container.
   *
   * @param container to add record for
   * @param requestSet resource request ask set
   * @param fulfillTimeStamp time at which allocation happened
   * @param fulfillLatency time elapsed in allocating since asked
   */
  public synchronized void addAllocationEntry(Container container,
      ResourceRequestSet requestSet, long fulfillTimeStamp, long fulfillLatency){
    if (!requestSet.isANYRelaxable()) {
      LOG.info("allocation history ignoring {}, relax locality is false", container);
      return;
    }
    this.relaxableG.add(new AbstractMap.SimpleEntry<>(
        fulfillTimeStamp, fulfillLatency));
    if (this.relaxableG.size() > this.maxEntryCount) {
      this.relaxableG.remove();
    }
  }
}
