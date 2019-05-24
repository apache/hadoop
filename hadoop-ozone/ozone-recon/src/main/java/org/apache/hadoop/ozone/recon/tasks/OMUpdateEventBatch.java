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

package org.apache.hadoop.ozone.recon.tasks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Wrapper class to hold multiple OM DB update events.
 */
public class OMUpdateEventBatch {

  private List<OMDBUpdateEvent> events;

  OMUpdateEventBatch(Collection<OMDBUpdateEvent> e) {
    events = new ArrayList<>(e);
  }

  /**
   * Get Sequence Number and timestamp of last event in this batch.
   * @return Event Info instance.
   */
  OMDBUpdateEvent.EventInfo getLastEventInfo() {
    if (events.isEmpty()) {
      return new OMDBUpdateEvent.EventInfo(-1, -1);
    } else {
      return events.get(events.size() - 1).getEventInfo();
    }
  }

  /**
   * Return iterator to Event batch.
   * @return iterator
   */
  public Iterator<OMDBUpdateEvent> getIterator() {
    return events.iterator();
  }

  /**
   * Filter events based on Tables.
   * @param tables set of tables to filter on.
   * @return trimmed event batch.
   */
  public OMUpdateEventBatch filter(Collection<String> tables) {
    return new OMUpdateEventBatch(events
        .stream()
        .filter(e -> tables.contains(e.getTable()))
        .collect(Collectors.toList()));
  }
}
