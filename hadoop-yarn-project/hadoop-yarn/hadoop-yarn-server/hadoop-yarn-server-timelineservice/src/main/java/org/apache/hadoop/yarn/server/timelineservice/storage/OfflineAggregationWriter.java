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

package org.apache.hadoop.yarn.server.timelineservice.storage;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.OfflineAggregationInfo;

import java.io.IOException;

/**
 * YARN timeline service v2 offline aggregation storage interface.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class OfflineAggregationWriter extends AbstractService {

  /**
   * Construct the offline writer.
   *
   * @param name service name
   */
  public OfflineAggregationWriter(String name) {
    super(name);
  }

  /**
   * Persist aggregated timeline entities to the offline store based on which
   * track this entity is to be rolled up to. The tracks along which
   * aggregations are to be done are given by {@link OfflineAggregationInfo}.
   *
   * @param context a {@link TimelineCollectorContext} object that describes the
   *                context information of the aggregated data. Depends on the
   *                type of the aggregation, some fields of this context maybe
   *                empty or null.
   * @param entities {@link TimelineEntities} to be persisted.
   * @param info an {@link OfflineAggregationInfo} object that describes the
   *             detail of the aggregation. Current supported option is
   *             {@link OfflineAggregationInfo#FLOW_AGGREGATION}.
   * @return a {@link TimelineWriteResponse} object.
   * @throws IOException if any problem occurs while writing aggregated
   *     entities.
   */
  abstract TimelineWriteResponse writeAggregatedEntity(
      TimelineCollectorContext context, TimelineEntities entities,
      OfflineAggregationInfo info) throws IOException;
}
