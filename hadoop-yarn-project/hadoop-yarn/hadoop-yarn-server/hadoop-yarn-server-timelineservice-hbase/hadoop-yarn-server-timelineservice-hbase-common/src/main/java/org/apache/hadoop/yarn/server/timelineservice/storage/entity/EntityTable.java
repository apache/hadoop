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

package org.apache.hadoop.yarn.server.timelineservice.storage.entity;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;

/**
 * The entity table as column families info, config and metrics. Info stores
 * information about a timeline entity object config stores configuration data
 * of a timeline entity object metrics stores the metrics of a timeline entity
 * object
 *
 * Example entity table record:
 *
 * <pre>
 * |-------------------------------------------------------------------------|
 * |  Row       | Column Family                | Column Family| Column Family|
 * |  key       | info                         | metrics      | config       |
 * |-------------------------------------------------------------------------|
 * | userName!  | id:entityId                  | metricId1:   | configKey1:  |
 * | clusterId! |                              | metricValue1 | configValue1 |
 * | flowName!  | type:entityType              | @timestamp1  |              |
 * | flowRunId! |                              |              | configKey2:  |
 * | AppId!     | created_time:                | metricId1:   | configValue2 |
 * | entityType!| 1392993084018                | metricValue2 |              |
 * | idPrefix!  |                              | @timestamp2  |              |
 * | entityId   | i!infoKey:                   |              |              |
 * |            | infoValue                    | metricId1:   |              |
 * |            |                              | metricValue1 |              |
 * |            | r!relatesToKey:              | @timestamp2  |              |
 * |            | id3=id4=id5                  |              |              |
 * |            |                              |              |              |
 * |            | s!isRelatedToKey             |              |              |
 * |            | id7=id9=id6                  |              |              |
 * |            |                              |              |              |
 * |            | e!eventId=timestamp=infoKey: |              |              |
 * |            | eventInfoValue               |              |              |
 * |            |                              |              |              |
 * |            | flowVersion:                 |              |              |
 * |            | versionValue                 |              |              |
 * |-------------------------------------------------------------------------|
 * </pre>
 */
public final class EntityTable extends BaseTable<EntityTable> {
}
