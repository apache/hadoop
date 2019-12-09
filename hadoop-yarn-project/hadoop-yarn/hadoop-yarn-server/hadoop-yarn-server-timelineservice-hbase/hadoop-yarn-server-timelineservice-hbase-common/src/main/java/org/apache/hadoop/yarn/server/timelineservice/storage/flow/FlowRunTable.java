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

package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;

/**
 * The flow run table has column family info
 * Stores per flow run information
 * aggregated across applications.
 *
 * Metrics are also stored in the info column family.
 *
 * Example flow run table record:
 *
 * <pre>
 * flow_run table
 * |-------------------------------------------|
 * |  Row key   | Column Family                |
 * |            | info                         |
 * |-------------------------------------------|
 * | clusterId! | flow_version:version7        |
 * | userName!  |                              |
 * | flowName!  | running_apps:1               |
 * | flowRunId  |                              |
 * |            | min_start_time:1392995080000 |
 * |            | #0:""                        |
 * |            |                              |
 * |            | min_start_time:1392995081012 |
 * |            | #0:appId2                    |
 * |            |                              |
 * |            | min_start_time:1392993083210 |
 * |            | #0:appId3                    |
 * |            |                              |
 * |            |                              |
 * |            | max_end_time:1392993084018   |
 * |            | #0:""                        |
 * |            |                              |
 * |            |                              |
 * |            | m!mapInputRecords:127        |
 * |            | #0:""                        |
 * |            |                              |
 * |            | m!mapInputRecords:31         |
 * |            | #2:appId2                    |
 * |            |                              |
 * |            | m!mapInputRecords:37         |
 * |            | #1:appId3                    |
 * |            |                              |
 * |            |                              |
 * |            | m!mapOutputRecords:181       |
 * |            | #0:""                        |
 * |            |                              |
 * |            | m!mapOutputRecords:37        |
 * |            | #1:appId3                    |
 * |            |                              |
 * |            |                              |
 * |-------------------------------------------|
 * </pre>
 */
public final class FlowRunTable extends BaseTable<FlowRunTable> {
}
