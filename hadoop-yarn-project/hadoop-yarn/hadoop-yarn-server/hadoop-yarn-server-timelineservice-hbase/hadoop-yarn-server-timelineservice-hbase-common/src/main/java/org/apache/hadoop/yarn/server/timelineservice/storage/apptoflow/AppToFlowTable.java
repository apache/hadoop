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

package org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;

/**
 * The app_flow table as column families mapping. Mapping stores
 * appId to flowName and flowRunId mapping information
 *
 * Example app_flow table record:
 *
 * <pre>
 * |--------------------------------------|
 * |  Row       | Column Family           |
 * |  key       | mapping                 |
 * |--------------------------------------|
 * | appId      | flow_name!cluster1:     |
 * |            | foo@daily_hive_report   |
 * |            |                         |
 * |            | flow_run_id!cluster1:   |
 * |            | 1452828720457           |
 * |            |                         |
 * |            | user_id!cluster1:       |
 * |            | admin                   |
 * |            |                         |
 * |            | flow_name!cluster2:     |
 * |            | bar@ad_hoc_query        |
 * |            |                         |
 * |            | flow_run_id!cluster2:   |
 * |            | 1452828498752           |
 * |            |                         |
 * |            | user_id!cluster2:       |
 * |            | joe                     |
 * |            |                         |
 * |--------------------------------------|
 * </pre>
 *
 * It is possible (although unlikely) in a multi-cluster environment that there
 * may be more than one applications for a given app id. Different clusters are
 * recorded as different sets of columns.
 */
public final class AppToFlowTable extends BaseTable<AppToFlowTable> {
}
