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

package org.apache.hadoop.yarn.server.timelineservice.storage.domain;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;

/**
 * The domain table has column family info. Info stores
 * information about a timeline domain object
 *
 * Example domain table record:
 *
 * <pre>
 * |-------------------------------------------|
 * |  Row       | Column Family                |
 * |  key       | info                         |
 * |-------------------------------------------|
 * | clusterId! | created_time:1521676928000   |
 * | domainI    | description: "domain         |
 * |            | information for XYZ job"     |
 * |            | owners: "user1, yarn"        |
 * |            | readers:                     |
 * |            | "user2,user33 yarn,group2"   |
 * |            |                              |
 * |-------------------------------------------|
 * </pre>
 */
public final class DomainTable extends BaseTable<DomainTable> {
}
