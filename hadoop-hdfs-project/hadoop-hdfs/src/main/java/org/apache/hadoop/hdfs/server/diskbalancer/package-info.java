/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Disk Balancer connects to a {@link org.apache.hadoop.hdfs.server.datanode
 * .DataNode} and attempts to spread data across all volumes evenly.
 *
 * This is achieved by :
 *
 * 1) Calculating the average data that should be on a set of volumes grouped
 * by the type. For example, how much data should be on each volume of SSDs on a
 * machine.
 *
 * 2) Once we know the average data that is expected to be on a volume we
 * move data from volumes with higher than average load to volumes with
 * less than average load.
 *
 * 3) Disk Balancer operates against data nodes which are live and operational.
 *
 */
package org.apache.hadoop.hdfs.server.diskbalancer;