/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * Connectors package is a set of logical connectors that connect
 * to various data sources to read the hadoop cluster information.
 *
 * We currently have 3 connectors in this package. They are
 *
 * DBNameNodeConnector - This uses the connector from the original
 * balancer package to connect to a real hadoop cluster.
 *
 * JsonNodeConnector - This connects to a file and reads the data about a
 * cluster. We can generate a cluster json from a real cluster using
 * the diskBalancer tool or hand-craft it. There are some sample Json files
 * checked in under test/resources/diskBalancer directory.
 *
 * NullConnector - This is an in-memory connector that is useful in testing.
 * we can crate dataNodes on the fly and attach to this connector and
 * ask the diskBalancer Cluster to read data from this source.
 */
package org.apache.hadoop.hdfs.server.diskbalancer.connectors;
