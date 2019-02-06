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
 * Implementations of state store data providers/drivers. Each driver is
 * responsible for maintaining, querying, updating and deleting persistent data
 * records. Data records are defined as subclasses of
 * {@link org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord}.
 * Each driver implements the
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver
 * StateStoreDriver} interface.
 * <p>
 * Currently supported drivers:
 * <ul>
 * <li>{@link StateStoreFileImpl} A file-based data storage backend.
 * <li>{@link StateStoreZooKeeperImpl} A zookeeper based data storage backend.
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
package org.apache.hadoop.hdfs.server.federation.store.driver.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;