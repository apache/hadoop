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
 * The state store uses modular data storage classes derived from
 * StateStoreDriver to handle querying, updating and deleting data records. The
 * data storage driver is initialized and maintained by the StateStoreService.
 * The state store supports fetching all records of a type, filtering by column
 * values or fetching a single record by its primary key.
 * <p>
 * Each data storage backend is required to implement the methods contained in
 * the StateStoreDriver interface. These methods allow the querying, updating,
 * inserting and deleting of data records into the state store.
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving

package org.apache.hadoop.hdfs.server.federation.store.driver;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;