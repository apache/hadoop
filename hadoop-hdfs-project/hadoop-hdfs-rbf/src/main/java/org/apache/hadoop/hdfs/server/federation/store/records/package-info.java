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
 * Contains the abstract definitions of the state store data records. The state
 * store supports multiple multiple data records.
 * <p>
 * Data records inherit from a common class
 * {@link org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord
 * BaseRecord}. Data records are serialized when written to the data store using
 * a modular serialization implementation. The default is profobuf
 * serialization. Data is stored as rows of records of the same type with each
 * data member in a record representing a column.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

package org.apache.hadoop.hdfs.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
