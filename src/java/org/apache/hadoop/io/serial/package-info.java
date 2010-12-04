/*
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

/**
 * This package provides a generic interface to multiple serialization
 * frameworks. The propoeraty "hadoop.serializations" defines a list of
 * {@link org.apache.hadoop.io.serial.Serialization} objects. Each 
 * serialization has a name and associated metadata, which is interpreted by 
 * that serialization.
 * <p>
 * The system is pluggable, but the currently supported frameworks are:
 * <ul>
 *   <li> Writable - the traditional Hadoop serialization
 *   <li> Protocol Buffers
 *   <li> Thrift
 *   <li> Avro
 *   <li> Java serialization - not recommended for real work loads
 * </ul>
 *
 * The {@link org.apache.hadoop.io.serial.SerializationFactory} provides 
 * accessors for finding Serializations either by name or by type. 
 * Serializations associated with a set of types extend 
 * {@link org.apache.hadoop.io.serial.TypedSerialization} and can determine 
 * whether they can accept a given type. They are the default serialization
 * for the types they accept.
 * <p>
 *
 * To add a new serialization framework write an implementation of
 * Serialization and add its name to  the "hadoop.serializations" property.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
package org.apache.hadoop.io.serial;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
