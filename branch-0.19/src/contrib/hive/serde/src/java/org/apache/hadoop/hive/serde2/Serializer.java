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

package org.apache.hadoop.hive.serde2;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import java.util.Properties;

/**
 * HiveSerializer is used to serialize data to a Hadoop Writable object.
 * The serialize 
 * In addition to the interface below, all implementations are assume to have a ctor
 * that takes a single 'Table' object as argument.
 *
 */
public interface Serializer {

  /**
   * Initialize the HiveSerializer.
   * @param conf System properties
   * @param tbl  table properties
   * @throws SerDeException
   */
  public void initialize(Configuration conf, Properties tbl) throws SerDeException;
  
  /**
   * Returns the Writable class that would be returned by the serialize method.
   * This is used to initialize SequenceFile header.
   */
  public Class<? extends Writable> getSerializedClass();
  /**
   * Serialize an object by navigating inside the Object with the ObjectInspector.
   * In most cases, the return value of this function will be constant since the function
   * will reuse the Writable object.
   * If the client wants to keep a copy of the Writable, the client needs to clone the
   * returned value.
   */
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException;

}
