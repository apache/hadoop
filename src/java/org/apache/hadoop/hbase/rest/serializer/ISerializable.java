/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.rest.serializer;

import org.apache.hadoop.hbase.rest.exception.HBaseRestException;

/**
 * 
 *         Interface for objects that wish to write back to the REST based
 *         interface output stream. Objects should implement this interface,
 *         then use the IRestSerializer passed to it to call the appropriate
 *         serialization method.
 */
public interface ISerializable {
  /**
   * visitor pattern method where the object implementing this interface will
   * call back on the IRestSerializer with the correct method to run to
   * serialize the output of the object to the stream.
   * 
   * @param serializer
   * @throws HBaseRestException
   */
  public void restSerialize(IRestSerializer serializer)
      throws HBaseRestException;
}
