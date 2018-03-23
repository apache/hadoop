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
package org.apache.hadoop.hdfs.server.federation.store.records.impl.pb;

import java.io.IOException;

import com.google.protobuf.Message;

/**
 * A record implementation using Protobuf.
 */
public interface PBRecord {

  /**
   * Get the protocol for the record.
   * @return The protocol for this record.
   */
  Message getProto();

  /**
   * Set the protocol for the record.
   * @param proto Protocol for this record.
   */
  void setProto(Message proto);

  /**
   * Populate this record with serialized data.
   * @param base64String Serialized data in base64.
   * @throws IOException If it cannot read the data.
   */
  void readInstance(String base64String) throws IOException;
}
