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

package org.apache.hadoop.registry.client.api;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.service.Service;

import java.io.IOException;

/**
 * DNS Operations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface DNSOperations extends Service {

  /**
   * Register a service based on a service record.
   *
   * @param path the ZK path.
   * @param record record providing DNS registration info.
   * @throws IOException Any other IO Exception.
   */
  void register(String path, ServiceRecord record)
      throws IOException;


  /**
   * Delete a service's registered endpoints.
   *
   * If the operation returns without an error then the entry has been
   * deleted.
   *
   * @param path the ZK path.
   * @param record service record
   * @throws IOException Any other IO Exception
   *
   */
  void delete(String path, ServiceRecord record)
      throws IOException;

}
