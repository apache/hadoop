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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.federation.store.protocol.DisableNameserviceRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.DisableNameserviceResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnableNameserviceRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnableNameserviceResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDisabledNameservicesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDisabledNameservicesResponse;

/**
 * Interface for enable/disable name service.
 */
public interface NameserviceManager {

  /**
   * Disable a name service.
   * @param request Request to disable a name service.
   * @return Response to disable a name service.
   * @throws IOException If it cannot perform the operation.
   */
  DisableNameserviceResponse disableNameservice(
      DisableNameserviceRequest request) throws IOException;

  /**
   * Enable a name service.
   * @param request Request to enable a name service.
   * @return Response to disable a name service.
   * @throws IOException If it cannot perform the operation.
   */
  EnableNameserviceResponse enableNameservice(EnableNameserviceRequest request)
      throws IOException;

  /**
   * Get the list of disabled name service.
   * @param request Request to get the disabled name services.
   * @return Response to get the disabled name services.
   * @throws IOException If it cannot perform the operation.
   */
  GetDisabledNameservicesResponse getDisabledNameservices(
      GetDisabledNameservicesRequest request) throws IOException;
}
