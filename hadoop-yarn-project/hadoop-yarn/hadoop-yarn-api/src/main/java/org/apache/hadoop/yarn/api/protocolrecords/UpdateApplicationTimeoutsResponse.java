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

package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The response sent by the <code>ResourceManager</code> to the client on update
 * application timeout.
 * </p>
 * <p>
 * A response without exception means that the update has completed
 * successfully.
 * </p>
 */
@Public
@Unstable
public abstract class UpdateApplicationTimeoutsResponse {

  public static UpdateApplicationTimeoutsResponse newInstance() {
    UpdateApplicationTimeoutsResponse response =
        Records.newRecord(UpdateApplicationTimeoutsResponse.class);
    return response;
  }

  /**
   * Get <code>ApplicationTimeouts</code> of the application. Timeout value is
   * in ISO8601 standard with format <b>yyyy-MM-dd'T'HH:mm:ss.SSSZ</b>.
   * @return all <code>ApplicationTimeouts</code> of the application.
   */
  public abstract Map<ApplicationTimeoutType, String> getApplicationTimeouts();

  /**
   * Set the <code>ApplicationTimeouts</code> for the application. Timeout value
   * is absolute. Timeout value should meet ISO8601 format. Support ISO8601
   * format is <b>yyyy-MM-dd'T'HH:mm:ss.SSSZ</b>. All pre-existing Map entries
   * are cleared before adding the new Map.
   * @param applicationTimeouts <code>ApplicationTimeouts</code>s for the
   *          application
   */
  public abstract void setApplicationTimeouts(
      Map<ApplicationTimeoutType, String> applicationTimeouts);
}
