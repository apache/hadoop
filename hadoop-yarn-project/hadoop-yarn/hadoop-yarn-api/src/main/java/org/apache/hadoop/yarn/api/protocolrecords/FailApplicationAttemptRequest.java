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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request sent by the client to the <code>ResourceManager</code>
 * to fail an application attempt.</p>
 *
 * <p>The request includes the {@link ApplicationAttemptId} of the attempt to
 * be failed.</p>
 *
 * @see ApplicationClientProtocol#failApplicationAttempt(FailApplicationAttemptRequest)
 */
@Public
@Stable
public abstract class FailApplicationAttemptRequest {

  @Public
  @Stable
  public static FailApplicationAttemptRequest newInstance(
      ApplicationAttemptId attemptId) {
    FailApplicationAttemptRequest request =
        Records.newRecord(FailApplicationAttemptRequest.class);
    request.setApplicationAttemptId(attemptId);
    return request;
  }

  /**
   * Get the <code>ApplicationAttemptId</code> of the attempt to be failed.
   * @return <code>ApplicationAttemptId</code> of the attempt.
   */
  @Public
  @Stable
  public abstract ApplicationAttemptId getApplicationAttemptId();

  @Public
  @Stable
  public abstract void setApplicationAttemptId(
      ApplicationAttemptId applicationAttemptId);
}
