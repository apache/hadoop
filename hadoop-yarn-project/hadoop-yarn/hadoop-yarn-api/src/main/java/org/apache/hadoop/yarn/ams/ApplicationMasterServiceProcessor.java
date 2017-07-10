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

package org.apache.hadoop.yarn.ams;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords
    .FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords
    .RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Interface to abstract out the the actual processing logic of the
 * Application Master Service.
 */
public interface ApplicationMasterServiceProcessor {

  /**
   * Register AM attempt.
   * @param applicationAttemptId applicationAttemptId.
   * @param request Register Request.
   * @return Register Response.
   * @throws IOException IOException.
   */
  RegisterApplicationMasterResponse registerApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      RegisterApplicationMasterRequest request) throws IOException;

  /**
   * Allocate call.
   * @param appAttemptId appAttemptId.
   * @param request Allocate Request.
   * @return Allocate Response.
   * @throws YarnException YarnException.
   */
  AllocateResponse allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request) throws YarnException;

  /**
   * Finish AM.
   * @param applicationAttemptId applicationAttemptId.
   * @param request Finish AM Request.
   * @return Finish AM response.
   */
  FinishApplicationMasterResponse finishApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      FinishApplicationMasterRequest request);

}
