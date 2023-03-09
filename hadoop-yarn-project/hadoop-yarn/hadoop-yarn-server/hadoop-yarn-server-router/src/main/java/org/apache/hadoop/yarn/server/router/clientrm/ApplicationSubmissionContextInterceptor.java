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

package org.apache.hadoop.yarn.server.router.clientrm;

import java.io.IOException;

import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.router.RouterAuditLogger;
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;

import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.SUBMIT_NEW_APP;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.TARGET_CLIENT_RM_SERVICE;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UNKNOWN;

/**
 * It prevents DoS attack over the ApplicationClientProtocol. Currently, it
 * checks the size of the ApplicationSubmissionContext. If it exceeds the limit
 * it can cause Zookeeper failures.
 */
public class ApplicationSubmissionContextInterceptor extends PassThroughClientRequestInterceptor {

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException, IOException {

    if (request == null || request.getApplicationSubmissionContext() == null ||
        request.getApplicationSubmissionContext().getApplicationId() == null) {
      RouterMetrics.getMetrics().incrAppsFailedSubmitted();
      String errMsg =
          "Missing submitApplication request or applicationSubmissionContext information.";
      RouterAuditLogger.logFailure(user.getShortUserName(), SUBMIT_NEW_APP, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, errMsg);
      RouterServerUtil.logAndThrowException(errMsg, null);
    }

    ApplicationSubmissionContext appContext = request.getApplicationSubmissionContext();
    ApplicationSubmissionContextPBImpl asc = (ApplicationSubmissionContextPBImpl) appContext;

    // Check for excessively large fields, throw exception if found
    RouterServerUtil.checkAppSubmissionContext(asc, getConf());

    // Check succeeded - app submit will be passed on to the next interceptor
    return getNextInterceptor().submitApplication(request);
  }
}