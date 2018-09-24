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

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for AMRMClient.
 */
@Private
public final class AMRMClientUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(AMRMClientUtils.class);

  public static final int PRE_REGISTER_RESPONSE_ID = -1;

  public static final String APP_ALREADY_REGISTERED_MESSAGE =
      "Application Master is already registered : ";

  public static final String EXPECTED_HB_RESPONSEID_MESSAGE =
      " expect responseId to be ";
  public static final String RECEIVED_HB_RESPONSEID_MESSAGE = " but get ";

  private AMRMClientUtils() {
  }

  /**
   * Create a proxy for the specified protocol.
   *
   * @param configuration Configuration to generate {@link ClientRMProxy}
   * @param protocol Protocol for the proxy
   * @param user the user on whose behalf the proxy is being created
   * @param token the auth token to use for connection
   * @param <T> Type information of the proxy
   * @return Proxy to the RM
   * @throws IOException on failure
   */
  @Public
  @Unstable
  public static <T> T createRMProxy(final Configuration configuration,
      final Class<T> protocol, UserGroupInformation user,
      final Token<? extends TokenIdentifier> token) throws IOException {
    try {
      String rmClusterId = configuration.get(YarnConfiguration.RM_CLUSTER_ID,
          YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
      LOG.info("Creating RMProxy to RM {} for protocol {} for user {}",
          rmClusterId, protocol.getSimpleName(), user);
      if (token != null) {
        // preserve the token service sent by the RM when adding the token
        // to ensure we replace the previous token setup by the RM.
        // Afterwards we can update the service address for the RPC layer.
        // Same as YarnServerSecurityUtils.updateAMRMToken()
        user.addToken(token);
        token.setService(ClientRMProxy.getAMRMTokenService(configuration));
        setAuthModeInConf(configuration);
      }
      final T proxyConnection = user.doAs(new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          return ClientRMProxy.createRMProxy(configuration, protocol);
        }
      });
      return proxyConnection;

    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  private static void setAuthModeInConf(Configuration conf) {
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());
  }

  /**
   * Generate the exception message when RM receives an AM heartbeat with
   * invalid responseId.
   *
   * @param appAttemptId the app attempt
   * @param expected the expected responseId value
   * @param received the received responseId value
   * @return the assembled exception message
   */
  public static String assembleInvalidResponseIdExceptionMessage(
      ApplicationAttemptId appAttemptId, int expected, int received) {
    return "Invalid responseId in AllocateRequest from application attempt: "
        + appAttemptId + EXPECTED_HB_RESPONSEID_MESSAGE + expected
        + RECEIVED_HB_RESPONSEID_MESSAGE + received;
  }

  /**
   * Parse the expected responseId from the exception generated by RM when
   * processing AM heartbeat.
   *
   * @param exceptionMessage the exception message thrown by RM
   * @return the parsed expected responseId, -1 if failed
   */
  public static int parseExpectedResponseIdFromException(
      String exceptionMessage) {
    if (exceptionMessage == null) {
      return -1;
    }
    int start = exceptionMessage.indexOf(EXPECTED_HB_RESPONSEID_MESSAGE);
    int end = exceptionMessage.indexOf(RECEIVED_HB_RESPONSEID_MESSAGE);
    if (start == -1 || end == -1) {
      return -1;
    }
    start += EXPECTED_HB_RESPONSEID_MESSAGE.length();

    try {
      return Integer.parseInt(exceptionMessage.substring(start, end));
    } catch (NumberFormatException ex) {
      return -1;
    }
  }

  public static int getNextResponseId(int responseId) {
    // Loop between 0 to Integer.MAX_VALUE
    return (responseId + 1) & Integer.MAX_VALUE;
  }

  public static void addToOutstandingSchedulingRequests(
      Collection<SchedulingRequest> requests,
      Map<Set<String>, List<SchedulingRequest>> outstandingSchedRequests) {
    for (SchedulingRequest req : requests) {
      List<SchedulingRequest> schedulingRequests = outstandingSchedRequests
          .computeIfAbsent(req.getAllocationTags(), x -> new LinkedList<>());
      SchedulingRequest matchingReq = null;
      for (SchedulingRequest schedReq : schedulingRequests) {
        if (isMatchingSchedulingRequests(req, schedReq)) {
          matchingReq = schedReq;
          break;
        }
      }
      if (matchingReq != null) {
        matchingReq.getResourceSizing()
            .setNumAllocations(req.getResourceSizing().getNumAllocations());
      } else {
        schedulingRequests.add(req);
      }
    }
  }

  public static boolean isMatchingSchedulingRequests(
      SchedulingRequest schedReq1, SchedulingRequest schedReq2) {
    return schedReq1.getPriority().equals(schedReq2.getPriority()) &&
        schedReq1.getExecutionType().getExecutionType().equals(
            schedReq1.getExecutionType().getExecutionType()) &&
        schedReq1.getAllocationRequestId() ==
            schedReq2.getAllocationRequestId();
  }

  public static void removeFromOutstandingSchedulingRequests(
      Collection<Container> containers,
      Map<Set<String>, List<SchedulingRequest>> outstandingSchedRequests) {
    if (containers == null || containers.isEmpty()) {
      return;
    }
    for (Container container : containers) {
      if (container.getAllocationTags() != null
          && !container.getAllocationTags().isEmpty()) {
        List<SchedulingRequest> schedReqs =
            outstandingSchedRequests.get(container.getAllocationTags());
        if (schedReqs != null && !schedReqs.isEmpty()) {
          Iterator<SchedulingRequest> iter = schedReqs.iterator();
          while (iter.hasNext()) {
            SchedulingRequest schedReq = iter.next();
            if (schedReq.getPriority().equals(container.getPriority())
                && schedReq.getAllocationRequestId() == container
                    .getAllocationRequestId()) {
              int numAllocations =
                  schedReq.getResourceSizing().getNumAllocations();
              numAllocations--;
              if (numAllocations == 0) {
                iter.remove();
              } else {
                schedReq.getResourceSizing().setNumAllocations(numAllocations);
              }
            }
          }
        }
      }
    }
  }
}