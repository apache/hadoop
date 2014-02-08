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

import java.util.EnumSet;
import java.util.Set;

import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request from clients to get a report of Applications
 * in the cluster from the <code>ResourceManager</code>.</p>
 *
 *
 * @see ApplicationClientProtocol#getApplications(GetApplicationsRequest)
 */
@Public
@Stable
public abstract class GetApplicationsRequest {
  @Public
  @Stable
  public static GetApplicationsRequest newInstance() {
    GetApplicationsRequest request =
        Records.newRecord(GetApplicationsRequest.class);
    return request;
  }

  /**
   * <p>
   * The request from clients to get a report of Applications matching the
   * giving application types in the cluster from the
   * <code>ResourceManager</code>.
   * </p>
   *
   * @see ApplicationClientProtocol#getApplications(GetApplicationsRequest)
   *
   * <p>Setting any of the parameters to null, would just disable that
   * filter</p>
   *
   * @param scope {@link ApplicationsRequestScope} to filter by
   * @param users list of users to filter by
   * @param queues list of scheduler queues to filter by
   * @param applicationTypes types of applications
   * @param applicationTags application tags to filter by
   * @param applicationStates application states to filter by
   * @param startRange range of application start times to filter by
   * @param finishRange range of application finish times to filter by
   * @param limit number of applications to limit to
   * @return {@link GetApplicationsRequest} to be used with
   * {@link ApplicationClientProtocol#getApplications(GetApplicationsRequest)}
   */
  @Public
  @Stable
  public static GetApplicationsRequest newInstance(
      ApplicationsRequestScope scope,
      Set<String> users,
      Set<String> queues,
      Set<String> applicationTypes,
      Set<String> applicationTags,
      EnumSet<YarnApplicationState> applicationStates,
      LongRange startRange,
      LongRange finishRange,
      Long limit) {
    GetApplicationsRequest request =
        Records.newRecord(GetApplicationsRequest.class);
    if (scope != null) {
      request.setScope(scope);
    }
    request.setUsers(users);
    request.setQueues(queues);
    request.setApplicationTypes(applicationTypes);
    request.setApplicationTags(applicationTags);
    request.setApplicationStates(applicationStates);
    if (startRange != null) {
      request.setStartRange(
          startRange.getMinimumLong(), startRange.getMaximumLong());
    }
    if (finishRange != null) {
      request.setFinishRange(
          finishRange.getMinimumLong(), finishRange.getMaximumLong());
    }
    if (limit != null) {
      request.setLimit(limit);
    }
    return request;
  }

  /**
   * <p>
   * The request from clients to get a report of Applications matching the
   * giving application types in the cluster from the
   * <code>ResourceManager</code>.
   * </p>
   *
   * @param scope {@link ApplicationsRequestScope} to filter by
   * @see ApplicationClientProtocol#getApplications(GetApplicationsRequest)
   */
  @Public
  @Stable
  public static GetApplicationsRequest newInstance(
      ApplicationsRequestScope scope) {
    GetApplicationsRequest request =
        Records.newRecord(GetApplicationsRequest.class);
    request.setScope(scope);
    return request;
  }

  /**
   * <p>
   * The request from clients to get a report of Applications matching the
   * giving application types in the cluster from the
   * <code>ResourceManager</code>.
   * </p>
   *
   *
   * @see ApplicationClientProtocol#getApplications(GetApplicationsRequest)
   */
  @Public
  @Stable
  public static GetApplicationsRequest
      newInstance(Set<String> applicationTypes) {
    GetApplicationsRequest request =
        Records.newRecord(GetApplicationsRequest.class);
    request.setApplicationTypes(applicationTypes);
    return request;
  }

  /**
   * <p>
   * The request from clients to get a report of Applications matching the
   * giving application states in the cluster from the
   * <code>ResourceManager</code>.
   * </p>
   *
   *
   * @see ApplicationClientProtocol#getApplications(GetApplicationsRequest)
   */
  @Public
  @Stable
  public static GetApplicationsRequest newInstance(
      EnumSet<YarnApplicationState> applicationStates) {
    GetApplicationsRequest request =
        Records.newRecord(GetApplicationsRequest.class);
    request.setApplicationStates(applicationStates);
    return request;
  }

  /**
   * <p>
   * The request from clients to get a report of Applications matching the
   * giving and application types and application types in the cluster from the
   * <code>ResourceManager</code>.
   * </p>
   *
   *
   * @see ApplicationClientProtocol#getApplications(GetApplicationsRequest)
   */
  @Public
  @Stable
  public static GetApplicationsRequest newInstance(
      Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates) {
    GetApplicationsRequest request =
        Records.newRecord(GetApplicationsRequest.class);
    request.setApplicationTypes(applicationTypes);
    request.setApplicationStates(applicationStates);
    return request;
  }

  /**
   * Get the application types to filter applications on
   *
   * @return Set of Application Types to filter on
   */
  @Public
  @Stable
  public abstract Set<String> getApplicationTypes();

  /**
   * Set the application types to filter applications on
   *
   * @param applicationTypes
   * A Set of Application Types to filter on.
   * If not defined, match all applications
   */
  @Private
  @Unstable
  public abstract void
      setApplicationTypes(Set<String> applicationTypes);

  /**
   * Get the application states to filter applications on
   *
   * @return Set of Application states to filter on
   */
  @Public
  @Stable
  public abstract EnumSet<YarnApplicationState> getApplicationStates();

  /**
   * Set the application states to filter applications on
   *
   * @param applicationStates
   * A Set of Application states to filter on.
   * If not defined, match all running applications
   */
  @Private
  @Unstable
  public abstract void
      setApplicationStates(EnumSet<YarnApplicationState> applicationStates);

  /**
   * Set the application states to filter applications on
   *
   * @param applicationStates all lower-case string representation of the
   *                          application states to filter on
   */
  @Private
  @Unstable
  public abstract void setApplicationStates(Set<String> applicationStates);

  /**
   * Get the users to filter applications on
   *
   * @return set of users to filter applications on
   */
  @Private
  @Unstable
  public abstract Set<String> getUsers();

  /**
   * Set the users to filter applications on
   *
   * @param users set of users to filter applications on
   */
  @Private
  @Unstable
  public abstract void setUsers(Set<String> users);

  /**
   * Get the queues to filter applications on
   *
   * @return set of queues to filter applications on
   */
  @Private
  @Unstable
  public abstract Set<String> getQueues();

  /**
   * Set the queue to filter applications on
   *
   * @param queue user to filter applications on
   */
  @Private
  @Unstable
  public abstract void setQueues(Set<String> queue);

  /**
   * Get the limit on the number applications to return
   *
   * @return number of applications to limit to
   */
  @Private
  @Unstable
  public abstract long getLimit();

  /**
   * Limit the number applications to return
   *
   * @param limit number of applications to limit to
   */
  @Private
  @Unstable
  public abstract void setLimit(long limit);

  /**
   * Get the range of start times to filter applications on
   *
   * @return {@link LongRange} of start times to filter applications on
   */
  @Private
  @Unstable
  public abstract LongRange getStartRange();

  /**
   * Set the range of start times to filter applications on
   *
   * @param begin beginning of the range
   * @param end end of the range
   * @throws IllegalArgumentException
   */
  @Private
  @Unstable
  public abstract void setStartRange(long begin, long end)
      throws IllegalArgumentException;

  /**
   * Get the range of finish times to filter applications on
   *
   * @return {@link LongRange} of finish times to filter applications on
   */
  @Private
  @Unstable
  public abstract LongRange getFinishRange();

  /**
   * Set the range of finish times to filter applications on
   *
   * @param begin beginning of the range
   * @param end end of the range
   * @throws IllegalArgumentException
   */
  @Private
  @Unstable
  public abstract void setFinishRange(long begin, long end);

  /**
   * Get the tags to filter applications on
   *
   * @return list of tags to filter on
   */
  @Private
  @Unstable
  public abstract Set<String> getApplicationTags();

  /**
   * Set the list of tags to filter applications on
   *
   * @param tags list of tags to filter on
   */
  @Private
  @Unstable
  public abstract void setApplicationTags(Set<String> tags);

  /**
   * Get the {@link ApplicationsRequestScope} of applications to be filtered.
   *
   * @return {@link ApplicationsRequestScope} of applications to return.
   */
  @Private
  @Unstable
  public abstract ApplicationsRequestScope getScope();

  /**
   * Set the {@link ApplicationsRequestScope} of applications to filter.
   *
   * @param scope scope to use for filtering applications
   */
  @Private
  @Unstable
  public abstract void setScope(ApplicationsRequestScope scope);
}
