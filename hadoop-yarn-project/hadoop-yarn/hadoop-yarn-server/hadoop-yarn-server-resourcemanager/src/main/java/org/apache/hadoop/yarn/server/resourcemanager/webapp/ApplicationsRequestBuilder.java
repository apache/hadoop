/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import com.google.common.collect.Sets;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
        .CapacityScheduler;
import org.apache.hadoop.yarn.webapp.BadRequestException;

import java.io.IOException;
import java.util.Set;

import static org.apache.hadoop.yarn.server.webapp.WebServices.parseQueries;

public class ApplicationsRequestBuilder {

  private Set<String> statesQuery = Sets.newHashSet();
  private Set<String> users = Sets.newHashSetWithExpectedSize(1);
  private Set<String> queues = Sets.newHashSetWithExpectedSize(1);
  private String limit = null;
  private Long limitNumber;

  // set values suitable in case both of begin/end not specified
  private long startedTimeBegin = 0;
  private long startedTimeEnd = Long.MAX_VALUE;
  private long finishTimeBegin = 0;
  private long finishTimeEnd = Long.MAX_VALUE;
  private Set<String> appTypes = Sets.newHashSet();
  private Set<String> appTags = Sets.newHashSet();
  private ResourceManager rm;

  private ApplicationsRequestBuilder() {
  }

  public static ApplicationsRequestBuilder create() {
    return new ApplicationsRequestBuilder();
  }

  public ApplicationsRequestBuilder withStateQuery(String stateQuery) {
    // stateQuery is deprecated.
    if (stateQuery != null && !stateQuery.isEmpty()) {
      statesQuery.add(stateQuery);
    }
    return this;
  }

  public ApplicationsRequestBuilder withStatesQuery(
      Set<String> statesQuery) {
    if (statesQuery != null) {
      this.statesQuery.addAll(statesQuery);
    }
    return this;
  }

  public ApplicationsRequestBuilder withUserQuery(String userQuery) {
    if (userQuery != null && !userQuery.isEmpty()) {
      users.add(userQuery);
    }
    return this;
  }

  public ApplicationsRequestBuilder withQueueQuery(ResourceManager rm,
      String queueQuery) {
    this.rm = rm;
    if (queueQuery != null && !queueQuery.isEmpty()) {
      queues.add(queueQuery);
    }
    return this;
  }

  public ApplicationsRequestBuilder withLimit(String limit) {
    if (limit != null && !limit.isEmpty()) {
      this.limit = limit;
    }
    return this;
  }

  public ApplicationsRequestBuilder withStartedTimeBegin(
      String startedBegin) {
    if (startedBegin != null && !startedBegin.isEmpty()) {
      startedTimeBegin = parseLongValue(startedBegin, "startedTimeBegin");
    }
    return this;
  }

  public ApplicationsRequestBuilder withStartedTimeEnd(String startedEnd) {
    if (startedEnd != null && !startedEnd.isEmpty()) {
      startedTimeEnd = parseLongValue(startedEnd, "startedTimeEnd");
    }
    return this;
  }

  public ApplicationsRequestBuilder withFinishTimeBegin(String finishBegin) {
    if (finishBegin != null && !finishBegin.isEmpty()) {
      finishTimeBegin = parseLongValue(finishBegin, "finishedTimeBegin");
    }
    return this;
  }

  public ApplicationsRequestBuilder withFinishTimeEnd(String finishEnd) {
    if (finishEnd != null && !finishEnd.isEmpty()) {
      finishTimeEnd = parseLongValue(finishEnd, "finishedTimeEnd");
    }
    return this;
  }

  public ApplicationsRequestBuilder withApplicationTypes(
      Set<String> applicationTypes) {
    if (applicationTypes !=  null) {
      appTypes = parseQueries(applicationTypes, false);
    }
    return this;
  }

  public ApplicationsRequestBuilder withApplicationTags(
      Set<String> applicationTags) {
    if (applicationTags != null) {
      appTags = parseQueries(applicationTags, false);
    }
    return this;
  }

  private void validate() {
    queues.forEach(q -> validateQueueExists(rm, q));
    validateLimit();
    validateStartTime();
    validateFinishTime();
  }

  private void validateQueueExists(ResourceManager rm, String queueQuery) {
    ResourceScheduler rs = rm.getResourceScheduler();
    if (rs instanceof CapacityScheduler) {
      CapacityScheduler cs = (CapacityScheduler) rs;
      try {
        cs.getQueueInfo(queueQuery, false, false);
      } catch (IOException e) {
        throw new BadRequestException(e.getMessage());
      }
    }
  }

  private void validateLimit() {
    if (limit != null) {
      limitNumber = parseLongValue(limit, "limit");
      if (limitNumber <= 0) {
        throw new BadRequestException("limit value must be greater then 0");
      }
    }
  }

  private long parseLongValue(String strValue, String queryName) {
    try {
      return Long.parseLong(strValue);
    } catch (NumberFormatException e) {
      throw new BadRequestException(queryName + " value must be a number!");
    }
  }

  private void validateStartTime() {
    if (startedTimeBegin < 0) {
      throw new BadRequestException("startedTimeBegin must be greater than 0");
    }
    if (startedTimeEnd < 0) {
      throw new BadRequestException("startedTimeEnd must be greater than 0");
    }
    if (startedTimeBegin > startedTimeEnd) {
      throw new BadRequestException(
          "startedTimeEnd must be greater than startTimeBegin");
    }
  }

  private void validateFinishTime() {
    if (finishTimeBegin < 0) {
      throw new BadRequestException("finishTimeBegin must be greater than 0");
    }
    if (finishTimeEnd < 0) {
      throw new BadRequestException("finishTimeEnd must be greater than 0");
    }
    if (finishTimeBegin > finishTimeEnd) {
      throw new BadRequestException(
          "finishTimeEnd must be greater than finishTimeBegin");
    }
  }

  public GetApplicationsRequest build() {
    validate();
    GetApplicationsRequest request = GetApplicationsRequest.newInstance();

    Set<String> appStates = parseQueries(statesQuery, true);
    if (!appStates.isEmpty()) {
      request.setApplicationStates(appStates);
    }
    if (!users.isEmpty()) {
      request.setUsers(users);
    }
    if (!queues.isEmpty()) {
      request.setQueues(queues);
    }
    if (limitNumber != null) {
      request.setLimit(limitNumber);
    }
    request.setStartRange(startedTimeBegin, startedTimeEnd);
    request.setFinishRange(finishTimeBegin, finishTimeEnd);

    if (!appTypes.isEmpty()) {
      request.setApplicationTypes(appTypes);
    }
    if (!appTags.isEmpty()) {
      request.setApplicationTags(appTags);
    }

    return request;
  }
}
