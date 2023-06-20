/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.util;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class AppsCacheKey {
  private static final Logger LOG = LoggerFactory.getLogger(AppsCacheKey.class.getName());

  private UserGroupInformation ugi;
  private String stateQuery;
  private String finalStatusQuery;
  private String userQuery;
  private String queueQuery;
  private String limit;
  private String startedBegin;
  private String startedEnd;
  private String finishBegin;
  private String finishEnd;
  private String name;
  private Set<String> unselectedFields;

  private Set<String> applicationTags;
  private Set<String> applicationTypes;
  private Set<String> statesQuery;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public AppsCacheKey(UserGroupInformation ugi, String stateQuery, Set<String> statesQuery,
      String finalStatusQuery, String userQuery, String queueQuery, String limit,
      String startedBegin, String startedEnd, String finishBegin, String finishEnd,
      Set<String> applicationTypes, Set<String> applicationTags, String name,
      Set<String> unselectedFields) {
    this.ugi = ugi;
    this.stateQuery = stateQuery;
    this.statesQuery = statesQuery;
    this.finalStatusQuery = finalStatusQuery;
    this.userQuery = userQuery;
    this.queueQuery = queueQuery;
    this.limit = limit;
    this.startedBegin = startedBegin;
    this.startedEnd = startedEnd;
    this.finishBegin = finishBegin;
    this.finishEnd = finishEnd;
    this.applicationTypes = applicationTypes;
    this.applicationTags = applicationTags;
    this.name = name;
    this.unselectedFields = unselectedFields;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public static AppsCacheKey newInstance(String stateQuery,
      Set<String> statesQuery, String finalStatusQuery, String userQuery, String queueQuery,
      String limit, String startedBegin, String startedEnd, String finishBegin, String finishEnd,
      Set<String> applicationTypes, Set<String> applicationTags, String name,
      Set<String> unselectedFields) {

    UserGroupInformation ugi = null;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      LOG.error("unable to get ugi", e);
    }

    return new AppsCacheKey(ugi, stateQuery, statesQuery, finalStatusQuery, userQuery, queueQuery,
        limit, startedBegin, startedEnd, finishBegin, finishEnd, applicationTypes, applicationTags,
        name, unselectedFields);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AppsCacheKey that = (AppsCacheKey) o;

    return new EqualsBuilder()
               .append(this.ugi.getUserName(), that.ugi.getUserName())
               .append(this.stateQuery, that.stateQuery)
               .append(this.statesQuery, that.statesQuery)
               .append(this.finalStatusQuery, that.finalStatusQuery)
               .append(this.userQuery, that.userQuery)
               .append(this.queueQuery, that.queueQuery)
               .append(this.limit, that.limit)
               .append(this.startedBegin, that.startedBegin)
               .append(this.startedEnd, that.startedEnd)
               .append(this.finishBegin, that.finishBegin)
               .append(this.finishEnd, that.finishEnd)
               .append(this.applicationTypes, that.applicationTypes)
               .append(this.applicationTags, that.applicationTags)
               .append(this.name, that.name)
               .append(this.unselectedFields, that.unselectedFields)
               .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
               .append(this.ugi.getUserName())
               .append(this.stateQuery)
               .append(this.statesQuery)
               .append(this.finalStatusQuery)
               .append(this.userQuery)
               .append(this.queueQuery)
               .append(this.limit)
               .append(this.startedBegin)
               .append(this.startedEnd)
               .append(this.finishBegin)
               .append(this.finishEnd)
               .append(this.applicationTypes)
               .append(this.applicationTags)
               .append(this.name)
               .append(this.unselectedFields)
               .toHashCode();
  }
}