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

package org.apache.hadoop.yarn.server.util.timeline;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.yarn.server.timeline.security.TimelineAuthenticationFilter;
import org.apache.hadoop.yarn.server.timeline.security.TimelineAuthenticationFilterInitializer;
import org.apache.hadoop.yarn.server.timeline.security.TimelineDelgationTokenSecretManagerService;

/**
 * Set of utility methods to be used across timeline reader and collector.
 */
public final class TimelineServerUtils {
  private static final Log LOG = LogFactory.getLog(TimelineServerUtils.class);

  private TimelineServerUtils() {
  }

  /**
   * Sets filter initializers configuration based on existing configuration and
   * default filters added by timeline service(such as timeline auth filter and
   * CORS filter).
   * @param conf Configuration object.
   * @param configuredInitializers Comma separated list of filter initializers.
   * @param defaultInitializers Set of initializers added by default by timeline
   *     service.
   */
  public static void setTimelineFilters(Configuration conf,
      String configuredInitializers, Set<String> defaultInitializers) {
    String[] parts = configuredInitializers.split(",");
    Set<String> target = new LinkedHashSet<String>();
    for (String filterInitializer : parts) {
      filterInitializer = filterInitializer.trim();
      if (filterInitializer.equals(
          AuthenticationFilterInitializer.class.getName()) ||
          filterInitializer.isEmpty()) {
        continue;
      }
      target.add(filterInitializer);
    }
    target.addAll(defaultInitializers);
    String actualInitializers =
        org.apache.commons.lang3.StringUtils.join(target, ",");
    LOG.info("Filter initializers set for timeline service: " +
        actualInitializers);
    conf.set("hadoop.http.filter.initializers", actualInitializers);
  }

  /**
   * Adds timeline authentication filter to the set of default filter
   * initializers and assigns the delegation token manager service to it.
   * @param initializers Comma separated list of filter initializers.
   * @param defaultInitializers Set of initializers added by default by timeline
   *     service.
   * @param delegationTokenMgrService Delegation token manager service.
   *     This will be used by timeline authentication filter to assign
   *     delegation tokens.
   */
  public static void addTimelineAuthFilter(String initializers,
      Set<String> defaultInitializers,
      TimelineDelgationTokenSecretManagerService delegationTokenMgrService) {
    TimelineAuthenticationFilter.setTimelineDelegationTokenSecretManager(
        delegationTokenMgrService.getTimelineDelegationTokenSecretManager());
    if (!initializers.contains(
        TimelineAuthenticationFilterInitializer.class.getName())) {
      defaultInitializers.add(
          TimelineAuthenticationFilterInitializer.class.getName());
    }
  }
}
