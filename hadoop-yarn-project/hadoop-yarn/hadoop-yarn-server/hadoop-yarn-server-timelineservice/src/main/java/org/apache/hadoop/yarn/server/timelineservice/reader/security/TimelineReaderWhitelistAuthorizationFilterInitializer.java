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

package org.apache.hadoop.yarn.server.timelineservice.reader.security;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Filter initializer to initialize
 * {@link TimelineReaderWhitelistAuthorizationFilter} for ATSv2 timeline reader
 * with timeline service specific configurations.
 */
public class TimelineReaderWhitelistAuthorizationFilterInitializer
    extends FilterInitializer {

  /**
   * Initializes {@link TimelineReaderWhitelistAuthorizationFilter}.
   *
   * @param container The filter container
   * @param conf Configuration for run-time parameters
   */
  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    Map<String, String> params = new HashMap<String, String>();
    String isWhitelistReadAuthEnabled = Boolean.toString(
        conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_READ_AUTH_ENABLED));
    params.put(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED,
        isWhitelistReadAuthEnabled);
    params.put(YarnConfiguration.TIMELINE_SERVICE_READ_ALLOWED_USERS,
        conf.get(YarnConfiguration.TIMELINE_SERVICE_READ_ALLOWED_USERS,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_READ_ALLOWED_USERS));

    params.put(YarnConfiguration.YARN_ADMIN_ACL,
        conf.get(YarnConfiguration.YARN_ADMIN_ACL,
            // using a default of ""
            // instead of DEFAULT_YARN_ADMIN_ACL
            // The reason being, DEFAULT_YARN_ADMIN_ACL is set to all users
            // and we do not wish to allow everyone by default if
            // read auth is enabled and YARN_ADMIN_ACL is unset
            TimelineReaderWhitelistAuthorizationFilter.EMPTY_STRING));
    container.addGlobalFilter("Timeline Reader Whitelist Authorization Filter",
        TimelineReaderWhitelistAuthorizationFilter.class.getName(), params);
  }
}
