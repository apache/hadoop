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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.security.AuthenticationWithProxyUserFilter;
import org.apache.hadoop.yarn.server.timeline.security.TimelineAuthenticationFilterInitializer;

/**
 * Filter initializer to initialize {@link AuthenticationWithProxyUserFilter}
 * for ATSv2 timeline reader server with timeline service specific
 * configurations.
 */
public class TimelineReaderAuthenticationFilterInitializer extends
    TimelineAuthenticationFilterInitializer{

  /**
   * Initializes {@link AuthenticationWithProxyUserFilter}
   * <p>
   * Propagates to {@link AuthenticationWithProxyUserFilter} configuration all
   * YARN configuration properties prefixed with
   * {@value TimelineAuthenticationFilterInitializer#PREFIX}.
   *
   * @param container
   *          The filter container
   * @param conf
   *          Configuration for run-time parameters
   */
  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    setAuthFilterConfig(conf);
    container.addGlobalFilter("Timeline Reader Authentication Filter",
        AuthenticationWithProxyUserFilter.class.getName(),
        getFilterConfig());
  }
}
