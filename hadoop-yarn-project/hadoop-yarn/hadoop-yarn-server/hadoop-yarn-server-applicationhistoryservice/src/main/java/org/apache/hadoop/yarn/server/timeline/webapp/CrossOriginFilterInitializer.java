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

package org.apache.hadoop.yarn.server.timeline.webapp;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;

public class CrossOriginFilterInitializer extends FilterInitializer {

  public static final String PREFIX =
      "yarn.timeline-service.http-cross-origin.";

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {

    container.addGlobalFilter("Cross Origin Filter",
        CrossOriginFilter.class.getName(), getFilterParameters(conf));
  }

  static Map<String, String> getFilterParameters(Configuration conf) {
    Map<String, String> filterParams =
        new HashMap<String, String>();
    for (Map.Entry<String, String> entry : conf.getValByRegex(PREFIX)
        .entrySet()) {
      String name = entry.getKey();
      String value = entry.getValue();
      name = name.substring(PREFIX.length());
      filterParams.put(name, value);
    }
    return filterParams;
  }
}
