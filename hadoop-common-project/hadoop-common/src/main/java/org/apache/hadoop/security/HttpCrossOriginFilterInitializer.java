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

package org.apache.hadoop.security;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.security.http.CrossOriginFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpCrossOriginFilterInitializer extends FilterInitializer {

  public static final String PREFIX = "hadoop.http.cross-origin.";
  public static final String ENABLED_SUFFIX = "enabled";

  private static final Logger LOG =
      LoggerFactory.getLogger(HttpCrossOriginFilterInitializer.class);

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {

    String key = getEnabledConfigKey();
    boolean enabled = conf.getBoolean(key, false);
    if (enabled) {
      container.addGlobalFilter("Cross Origin Filter",
          CrossOriginFilter.class.getName(),
          getFilterParameters(conf, getPrefix()));
    } else {
      LOG.info("CORS filter not enabled. Please set " + key
          + " to 'true' to enable it");
    }
  }

  protected static Map<String, String> getFilterParameters(Configuration conf,
      String prefix) {
    Map<String, String> filterParams = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : conf.getValByRegex(prefix)
        .entrySet()) {
      String name = entry.getKey();
      String value = entry.getValue();
      name = name.substring(prefix.length());
      filterParams.put(name, value);
    }
    return filterParams;
  }

  protected String getPrefix() {
    return HttpCrossOriginFilterInitializer.PREFIX;
  }

  protected String getEnabledConfigKey() {
    return getPrefix() + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX;
  }
}
