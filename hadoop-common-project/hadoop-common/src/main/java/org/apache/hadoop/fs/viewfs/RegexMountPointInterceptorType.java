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
package org.apache.hadoop.fs.viewfs;

import java.util.HashMap;
import java.util.Map;

/**
 * RegexMountPointInterceptorType.
 */
public enum RegexMountPointInterceptorType {
  REPLACE_RESOLVED_DST_PATH("replaceresolveddstpath");

  private final String configName;
  private static final Map<String, RegexMountPointInterceptorType>
      INTERCEPTOR_TYPE_MAP
      = new HashMap<String, RegexMountPointInterceptorType>();

  static {
    for (RegexMountPointInterceptorType interceptorType
        : RegexMountPointInterceptorType.values()) {
      INTERCEPTOR_TYPE_MAP.put(
          interceptorType.getConfigName(), interceptorType);
    }
  }

  RegexMountPointInterceptorType(String configName) {
    this.configName = configName;
  }

  public String getConfigName() {
    return configName;
  }

  public static RegexMountPointInterceptorType get(String configName) {
    return INTERCEPTOR_TYPE_MAP.get(configName);
  }
}
