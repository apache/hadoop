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
package org.apache.hadoop.hdfs.server.federation.router;

import java.util.Map;

/**
 * A dynamically assignable parameter that is location-specific.
 * <p>
 * There are 2 ways this mapping is determined:
 * <ul>
 * <li>Default: Uses the RemoteLocationContext's destination
 * <li>Map: Uses the value of the RemoteLocationContext key provided in the
 * parameter map.
 * </ul>
 */
public class RemoteParam {

  private final Map<? extends Object, ? extends Object> paramMap;

  /**
   * Constructs a default remote parameter. Always maps the value to the
   * destination of the provided RemoveLocationContext.
   */
  public RemoteParam() {
    this.paramMap = null;
  }

  /**
   * Constructs a map based remote parameter. Determines the value using the
   * provided RemoteLocationContext as a key into the map.
   *
   * @param map Map with RemoteLocationContext keys.
   */
  public RemoteParam(
      Map<? extends RemoteLocationContext, ? extends Object> map) {
    this.paramMap = map;
  }

  /**
   * Determine the appropriate value for this parameter based on the location.
   *
   * @param context Context identifying the location.
   * @return A parameter specific to this location.
   */
  public Object getParameterForContext(RemoteLocationContext context) {
    if (context == null) {
      return null;
    } else if (this.paramMap != null) {
      return this.paramMap.get(context);
    } else {
      // Default case
      return context.getDest();
    }
  }
}