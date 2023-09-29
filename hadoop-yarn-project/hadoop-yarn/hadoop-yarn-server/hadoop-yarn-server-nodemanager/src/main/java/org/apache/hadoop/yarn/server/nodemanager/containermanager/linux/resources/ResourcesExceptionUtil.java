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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RESOURCE_PLUGINS_FAIL_FAST;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RESOURCE_PLUGINS_FAIL_FAST;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * Small utility class which only re-throws YarnException if
 * NM_RESOURCE_PLUGINS_FAIL_FAST property is true.
 *
 */
public final class ResourcesExceptionUtil {
  private ResourcesExceptionUtil() {}

  public static void throwIfNecessary(YarnException e, Configuration conf)
      throws YarnException {
    if (conf.getBoolean(NM_RESOURCE_PLUGINS_FAIL_FAST,
        DEFAULT_NM_RESOURCE_PLUGINS_FAIL_FAST)) {
      throw e;
    }
  }
}
