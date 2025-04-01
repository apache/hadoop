/*
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

package org.apache.hadoop.fs.tosfs.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TOSClientContextUtils {
  public static final Logger LOG = LoggerFactory.getLogger(TOSClientContextUtils.class);

  private TOSClientContextUtils() {
  }

  public static String normalizeEndpoint(String endpoint) {
    for (String scheme : new String[]{"https://", "http://", "tos://"}) {
      if (endpoint.startsWith(scheme)) {
        return endpoint.substring(scheme.length());
      }
    }
    return endpoint;
  }

  public static String parseRegion(String endpoint) {
    String region = null;
    String newEndpoint = normalizeEndpoint(endpoint);
    String[] parts = newEndpoint.split("\\.");
    if (parts.length == 3) {
      // Endpoint  is formatted like 'tos-<region>.volces.com'
      region = parts[0].replace("tos-", "");
    } else if (parts.length == 4) {
      // Endpoint is formatted like '<bucket>.tos-<region>.volces.com'
      region = parts[1].replace("tos-", "");
    } else if (parts.length == 6) {
      // Endpoint is formatted like '<ep-id>.tos.<region>.privatelink.volces.com'
      region = parts[2];
    } else if (parts.length == 7) {
      // Endpoint is formatted like '<bucket>.<ep-id>.tos.<region>.privatelink.volces.com'
      region = parts[3];
    }
    LOG.debug("parse region [{}] from endpoint [{}]", region, endpoint);
    return region;
  }
}
