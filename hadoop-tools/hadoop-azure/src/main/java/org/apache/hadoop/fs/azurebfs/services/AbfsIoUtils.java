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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

/**
 * Utility classes to work with the remote store.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class AbfsIoUtils {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsIoUtils.class);

  private AbfsIoUtils() {
  }

  /**
   * Dump the headers of a request/response to the log at DEBUG level.
   * @param origin header origin for log
   * @param headers map of headers.
   */
  public static void dumpHeadersToDebugLog(final String origin,
      final Map<String, List<String>> headers) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}", origin);
      for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
        String key = entry.getKey();
        if (key == null) {
          key = "HTTP Response";
        }
        String values = StringUtils.join(";", entry.getValue());
        if (key.contains("Cookie")) {
          values = "*cookie info*";
        }
        if (key.equals("sig")) {
          values = "XXXX";
        }
        LOG.debug("  {}={}",
            key,
            values);
      }
    }
  }
}
