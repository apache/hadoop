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

package org.apache.hadoop.mapreduce.v2.app2.job.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TaskAttemptImplHelpers {

  private static final Log LOG = LogFactory.getLog(TaskAttemptImplHelpers.class);
   
  static String[] resolveHosts(String[] src) {
    String[] result = new String[src.length];
    for (int i = 0; i < src.length; i++) {
      if (isIP(src[i])) {
        result[i] = resolveHost(src[i]);
      } else {
        result[i] = src[i];
      }
    }
    return result;
  }

  static String resolveHost(String src) {
    String result = src; // Fallback in case of failure.
    try {
      InetAddress addr = InetAddress.getByName(src);
      result = addr.getHostName();
    } catch (UnknownHostException e) {
      LOG.warn("Failed to resolve address: " + src
          + ". Continuing to use the same.");
    }
    return result;
  }

  private static final Pattern ipPattern = // Pattern for matching ip
    Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
  
  static boolean isIP(String src) {
    return ipPattern.matcher(src).matches();
  }
}
