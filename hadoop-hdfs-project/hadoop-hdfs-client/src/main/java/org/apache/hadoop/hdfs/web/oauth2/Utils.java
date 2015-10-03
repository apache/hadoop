/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hdfs.web.oauth2;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

@InterfaceAudience.Private
@InterfaceStability.Evolving
final class Utils {
  private Utils() { /* Private constructor */ }

  public static String notNull(Configuration conf, String key) {
    String value = conf.get(key);

    if(value == null) {
      throw new IllegalArgumentException("No value for " + key +
          " found in conf file.");
    }

    return value;
  }

  public static String postBody(String ... kv)
      throws UnsupportedEncodingException {
    if(kv.length % 2 != 0) {
      throw new IllegalArgumentException("Arguments must be key value pairs");
    }
    StringBuilder sb = new StringBuilder();
    int i = 0;

    while(i < kv.length) {
      if(i > 0) {
        sb.append("&");
      }
      sb.append(URLEncoder.encode(kv[i++], "UTF-8"));
      sb.append("=");
      sb.append(URLEncoder.encode(kv[i++], "UTF-8"));
    }

    return sb.toString();
  }
}
