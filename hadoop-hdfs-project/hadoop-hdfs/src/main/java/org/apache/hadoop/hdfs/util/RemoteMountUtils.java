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

package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Static functions for dealing with remote volume mounting.
 */
public class RemoteMountUtils {
  /**
   * Parses a command separated string of key=value pairs and produces
   * configurations required to connect to a remote volume.
   *
   * @param xConfig extra configuration provided by the user. The expected
   *                value of this string is a comma separated string of
   *                key=value pairs
   */
  public static Map<String, String> decodeConfig(String xConfig) {
    if (xConfig == null || xConfig.trim().isEmpty()) {
      return Collections.emptyMap();
    }

    HashMap<String, String> mountConfigs = new HashMap<>();
    String[] pairs = StringUtils.split(xConfig.trim());
    for (String p : pairs) {
      String[] pair = StringUtils.split(p.trim(), '\\', '=');
      Preconditions.checkArgument(pair.length == 2,
          "Could not parse key-value pair " + p);
      String key = pair[0];
      String value =
          StringUtils.unEscapeString(pair[1], '\\', new char[] {'=', ',' });
      mountConfigs.put(key, value);
    }
    return mountConfigs;
  }

  /**
   * Inverse of {@link RemoteMountUtils#decodeConfig} utility method.
   *
   * @param config map of configs related to a remote mount
   * @return comma separated string of key=value pairs
   */
  public static String encodeConfig(Map<String, String> config) {
    if (config == null || config.isEmpty()) {
      return "";
    }

    char[] charsToEscape = {'=', ',' };
    char escapeChar = '\\';

    List<String> configStrings = new ArrayList<>();
    for (Entry<String, String> conf : config.entrySet()) {
      String str = conf.getKey() + "=" + StringUtils
          .escapeString(conf.getValue(), escapeChar, charsToEscape);
      configStrings.add(str);
    }

    return StringUtils.join(",", configStrings);
  }
}
