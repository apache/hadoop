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
package org.apache.hadoop.yarn.service.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigUtils {

  public static String replaceProps(Map<String, String> config, String content) {
    Map<String, String> tokens = new HashMap<>();
    for (Entry<String, String> entry : config.entrySet()) {
      tokens.put("${" + entry.getKey() + "}", entry.getValue());
      tokens.put("{{" + entry.getKey() + "}}", entry.getValue());
    }
    String value = content;
    for (Map.Entry<String,String> token : tokens.entrySet()) {
      value = value.replaceAll(Pattern.quote(token.getKey()),
          Matcher.quoteReplacement(token.getValue()));
    }
    return value;
  }
}
