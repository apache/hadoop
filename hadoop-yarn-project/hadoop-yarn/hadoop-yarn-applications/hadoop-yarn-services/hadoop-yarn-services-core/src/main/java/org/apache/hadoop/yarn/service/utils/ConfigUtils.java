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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.service.api.records.ConfigFormat;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigUtils {
  public static final String TEMPLATE_FILE = "template.file";

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

  public static Map<String, String> replacePropsInConfig(
      Map<String, String> config, Map<String, String> env) {
    Map<String, String> tokens = new HashMap<>();
    for (Entry<String, String> entry : env.entrySet()) {
      tokens.put("${" + entry.getKey() + "}", entry.getValue());
    }
    Map<String, String> newConfig = new HashMap<>();
    for (Entry<String, String> entry : config.entrySet()) {
      String value = entry.getValue();
      for (Map.Entry<String,String> token : tokens.entrySet()) {
        value = value.replaceAll(Pattern.quote(token.getKey()),
            Matcher.quoteReplacement(token.getValue()));
      }
      newConfig.put(entry.getKey(), entry.getValue());
    }
    return newConfig;
  }

  public static void prepConfigForTemplateOutputter(ConfigFormat configFormat,
      Map<String, String> config, SliderFileSystem fileSystem,
      String clusterName, String fileName) throws IOException {
    if (!configFormat.equals(ConfigFormat.TEMPLATE)) {
      return;
    }
    Path templateFile = null;
    if (config.containsKey(TEMPLATE_FILE)) {
      templateFile = fileSystem.buildResourcePath(config.get(TEMPLATE_FILE));
      if (!fileSystem.isFile(templateFile)) {
        templateFile = fileSystem.buildResourcePath(clusterName,
            config.get(TEMPLATE_FILE));
      }
      if (!fileSystem.isFile(templateFile)) {
        throw new IOException("config specified template file " + config
            .get(TEMPLATE_FILE) + " but " + templateFile + " doesn't exist");
      }
    }
    if (templateFile == null && fileName != null) {
      templateFile = fileSystem.buildResourcePath(fileName);
      if (!fileSystem.isFile(templateFile)) {
        templateFile = fileSystem.buildResourcePath(clusterName,
            fileName);
      }
    }
    if (fileSystem.isFile(templateFile)) {
      config.put("content", fileSystem.cat(templateFile));
    } else {
      config.put("content", "");
    }
  }
}
