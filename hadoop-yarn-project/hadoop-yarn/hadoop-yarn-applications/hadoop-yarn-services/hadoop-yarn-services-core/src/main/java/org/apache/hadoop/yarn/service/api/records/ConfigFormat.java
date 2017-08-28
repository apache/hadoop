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

package org.apache.hadoop.yarn.service.api.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Locale;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum ConfigFormat {

  JSON("json"),
  PROPERTIES("properties"),
  XML("xml"),
  HADOOP_XML("hadoop_xml"),
  ENV("env"),
  TEMPLATE("template"),
  YAML("yaml"),
  ;
  ConfigFormat(String suffix) {
    this.suffix = suffix;
  }

  private final String suffix;

  public String getSuffix() {
    return suffix;
  }


  @Override
  public String toString() {
    return suffix;
  }

  /**
   * Get a matching format or null
   * @param type
   * @return the format
   */
  public static ConfigFormat resolve(String type) {
    for (ConfigFormat format: values()) {
      if (format.getSuffix().equals(type.toLowerCase(Locale.ENGLISH))) {
        return format;
      }
    }
    return null;
  }
}
