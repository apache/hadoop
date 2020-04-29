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
package org.apache.hadoop.tools.dynamometer;

import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;

class DynoResource {

  private final String name;
  private final LocalResourceType type;
  private final String resourcePath;

  DynoResource(String name, LocalResourceType type, String resourcePath) {
    this.name = name;
    this.type = type;
    this.resourcePath = resourcePath;
  }

  public Path getPath(Map<String, String> env) {
    return new Path(env.get(getLocationEnvVar()));
  }

  public long getTimestamp(Map<String, String> env) {
    return Long.parseLong(env.get(getTimestampEnvVar()));
  }

  public long getLength(Map<String, String> env) {
    return Long.parseLong(env.get(getLengthEnvVar()));
  }

  public String getLocationEnvVar() {
    return name + "_LOCATION";
  }

  public String getTimestampEnvVar() {
    return name + "_TIMESTAMP";
  }

  public String getLengthEnvVar() {
    return name + "_LENGTH";
  }

  public LocalResourceType getType() {
    return type;
  }

  public String getResourcePath() {
    return resourcePath;
  }

  public String toString() {
    return name;
  }

}
