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

package org.apache.slider.api.types;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialized node information. Must be kept in sync with the protobuf equivalent.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class NodeInformation {

  public String hostname;
  public String state;
  public String labels;
  public String rackName;
  public String httpAddress;
  public String healthReport;
  public long lastUpdated;
  public Map<String, NodeEntryInformation> entries = new HashMap<>();

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
      "NodeInformation{");
    sb.append("hostname='").append(hostname).append('\'');
    sb.append(", state='").append(state).append('\'');
    sb.append(", labels='").append(labels).append('\'');
    sb.append(", rackName='").append(rackName).append('\'');
    sb.append(", httpAddress='").append(httpAddress).append('\'');
    sb.append(", healthReport='").append(healthReport).append('\'');
    sb.append(", lastUpdated=").append(lastUpdated);
    sb.append('}');
    return sb.toString();
  }
}
