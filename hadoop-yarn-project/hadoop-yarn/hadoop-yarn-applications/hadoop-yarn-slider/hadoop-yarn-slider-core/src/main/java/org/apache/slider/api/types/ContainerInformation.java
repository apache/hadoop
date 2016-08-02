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

import org.apache.hadoop.registry.client.binding.JsonSerDeser;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Serializable version of component instance data
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class ContainerInformation {
  
  public String containerId;
  public String component;
  public String appVersion;
  public Boolean released;
  public int state;
  public Integer exitCode;
  public String diagnostics;
  public long createTime;
  public long startTime;

  public String host;
  public String hostURL;
  public String placement;
  /**
   * What is the tail output from the executed process (or [] if not started
   * or the log cannot be picked up
   */
  public String[] output;

  @Override
  public String toString() {
    JsonSerDeser<ContainerInformation> serDeser =
        new JsonSerDeser<>(
            ContainerInformation.class);
    return serDeser.toString(this);
  }
}
