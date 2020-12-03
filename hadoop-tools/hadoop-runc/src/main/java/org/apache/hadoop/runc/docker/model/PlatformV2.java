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

package org.apache.hadoop.runc.docker.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PlatformV2 {

  private String architecture;
  private String os;
  private String osVersion;
  private List<String> osFeatures = new ArrayList<>();
  private String variant;
  private List<String> features = new ArrayList<>();

  @JsonProperty
  public String getArchitecture() {
    return architecture;
  }

  public void setArchitecture(String architecture) {
    this.architecture = architecture;
  }

  @JsonProperty
  public String getOs() {
    return os;
  }

  public void setOs(String os) {
    this.os = os;
  }

  @JsonProperty("os.version")
  public String getOsVersion() {
    return osVersion;
  }

  public void setOsVersion(String osVersion) {
    this.osVersion = osVersion;
  }

  @JsonProperty("os.features")
  public List<String> getOsFeatures() {
    return osFeatures;
  }

  @JsonProperty
  public String getVariant() {
    return variant;
  }

  public void setVariant(String variant) {
    this.variant = variant;
  }

  @JsonProperty
  public List<String> getFeatures() {
    return features;
  }

  @Override
  public String toString() {
    return String.format("{ architecture=%s, os=%s, os.version=%s, "
            + "os.features=%s, variant=%s, features=%s }",
        architecture,
        os,
        osVersion,
        osFeatures
            .stream()
            .map(Objects::toString)
            .collect(Collectors.joining(", ", "[", "]")),
        variant,
        features
            .stream()
            .map(Objects::toString)
            .collect(Collectors.joining(", ", "[", "]")));
  }

}
