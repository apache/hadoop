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

package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

import java.util.Objects;

/**
 * Contains plugin register request info.
 * */
public final class DeviceRegisterRequest {

  // plugin's own version
  private final String pluginVersion;
  private final String resourceName;

  private DeviceRegisterRequest(Builder builder) {
    this.resourceName = Objects.requireNonNull(builder.resourceName);
    this.pluginVersion = builder.pluginVersion;
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getPluginVersion() {
    return pluginVersion;
  }

  /**
   * Builder class for construct {@link DeviceRegisterRequest}.
   * */
  public final static class Builder {
    private String pluginVersion;
    private String resourceName;

    private Builder() {}

    public static Builder newInstance() {
      return new Builder();
    }

    public DeviceRegisterRequest build() {
      return new DeviceRegisterRequest(this);
    }

    public Builder setResourceName(String resName) {
      this.resourceName = resName;
      return this;
    }

    public Builder setPluginVersion(String plVersion) {
      this.pluginVersion = plVersion;
      return this;
    }

  }
}
