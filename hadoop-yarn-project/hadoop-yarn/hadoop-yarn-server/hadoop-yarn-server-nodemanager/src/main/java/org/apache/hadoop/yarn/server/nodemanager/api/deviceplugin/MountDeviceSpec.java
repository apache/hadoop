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

package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

import java.io.Serializable;
import java.util.Objects;

/**
 * Describe one device mount.
 * */
public final class MountDeviceSpec implements Serializable, Comparable {

  private static final long serialVersionUID = -160806358136943052L;

  private final String devicePathInHost;
  private final String devicePathInContainer;

  // r for only read, rw can do read and write
  private final String devicePermission;

  public final static String RO = "r";
  public final static String RW = "rw";

  private MountDeviceSpec(Builder builder) {
    this.devicePathInContainer = builder.devicePathInContainer;
    this.devicePathInHost = builder.devicePathInHost;
    this.devicePermission = builder.devicePermission;
  }

  public String getDevicePathInHost() {
    return devicePathInHost;
  }

  public String getDevicePathInContainer() {
    return devicePathInContainer;
  }

  public String getDevicePermission() {
    return devicePermission;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()){
      return false;
    }
    MountDeviceSpec other = (MountDeviceSpec) o;
    return Objects.equals(devicePathInHost, other.getDevicePathInHost())
        && Objects.equals(devicePathInContainer,
            other.getDevicePathInContainer())
        && Objects.equals(devicePermission, other.getDevicePermission());
  }

  @Override
  public int hashCode() {
    return Objects.hash(devicePathInContainer,
        devicePathInHost, devicePermission);
  }

  @Override
  public int compareTo(Object o) {
    if (o == null || (!(o instanceof MountDeviceSpec))) {
      return -1;
    }
    MountDeviceSpec other = (MountDeviceSpec) o;
    int result = devicePathInContainer.compareTo(
        other.getDevicePathInContainer());
    if (0 != result) {
      return result;
    }
    result = devicePathInHost.compareTo(other.getDevicePathInHost());
    if (0 != result) {
      return result;
    }
    return devicePermission.compareTo(other.getDevicePermission());
  }

  /**
   * Builder for MountDeviceSpec.
   * */
  public final static class Builder {
    private String devicePathInHost;
    private String devicePathInContainer;
    private String devicePermission;

    private Builder() {}

    public static Builder newInstance() {
      return new Builder();
    }

    public MountDeviceSpec build() {
      return new MountDeviceSpec(this);
    }

    public Builder setDevicePermission(String permission) {
      this.devicePermission = permission;
      return this;
    }

    public Builder setDevicePathInContainer(String pathInContainer) {
      this.devicePathInContainer = pathInContainer;
      return this;
    }

    public Builder setDevicePathInHost(String pathInHost) {
      this.devicePathInHost = pathInHost;
      return this;
    }
  }

}
