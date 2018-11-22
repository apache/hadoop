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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * This is a spec used to prepare and run container.
 * It's return value of onDeviceAllocated invoked by the framework.
 * For preparation, if volumeSpecs is populated then the framework will
 * create the volume before using the device
 * When running container, the envs indicates environment variable needed.
 * The containerRuntime indicates what Docker runtime to use.
 * The volume and device mounts describes key isolation requirements
 * */
public final class DeviceRuntimeSpec implements Serializable {

  private static final long serialVersionUID = 554704120015467660L;

  /**
   * The containerRuntime gives device framework a hint (not forced to).
   * On which containerRuntime be used
   * (if empty then default "runc" is used).
   * For instance, it could be "nvidia" in Nvidia GPU Docker v2.
   * The "nvidia" will be passed as a parameter to docker run
   * with --runtime "nvidia"
   */
  private final String containerRuntime;
  private final Map<String, String> envs;
  private final Set<MountVolumeSpec> volumeMounts;
  private final Set<MountDeviceSpec> deviceMounts;
  private final Set<VolumeSpec> volumeSpecs;

  private DeviceRuntimeSpec(Builder builder) {
    this.containerRuntime = builder.containerRuntime;
    this.deviceMounts = builder.deviceMounts;
    this.envs = builder.envs;
    this.volumeSpecs = builder.volumeSpecs;
    this.volumeMounts = builder.volumeMounts;
  }

  public String getContainerRuntime() {
    return containerRuntime;
  }

  public Map<String, String> getEnvs() {
    return envs;
  }

  public Set<MountVolumeSpec> getVolumeMounts() {
    return volumeMounts;
  }

  public Set<MountDeviceSpec> getDeviceMounts() {
    return deviceMounts;
  }

  public Set<VolumeSpec> getVolumeSpecs() {
    return volumeSpecs;
  }
  /**
   * Builder for DeviceRuntimeSpec.
   * */
  public final static class Builder {

    private String containerRuntime;
    private Map<String, String> envs;
    private Set<MountVolumeSpec> volumeMounts;
    private Set<MountDeviceSpec> deviceMounts;
    private Set<VolumeSpec> volumeSpecs;

    private Builder() {
      containerRuntime = "";
      envs = new HashMap<>();
      volumeSpecs = new TreeSet<>();
      deviceMounts = new TreeSet<>();
      volumeMounts = new TreeSet<>();
    }

    public static Builder newInstance() {
      return new Builder();
    }

    public DeviceRuntimeSpec build() {
      return new DeviceRuntimeSpec(this);
    }

    public Builder setContainerRuntime(String cRuntime) {
      this.containerRuntime = cRuntime;
      return this;
    }

    public Builder addVolumeSpec(VolumeSpec spec) {
      this.volumeSpecs.add(spec);
      return this;
    }

    public Builder addMountVolumeSpec(MountVolumeSpec spec) {
      this.volumeMounts.add(spec);
      return this;
    }

    public Builder addMountDeviceSpec(MountDeviceSpec spec) {
      this.deviceMounts.add(spec);
      return this;
    }

    public Builder addEnv(String key, String value) {
      this.envs.put(Objects.requireNonNull(key),
          Objects.requireNonNull(value));
      return this;
    }

  }

}
