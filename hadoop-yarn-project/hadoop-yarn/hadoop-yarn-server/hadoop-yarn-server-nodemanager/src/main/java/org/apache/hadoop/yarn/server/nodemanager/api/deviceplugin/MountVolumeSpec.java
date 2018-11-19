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
 * Describe one volume mount.
 * */
public final class MountVolumeSpec implements Serializable, Comparable {

  private static final long serialVersionUID = 2479676805545997492L;

  // host path or volume name
  private final String hostPath;

  // path in the container
  private final String mountPath;

  // if true, data in mountPath can only be read
  // "-v hostPath:mountPath:ro"
  private final Boolean isReadOnly;

  public final static String READONLYOPTION = "ro";

  private MountVolumeSpec(Builder builder) {
    this.hostPath = builder.hostPath;
    this.mountPath = builder.mountPath;
    this.isReadOnly = builder.isReadOnly;
  }

  public String getHostPath() {
    return hostPath;
  }

  public String getMountPath() {
    return mountPath;
  }

  public Boolean getReadOnly() {
    return isReadOnly;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()){
      return false;
    }
    MountVolumeSpec other = (MountVolumeSpec) o;
    return Objects.equals(hostPath, other.getHostPath())
        && Objects.equals(mountPath, other.getMountPath())
        && Objects.equals(isReadOnly, other.getReadOnly());
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostPath, mountPath, isReadOnly);
  }

  @Override
  public int compareTo(Object o) {
    return 0;
  }

  /**
   * Builder for MountVolumeSpec.
   * */
  public final static class Builder {
    private String hostPath;
    private String mountPath;
    private Boolean isReadOnly;

    private Builder() {}

    public static Builder newInstance() {
      return new Builder();
    }

    public MountVolumeSpec build() {
      return new MountVolumeSpec(this);
    }

    public Builder setHostPath(String hPath) {
      this.hostPath = hPath;
      return this;
    }

    public Builder setMountPath(String mPath) {
      this.mountPath = mPath;
      return this;
    }

    public Builder setReadOnly(Boolean readOnly) {
      isReadOnly = readOnly;
      return this;
    }
  }
}
