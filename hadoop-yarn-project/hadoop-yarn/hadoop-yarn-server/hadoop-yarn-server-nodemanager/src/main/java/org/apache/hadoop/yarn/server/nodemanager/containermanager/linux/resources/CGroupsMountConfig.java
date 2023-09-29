/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Stores config related to cgroups.
 */
public class CGroupsMountConfig {
  private final boolean enableMount;
  private final String mountPath;

  public CGroupsMountConfig(Configuration conf) {
    this.enableMount = conf.getBoolean(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_MOUNT, false);
    this.mountPath = conf.get(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, null);
  }

  public boolean ensureMountPathIsDefined() throws ResourceHandlerException {
    if (mountPath == null) {
      throw new ResourceHandlerException(
          String.format("Cgroups mount path not specified in %s.",
              YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH));
    }
    return true;
  }

  public boolean isMountPathDefined() {
    return mountPath != null;
  }

  public boolean isMountEnabled() {
    return enableMount;
  }

  public boolean mountDisabledButMountPathDefined() {
    return !enableMount && mountPath != null;
  }

  public boolean mountEnabledAndMountPathDefined() {
    return enableMount && mountPath != null;
  }

  public String getMountPath() {
    return mountPath;
  }

  @Override
  public String toString() {
    return "CGroupsMountConfig{" +
        "enableMount=" + enableMount +
        ", mountPath='" + mountPath + '\'' +
        '}';
  }
}
