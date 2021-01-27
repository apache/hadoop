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
package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.MountMode;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Contains information related to a provided volume; including remote path,
 * config, policies etc. This information is required by both Namenode and
 * Datanode.
 */
public class ProvidedVolumeInfo {
  private final UUID id;
  // A local HDFS path
  private final String mountPath;
  // A remote provided storage path
  private final String remotePath;
  private final MountMode mountMode;
  private final Map<String, String> config;
  private boolean enabled = true;

  public ProvidedVolumeInfo(UUID id, String mountPath, String remotePath,
      MountMode mountMode, Map<String, String> config) {
    this.id = id;
    this.mountPath = mountPath;
    this.remotePath = remotePath;
    this.mountMode = mountMode;
    this.config = config;
  }

  public ProvidedVolumeInfo(UUID id, String mountPath, String remotePath,
      MountMode mountMode) {
    this(id, mountPath, remotePath, mountMode, Collections.emptyMap());
  }

  // readOnly is the default mount mode.
  public ProvidedVolumeInfo(UUID id, String mountPath, String remotePath,
      Map<String, String> config) {
    this(id, mountPath, remotePath, MountMode.READONLY, config);
  }

  // For datanode use.
  public ProvidedVolumeInfo(UUID id, String remotePath,
      MountMode mountMode, Map<String, String> config) {
    this(id, null, remotePath, mountMode, config);
  }

  @VisibleForTesting
  public ProvidedVolumeInfo() {
    this(UUID.randomUUID(), "", "", null, Collections.emptyMap());
  }

  public String getId() {
    return id.toString();
  }

  public String getRemotePath() {
    return remotePath;
  }

  public Map<String, String> getConfig() {
    return new HashMap<>(config);
  }

  public String getMountPath() {
    return mountPath;
  }

  public MountMode getMountMode() {
    return mountMode;
  }

  public String getMountModeString() {
    return mountMode.toString();
  }

  public boolean isWriteBackMountMode() {
    return mountMode == MountMode.WRITEBACK;
  }

  public boolean isSyncMount() {
    return mountMode == MountMode.BACKUP ||
        mountMode == MountMode.WRITEBACK;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public boolean isPaused() {
    return !enabled;
  }

  public void pause() {
    enabled = false;
  }

  public void resume() {
    enabled = true;
  }
}
