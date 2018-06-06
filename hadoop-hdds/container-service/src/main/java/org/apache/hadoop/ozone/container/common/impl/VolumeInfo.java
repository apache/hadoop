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

package org.apache.hadoop.ozone.container.common.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Stores information about a disk/volume.
 */
public class VolumeInfo {

  private static final Logger LOG = LoggerFactory.getLogger(VolumeInfo.class);

  private final Path rootDir;
  private final StorageType storageType;
  private VolumeState state;

  // Space usage calculator
  private VolumeUsage usage;
  // Capacity configured. This is useful when we want to
  // limit the visible capacity for tests. If negative, then we just
  // query from the filesystem.
  private long configuredCapacity;

  public static class Builder {
    private final Configuration conf;
    private final Path rootDir;
    private StorageType storageType;
    private VolumeState state;
    private long configuredCapacity;

    public Builder(Path rootDir, Configuration conf) {
      this.rootDir = rootDir;
      this.conf = conf;
    }

    public Builder(String rootDirStr, Configuration conf) {
      this.rootDir = new Path(rootDirStr);
      this.conf = conf;
    }

    public Builder storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
    }

    public Builder volumeState(VolumeState state) {
      this.state = state;
      return this;
    }

    public Builder configuredCapacity(long configuredCapacity) {
      this.configuredCapacity = configuredCapacity;
      return this;
    }

    public VolumeInfo build() throws IOException {
      return new VolumeInfo(this);
    }
  }

  private VolumeInfo(Builder b) throws IOException {

    this.rootDir = b.rootDir;
    File root = new File(rootDir.toString());

    Boolean succeeded = root.isDirectory() || root.mkdirs();

    if (!succeeded) {
      LOG.error("Unable to create the volume root dir at : {}", root);
      throw new IOException("Unable to create the volume root dir at " + root);
    }

    this.storageType = (b.storageType != null ?
        b.storageType : StorageType.DEFAULT);

    this.configuredCapacity = (b.configuredCapacity != 0 ?
        b.configuredCapacity : -1);

    this.state = (b.state != null ? b.state : VolumeState.NOT_FORMATTED);

    this.usage = new VolumeUsage(root, b.conf);

    LOG.info("Creating Volume : " + rootDir + " of storage type : " +
        storageType + " and capacity : " + configuredCapacity);
  }

  public long getCapacity() {
    return configuredCapacity < 0 ? usage.getCapacity() : configuredCapacity;
  }

  public long getAvailable() throws IOException {
    return usage.getAvailable();
  }

  public long getScmUsed() throws IOException {
    return usage.getScmUsed();
  }

  void shutdown() {
    this.state = VolumeState.NON_EXISTENT;
    shutdownUsageThread();
  }

  void failVolume() {
    setState(VolumeState.FAILED);
    shutdownUsageThread();
  }

  private void shutdownUsageThread() {
    if (usage != null) {
      usage.shutdown();
    }
    usage = null;
  }

  void setState(VolumeState state) {
    this.state = state;
  }

  public boolean isFailed() {
    return (state == VolumeState.FAILED);
  }

  public Path getRootDir() {
    return this.rootDir;
  }

  public StorageType getStorageType() {
    return this.storageType;
  }

  public enum VolumeState {
    NORMAL,
    FAILED,
    NON_EXISTENT,
    NOT_FORMATTED,
  }
}
