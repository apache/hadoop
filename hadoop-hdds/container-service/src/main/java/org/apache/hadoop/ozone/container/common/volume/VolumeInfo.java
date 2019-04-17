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

package org.apache.hadoop.ozone.container.common.volume;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.fs.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Stores information about a disk/volume.
 */
public final class VolumeInfo {

  private static final Logger LOG = LoggerFactory.getLogger(VolumeInfo.class);

  private final String rootDir;
  private final StorageType storageType;

  // Space usage calculator
  private VolumeUsage usage;
  // Capacity configured. This is useful when we want to
  // limit the visible capacity for tests. If negative, then we just
  // query from the filesystem.
  private long configuredCapacity;

  /**
   * Builder for VolumeInfo.
   */
  public static class Builder {
    private final Configuration conf;
    private final String rootDir;
    private StorageType storageType;
    private long configuredCapacity;

    public Builder(String root, Configuration config) {
      this.rootDir = root;
      this.conf = config;
    }

    public Builder storageType(StorageType st) {
      this.storageType = st;
      return this;
    }

    public Builder configuredCapacity(long capacity) {
      this.configuredCapacity = capacity;
      return this;
    }

    public VolumeInfo build() throws IOException {
      return new VolumeInfo(this);
    }
  }

  private VolumeInfo(Builder b) throws IOException {

    this.rootDir = b.rootDir;
    File root = new File(this.rootDir);

    Boolean succeeded = root.isDirectory() || root.mkdirs();

    if (!succeeded) {
      LOG.error("Unable to create the volume root dir at : {}", root);
      throw new IOException("Unable to create the volume root dir at " + root);
    }

    this.storageType = (b.storageType != null ?
        b.storageType : StorageType.DEFAULT);

    this.configuredCapacity = (b.configuredCapacity != 0 ?
        b.configuredCapacity : -1);

    this.usage = new VolumeUsage(root, b.conf);
  }

  public long getCapacity() throws IOException {
    if (configuredCapacity < 0) {
      if (usage == null) {
        throw new IOException("Volume Usage thread is not running. This error" +
            " is usually seen during DataNode shutdown.");
      }
      return usage.getCapacity();
    }
    return configuredCapacity;
  }

  public long getAvailable() throws IOException {
    if (usage == null) {
      throw new IOException("Volume Usage thread is not running. This error " +
          "is usually seen during DataNode shutdown.");
    }
    return usage.getAvailable();
  }

  public long getScmUsed() throws IOException {
    if (usage == null) {
      throw new IOException("Volume Usage thread is not running. This error " +
          "is usually seen during DataNode shutdown.");
    }
    return usage.getScmUsed();
  }

  protected void shutdownUsageThread() {
    if (usage != null) {
      usage.shutdown();
    }
    usage = null;
  }

  public String getRootDir() {
    return this.rootDir;
  }

  public StorageType getStorageType() {
    return this.storageType;
  }

  /**
   * Only for testing. Do not use otherwise.
   */
  @VisibleForTesting
  public void setScmUsageForTesting(GetSpaceUsed scmUsageForTest) {
    usage.setScmUsageForTesting(scmUsageForTest);
  }

  /**
   * Only for testing. Do not use otherwise.
   */
  @VisibleForTesting
  public VolumeUsage getUsageForTesting() {
    return usage;
  }
}
