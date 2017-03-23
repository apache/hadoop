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
package org.apache.hadoop.cblock.jscsiHelper.cache.impl;

import org.apache.hadoop.cblock.jscsiHelper.CBlockTargetMetrics;
import org.apache.hadoop.cblock.jscsiHelper.ContainerCacheFlusher;
import org.apache.hadoop.cblock.jscsiHelper.cache.CacheModule;
import org.apache.hadoop.cblock.jscsiHelper.cache.LogicalBlock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.IOException;
import java.util.List;


/**
 * A local cache used by the CBlock ISCSI server. This class is enabled or
 * disabled via config settings.
 *
 * TODO : currently, this class is a just a place holder.
 */
final public class CBlockLocalCache implements CacheModule {

  private CBlockLocalCache() {
  }

  @Override
  public LogicalBlock get(long blockID) throws IOException {
    return null;
  }

  @Override
  public void put(long blockID, byte[] data) throws IOException {
  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public void start() throws IOException {

  }

  @Override
  public void stop() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public boolean isDirtyCache() {
    return false;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for CBlocklocalCache.
   */
  public static class Builder {

    /**
     * Sets the Config to be used by this cache.
     *
     * @param configuration - Config
     * @return Builder
     */
    public Builder setConfiguration(Configuration configuration) {
      return this;
    }

    /**
     * Sets the user name who is the owner of this volume.
     *
     * @param userName - name of the owner, please note this is not the current
     * user name.
     * @return - Builder
     */
    public Builder setUserName(String userName) {
      return this;
    }

    /**
     * Sets the VolumeName.
     *
     * @param volumeName - Name of the volume
     * @return Builder
     */
    public Builder setVolumeName(String volumeName) {
      return this;
    }

    /**
     * Sets the Pipelines that form this volume.
     *
     * @param pipelines - list of pipelines
     * @return Builder
     */
    public Builder setPipelines(List<Pipeline> pipelines) {
      return this;
    }

    /**
     * Sets the Client Manager that manages the communication with containers.
     *
     * @param clientManager - clientManager.
     * @return - Builder
     */
    public Builder setClientManager(XceiverClientManager clientManager) {
      return this;
    }

    /**
     * Sets the block size -- Typical sizes are 4KB, 8KB etc.
     *
     * @param blockSize - BlockSize.
     * @return - Builder
     */
    public Builder setBlockSize(int blockSize) {
      return this;
    }

    /**
     * Sets the volumeSize.
     *
     * @param volumeSize - VolumeSize
     * @return - Builder
     */
    public Builder setVolumeSize(long volumeSize) {
      return this;
    }

    /**
     * Set flusher.
     * @param flusher - cache Flusher
     * @return Builder.
     */
    public Builder setFlusher(ContainerCacheFlusher flusher) {
      return this;
    }

    /**
     * Sets the cblock Metrics.
     *
     * @param targetMetrics - CBlock Target Metrics
     * @return - Builder
     */
    public Builder setCBlockTargetMetrics(CBlockTargetMetrics targetMetrics) {
      return this;
    }

    public CBlockLocalCache build() throws IOException {
      return new CBlockLocalCache();
    }
  }
}
