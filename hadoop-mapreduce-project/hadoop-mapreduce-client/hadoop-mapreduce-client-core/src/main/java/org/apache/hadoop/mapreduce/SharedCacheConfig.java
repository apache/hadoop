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
package org.apache.hadoop.mapreduce;

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for parsing configuration parameters associated with the shared
 * cache.
 */
@Private
@Unstable
public class SharedCacheConfig {
  protected static final Logger LOG =
      LoggerFactory.getLogger(SharedCacheConfig.class);

  private boolean sharedCacheFilesEnabled = false;
  private boolean sharedCacheLibjarsEnabled = false;
  private boolean sharedCacheArchivesEnabled = false;
  private boolean sharedCacheJobjarEnabled = false;

  public void init(Configuration conf) {
    if (!MRConfig.YARN_FRAMEWORK_NAME.equals(conf.get(
        MRConfig.FRAMEWORK_NAME))) {
      // Shared cache is only valid if the job runs on yarn
      return;
    }

    if(!conf.getBoolean(YarnConfiguration.SHARED_CACHE_ENABLED,
        YarnConfiguration.DEFAULT_SHARED_CACHE_ENABLED)) {
      return;
    }


    Collection<String> configs = StringUtils.getTrimmedStringCollection(
        conf.get(MRJobConfig.SHARED_CACHE_MODE,
            MRJobConfig.SHARED_CACHE_MODE_DEFAULT));
    if (configs.contains("files")) {
      this.sharedCacheFilesEnabled = true;
    }
    if (configs.contains("libjars")) {
      this.sharedCacheLibjarsEnabled = true;
    }
    if (configs.contains("archives")) {
      this.sharedCacheArchivesEnabled = true;
    }
    if (configs.contains("jobjar")) {
      this.sharedCacheJobjarEnabled = true;
    }
    if (configs.contains("enabled")) {
      this.sharedCacheFilesEnabled = true;
      this.sharedCacheLibjarsEnabled = true;
      this.sharedCacheArchivesEnabled = true;
      this.sharedCacheJobjarEnabled = true;
    }
    if (configs.contains("disabled")) {
      this.sharedCacheFilesEnabled = false;
      this.sharedCacheLibjarsEnabled = false;
      this.sharedCacheArchivesEnabled = false;
      this.sharedCacheJobjarEnabled = false;
    }
  }

  public boolean isSharedCacheFilesEnabled() {
    return sharedCacheFilesEnabled;
  }
  public boolean isSharedCacheLibjarsEnabled() {
    return sharedCacheLibjarsEnabled;
  }
  public boolean isSharedCacheArchivesEnabled() {
    return sharedCacheArchivesEnabled;
  }
  public boolean isSharedCacheJobjarEnabled() {
    return sharedCacheJobjarEnabled;
  }
  public boolean isSharedCacheEnabled() {
    return (sharedCacheFilesEnabled || sharedCacheLibjarsEnabled ||
        sharedCacheArchivesEnabled || sharedCacheJobjarEnabled);
  }
}