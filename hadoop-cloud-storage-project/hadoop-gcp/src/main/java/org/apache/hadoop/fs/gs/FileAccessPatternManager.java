/*
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

package org.apache.hadoop.fs.gs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the access pattern of object being read from cloud storage. For adaptive fadvise
 * configurations it computes the access pattern based on previous requests.
 */
class FileAccessPatternManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileAccessPatternManager.class);
  private final StorageResourceId resourceId;
  private final GoogleHadoopFileSystemConfiguration config;
  private final Fadvise fadvise;
  private boolean isPatternOverriden;
  private boolean randomAccess;
  // keeps track of any backward seek requested in lifecycle of InputStream
  private boolean isBackwardSeekRequested = false;
  // keeps track of any backward seek requested in lifecycle of InputStream
  private boolean isForwardSeekRequested = false;
  private long lastServedIndex = -1;
  // Keeps track of distance between consecutive requests
  private int consecutiveSequentialCount = 0;

  FileAccessPatternManager(
      StorageResourceId resourceId, GoogleHadoopFileSystemConfiguration configuration) {
    this.isPatternOverriden = false;
    this.resourceId = resourceId;
    this.config = configuration;
    this.fadvise = config.getFadvise();
    this.randomAccess = fadvise == Fadvise.AUTO_RANDOM || fadvise == Fadvise.RANDOM;
  }

  void updateLastServedIndex(long position) {
    this.lastServedIndex = position;
  }

  boolean shouldAdaptToRandomAccess() {
    return randomAccess;
  }

  void updateAccessPattern(long currentPosition) {
    if (isPatternOverriden) {
      LOG.trace("Will bypass computing access pattern as it's overriden for resource :{}",
          resourceId);
      return;
    }
    updateSeekFlags(currentPosition);
    if (fadvise == Fadvise.AUTO_RANDOM) {
      if (randomAccess) {
        if (shouldAdaptToSequential(currentPosition)) {
          unsetRandomAccess();
        }
      } else {
        if (shouldAdaptToRandomAccess(currentPosition)) {
          setRandomAccess();
        }
      }
    } else if (fadvise == Fadvise.AUTO) {
      if (shouldAdaptToRandomAccess(currentPosition)) {
        setRandomAccess();
      }
    }
  }

  /**
   * This provides a way to override the access isRandomPattern, once overridden it will not be
   * recomputed for adaptive fadvise types.
   *
   * @param isRandomPattern, true, to override with random access else false
   */
  void overrideAccessPattern(boolean isRandomPattern) {
    this.isPatternOverriden = true;
    this.randomAccess = isRandomPattern;
    LOG.trace(
        "Overriding the random access pattern to %s for fadvise:%s for resource: %s ",
        isRandomPattern, fadvise, resourceId);
  }

  private boolean shouldAdaptToSequential(long currentPosition) {
    if (lastServedIndex != -1) {
      long distance = currentPosition - lastServedIndex;
      if (distance < 0 || distance > config.getInplaceSeekLimit()) {
        consecutiveSequentialCount = 0;
      } else {
        consecutiveSequentialCount++;
      }
    }

    if (!shouldDetectSequentialAccess()) {
      return false;
    }

    if (consecutiveSequentialCount < config.getFadviseRequestTrackCount()) {
      return false;
    }
    LOG.trace(
        "Detected {} consecutive read request within distance threshold {} with fadvise: {} "
            + "switching to sequential IO for '{}'",
        consecutiveSequentialCount,
        config.getInplaceSeekLimit(),
        fadvise,
        resourceId);
    return true;
  }

  private boolean shouldAdaptToRandomAccess(long currentPosition) {
    if (!shouldDetectRandomAccess()) {
      return false;
    }
    if (lastServedIndex == -1) {
      return false;
    }

    if (isBackwardOrForwardSeekRequested()) {
      LOG.trace(
          "Backward or forward seek requested, isBackwardSeek: {}, isForwardSeek:{} for '{}'",
          isBackwardSeekRequested, isForwardSeekRequested, resourceId);
      return true;
    }
    return false;
  }

  private boolean shouldDetectSequentialAccess() {
    return randomAccess
        && !isBackwardOrForwardSeekRequested()
        && consecutiveSequentialCount >= config.getFadviseRequestTrackCount()
        && fadvise == Fadvise.AUTO_RANDOM;
  }

  private boolean shouldDetectRandomAccess() {
    return !randomAccess && (fadvise == Fadvise.AUTO || fadvise == Fadvise.AUTO_RANDOM);
  }

  private void setRandomAccess() {
    randomAccess = true;
  }

  private void unsetRandomAccess() {
    randomAccess = false;
  }

  private boolean isBackwardOrForwardSeekRequested() {
    return isBackwardSeekRequested || isForwardSeekRequested;
  }

  private void updateSeekFlags(long currentPosition) {
    if (lastServedIndex == -1) {
      return;
    }

    if (currentPosition < lastServedIndex) {
      isBackwardSeekRequested = true;
      LOG.trace(
          "Detected backward read from {} to {} position, updating to backwardSeek for '{}'",
          lastServedIndex, currentPosition, resourceId);

    } else if (lastServedIndex + config.getInplaceSeekLimit() < currentPosition) {
      isForwardSeekRequested = true;
      LOG.trace(
          "Detected forward read from {} to {} position over {} threshold,"
              + " updated to forwardSeek for '{}'",
          lastServedIndex, currentPosition, config.getInplaceSeekLimit(), resourceId);
    }
  }
}
