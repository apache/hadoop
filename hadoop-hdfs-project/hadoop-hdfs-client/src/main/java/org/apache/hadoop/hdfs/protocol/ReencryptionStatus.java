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
package org.apache.hadoop.hdfs.protocol;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ReencryptionInfoProto;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * A class representing information about re-encrypting encryption zones. It
 * contains a collection of @{code ZoneReencryptionStatus} for each EZ.
 * <p>
 * FSDirectory lock is used for synchronization (except test-only methods, which
 * are not protected).
 */
@InterfaceAudience.Private
public final class ReencryptionStatus {

  public static final Logger LOG =
      LoggerFactory.getLogger(ReencryptionStatus.class);

  public static final BatchedListEntries<ZoneReencryptionStatus> EMPTY_LIST =
      new BatchedListEntries<>(Lists.newArrayList(), false);

  /**
   * The zones that were submitted for re-encryption. This should preserve
   * the order of submission.
   */
  private final TreeMap<Long, ZoneReencryptionStatus> zoneStatuses;
  // Metrics
  private long zonesReencrypted;

  public ReencryptionStatus() {
    zoneStatuses = new TreeMap<>();
  }

  @VisibleForTesting
  public ReencryptionStatus(ReencryptionStatus rhs) {
    if (rhs != null) {
      this.zoneStatuses = new TreeMap<>(rhs.zoneStatuses);
      this.zonesReencrypted = rhs.zonesReencrypted;
    } else {
      zoneStatuses = new TreeMap<>();
    }
  }

  @VisibleForTesting
  public void resetMetrics() {
    zonesReencrypted = 0;
    for (Map.Entry<Long, ZoneReencryptionStatus> entry : zoneStatuses
        .entrySet()) {
      entry.getValue().resetMetrics();
    }
  }

  public ZoneReencryptionStatus getZoneStatus(final Long zondId) {
    return zoneStatuses.get(zondId);
  }

  public void markZoneForRetry(final Long zoneId) {
    final ZoneReencryptionStatus zs = zoneStatuses.get(zoneId);
    Preconditions.checkNotNull(zs, "Cannot find zone " + zoneId);
    LOG.info("Zone {} will retry re-encryption", zoneId);
    zs.setState(State.Submitted);
  }

  public void markZoneStarted(final Long zoneId) {
    final ZoneReencryptionStatus zs = zoneStatuses.get(zoneId);
    Preconditions.checkNotNull(zs, "Cannot find zone " + zoneId);
    LOG.info("Zone {} starts re-encryption processing", zoneId);
    zs.setState(State.Processing);
  }

  public void markZoneCompleted(final Long zoneId) {
    final ZoneReencryptionStatus zs = zoneStatuses.get(zoneId);
    Preconditions.checkNotNull(zs, "Cannot find zone " + zoneId);
    LOG.info("Zone {} completed re-encryption.", zoneId);
    zs.setState(State.Completed);
    zonesReencrypted++;
  }

  public Long getNextUnprocessedZone() {
    for (Map.Entry<Long, ZoneReencryptionStatus> entry : zoneStatuses
        .entrySet()) {
      if (entry.getValue().getState() == State.Submitted) {
        return entry.getKey();
      }
    }
    return null;
  }

  public boolean hasRunningZone(final Long zoneId) {
    return zoneStatuses.containsKey(zoneId)
        && zoneStatuses.get(zoneId).getState() != State.Completed;
  }

  /**
   * @param zoneId
   * @return true if this is a zone is added.
   */
  private boolean addZoneIfNecessary(final Long zoneId, final String name,
      final ReencryptionInfoProto reProto) {
    if (!zoneStatuses.containsKey(zoneId)) {
      LOG.debug("Adding zone {} for re-encryption status", zoneId);
      Preconditions.checkNotNull(reProto);
      final ZoneReencryptionStatus.Builder builder =
          new ZoneReencryptionStatus.Builder();
      builder.id(zoneId).zoneName(name)
          .ezKeyVersionName(reProto.getEzKeyVersionName())
          .submissionTime(reProto.getSubmissionTime())
          .canceled(reProto.getCanceled())
          .filesReencrypted(reProto.getNumReencrypted())
          .fileReencryptionFailures(reProto.getNumFailures());
      if (reProto.hasCompletionTime()) {
        builder.completionTime(reProto.getCompletionTime());
        builder.state(State.Completed);
        zonesReencrypted++;
      } else {
        builder.state(State.Submitted);
      }
      if (reProto.hasLastFile()) {
        builder.lastCheckpointFile(reProto.getLastFile());
      }
      return zoneStatuses.put(zoneId, builder.build()) == null;
    }
    return false;
  }

  public void updateZoneStatus(final Long zoneId, final String zonePath,
      final ReencryptionInfoProto reProto) {
    Preconditions.checkArgument(zoneId != null, "zoneId can't be null");
    if (addZoneIfNecessary(zoneId, zonePath, reProto)) {
      return;
    }
    final ZoneReencryptionStatus zs = getZoneStatus(zoneId);
    assert zs != null;
    if (reProto.hasCompletionTime()) {
      zs.markZoneCompleted(reProto);
    } else if (!reProto.hasLastFile() && !reProto.hasCompletionTime()) {
      zs.markZoneSubmitted(reProto);
    } else {
      zs.updateZoneProcess(reProto);
    }
  }

  public boolean removeZone(final Long zoneId) {
    LOG.debug("Removing re-encryption status of zone {} ", zoneId);
    return zoneStatuses.remove(zoneId) != null;
  }

  @VisibleForTesting
  public int zonesQueued() {
    int ret = 0;
    for (Map.Entry<Long, ZoneReencryptionStatus> entry : zoneStatuses
        .entrySet()) {
      if (entry.getValue().getState() == State.Submitted) {
        ret++;
      }
    }
    return ret;
  }

  @VisibleForTesting
  public int zonesTotal() {
    return zoneStatuses.size();
  }

  @VisibleForTesting
  public long getNumZonesReencrypted() {
    return zonesReencrypted;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Long, ZoneReencryptionStatus> entry : zoneStatuses
        .entrySet()) {
      sb.append("[zone:" + entry.getKey())
          .append(" state:" + entry.getValue().getState())
          .append(" lastProcessed:" + entry.getValue().getLastCheckpointFile())
          .append(" filesReencrypted:" + entry.getValue().getFilesReencrypted())
          .append(" fileReencryptionFailures:" + entry.getValue()
              .getNumReencryptionFailures() + "]");
    }
    return sb.toString();
  }

  public NavigableMap<Long, ZoneReencryptionStatus> getZoneStatuses() {
    return zoneStatuses;
  }
}