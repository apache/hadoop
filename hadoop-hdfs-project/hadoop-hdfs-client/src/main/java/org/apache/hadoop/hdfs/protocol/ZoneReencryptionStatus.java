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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ReencryptionInfoProto;

/**
 * A class representing information about re-encryption of an encryption zone.
 * <p>
 * FSDirectory lock is used for synchronization (except test-only methods, which
 * are not protected).
 */
public class ZoneReencryptionStatus {
  /**
   * State of re-encryption.
   */
  public enum State {
    /**
     * Submitted for re-encryption but hasn't been picked up.
     * This is the initial state.
     */
    Submitted,
    /**
     * Currently re-encrypting.
     */
    Processing,
    /**
     * Re-encryption completed.
     */
    Completed
  }

  private long id;
  private String zoneName;
  /**
   * The re-encryption status of the zone. Note this is a in-memory only
   * variable. On failover it will always be submitted, or completed if
   * completionTime != 0;
   */
  private State state;
  private String ezKeyVersionName;
  private long submissionTime;
  private long completionTime;
  private boolean canceled;
  /**
   * Name of last file processed. It's important to record name (not inode)
   * because we want to restore to the position even if the inode is removed.
   */
  private String lastCheckpointFile;
  private long filesReencrypted;
  private long numReencryptionFailures;

  /**
   * Builder of {@link ZoneReencryptionStatus}.
   */
  public static final class Builder {
    private long id;
    private String zoneName;
    private State state;
    private String ezKeyVersionName;
    private long submissionTime;
    private long completionTime;
    private boolean canceled;
    private String lastCheckpointFile;
    private long filesReencrypted;
    private long fileReencryptionFailures;

    public Builder() {
    }

    public Builder id(final long inodeid) {
      id = inodeid;
      return this;
    }

    public Builder zoneName(final String ezName) {
      zoneName = ezName;
      return this;
    }

    public Builder state(final State st) {
      state = st;
      return this;
    }

    public Builder ezKeyVersionName(final String ezkvn) {
      ezKeyVersionName = ezkvn;
      return this;
    }

    public Builder submissionTime(final long submission) {
      submissionTime = submission;
      return this;
    }

    public Builder completionTime(final long completion) {
      completionTime = completion;
      return this;
    }

    public Builder canceled(final boolean isCanceled) {
      canceled = isCanceled;
      return this;
    }

    public Builder lastCheckpointFile(final String lastFile) {
      lastCheckpointFile = lastFile;
      return this;
    }

    public Builder filesReencrypted(final long numReencrypted) {
      filesReencrypted = numReencrypted;
      return this;
    }

    public Builder fileReencryptionFailures(final long numFailures) {
      fileReencryptionFailures = numFailures;
      return this;
    }

    public ZoneReencryptionStatus build() {
      ZoneReencryptionStatus ret = new ZoneReencryptionStatus();
      Preconditions.checkArgument(id != 0, "no inode id set.");
      Preconditions.checkNotNull(state, "no state id set.");
      Preconditions.checkNotNull(ezKeyVersionName, "no keyVersionName set.");
      Preconditions
          .checkArgument(submissionTime != 0, "no submission time set.");
      ret.id = this.id;
      ret.zoneName = this.zoneName;
      ret.state = this.state;
      ret.ezKeyVersionName = this.ezKeyVersionName;
      ret.submissionTime = this.submissionTime;
      ret.completionTime = this.completionTime;
      ret.canceled = this.canceled;
      ret.lastCheckpointFile = this.lastCheckpointFile;
      ret.filesReencrypted = this.filesReencrypted;
      ret.numReencryptionFailures = this.fileReencryptionFailures;
      return ret;
    }
  }

  public ZoneReencryptionStatus() {
    reset();
  }

  void resetMetrics() {
    filesReencrypted = 0;
    numReencryptionFailures = 0;
  }

  public long getId() {
    return id;
  }

  public String getZoneName() {
    return zoneName;
  }

  void setState(final State s) {
    state = s;
  }

  public State getState() {
    return state;
  }

  public String getEzKeyVersionName() {
    return ezKeyVersionName;
  }

  public long getSubmissionTime() {
    return submissionTime;
  }

  public long getCompletionTime() {
    return completionTime;
  }

  public boolean isCanceled() {
    return canceled;
  }

  public String getLastCheckpointFile() {
    return lastCheckpointFile;
  }

  public long getFilesReencrypted() {
    return filesReencrypted;
  }

  public long getNumReencryptionFailures() {
    return numReencryptionFailures;
  }

  public void reset() {
    state = State.Submitted;
    ezKeyVersionName = null;
    submissionTime = 0;
    completionTime = 0;
    canceled = false;
    lastCheckpointFile = null;
    resetMetrics();
  }

  /**
   * Set the zone name. The zone name is resolved from inode id and set during
   * a listReencryptionStatus call, for the crypto admin to consume.
   */
  public void setZoneName(final String name) {
    Preconditions.checkNotNull(name, "zone name cannot be null");
    zoneName = name;
  }

  public void cancel() {
    canceled = true;
  }

  void markZoneCompleted(final ReencryptionInfoProto proto) {
    state = ZoneReencryptionStatus.State.Completed;
    completionTime = proto.getCompletionTime();
    lastCheckpointFile = null;
    canceled = proto.getCanceled();
    filesReencrypted = proto.getNumReencrypted();
    numReencryptionFailures = proto.getNumFailures();
  }

  void markZoneSubmitted(final ReencryptionInfoProto proto) {
    reset();
    state = ZoneReencryptionStatus.State.Submitted;
    ezKeyVersionName = proto.getEzKeyVersionName();
    submissionTime = proto.getSubmissionTime();
    filesReencrypted = proto.getNumReencrypted();
    numReencryptionFailures = proto.getNumFailures();
  }

  void updateZoneProcess(final ReencryptionInfoProto proto) {
    lastCheckpointFile = proto.getLastFile();
    filesReencrypted = proto.getNumReencrypted();
    numReencryptionFailures = proto.getNumFailures();
  }
}
