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
package org.apache.hadoop.tools.fedbalance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.TrashOption;

/**
 * This class contains the basic information needed when Federation Balance.
 */
public class FedBalanceContext implements Writable {

  /* the source path in the source sub-cluster */
  private Path src;
  /* the target path in the target sub-cluster */
  private Path dst;
  /* the mount point to be balanced */
  private String mount;
  /* Force close all open files when there is no diff between src and dst */
  private boolean forceCloseOpenFiles;
  /* Disable write by setting the mount point readonly. */
  private boolean useMountReadOnly;
  /* The map number of the distcp job. */
  private int mapNum;
  /* The bandwidth limit of the distcp job(MB). */
  private int bandwidthLimit;
  /* Move source path to trash after all the data are sync to target. Otherwise
     delete the source directly. */
  private TrashOption trashOpt;
  /* How long will the procedures be delayed. */
  private long delayDuration;
  /* The threshold of diff entries. */
  private int diffThreshold;
  /* Whether to stop after the initial DistCp job succeeds. */
  private boolean stopAfterInitialCopy;
  /* Whether to start from the incremental DistCp stage. */
  private boolean startFromIncremental;
  /* Whether to stop when a small diff cannot move to final copy. */
  private boolean stopOnSmallDiff;
  /* Sentinel path that gates leaving the incremental DistCp stage. */
  private String timeWindowSentinelPath;
  /* Sentinel path that allows force-closing open files. */
  private String forceCloseSentinelPath;

  private Configuration conf;

  public FedBalanceContext() {}

  public Configuration getConf() {
    return conf;
  }

  public Path getSrc() {
    return src;
  }

  public Path getDst() {
    return dst;
  }

  public String getMount() {
    return mount;
  }

  public boolean getForceCloseOpenFiles() {
    return forceCloseOpenFiles;
  }

  public boolean getUseMountReadOnly() {
    return useMountReadOnly;
  }

  public int getMapNum() {
    return mapNum;
  }

  public int getBandwidthLimit() {
    return bandwidthLimit;
  }

  public int getDiffThreshold() {
    return diffThreshold;
  }

  /**
   * Get whether the job should stop after the initial copy.
   *
   * @return true if the job should stop after the initial copy.
   */
  public boolean getStopAfterInitialCopy() {
    return stopAfterInitialCopy;
  }

  /**
   * Get whether the job should start from the incremental DistCp stage.
   *
   * @return true if the job should start from the incremental DistCp stage.
   */
  public boolean getStartFromIncremental() {
    return startFromIncremental;
  }

  /**
   * Get whether the job should stop on a small diff that cannot finish.
   *
   * @return true if the job should stop on a small diff that cannot finish.
   */
  public boolean getStopOnSmallDiff() {
    return stopOnSmallDiff;
  }

  /**
   * Get the sentinel path that gates leaving the incremental DistCp stage.
   *
   * @return sentinel path, or null if disabled.
   */
  public String getTimeWindowSentinelPath() {
    return timeWindowSentinelPath;
  }

  /**
   * Get the sentinel path that allows force-closing open files.
   *
   * @return sentinel path, or null if disabled.
   */
  public String getForceCloseSentinelPath() {
    return forceCloseSentinelPath;
  }

  public TrashOption getTrashOpt() {
    return trashOpt;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    conf.write(out);
    Text.writeString(out, src.toString());
    Text.writeString(out, dst.toString());
    Text.writeString(out, mount);
    out.writeBoolean(forceCloseOpenFiles);
    out.writeBoolean(useMountReadOnly);
    out.writeInt(mapNum);
    out.writeInt(bandwidthLimit);
    out.writeInt(trashOpt.ordinal());
    out.writeLong(delayDuration);
    out.writeInt(diffThreshold);
    out.writeBoolean(stopAfterInitialCopy);
    out.writeBoolean(startFromIncremental);
    out.writeBoolean(stopOnSmallDiff);
    Text.writeString(out, timeWindowSentinelPath == null ? ""
        : timeWindowSentinelPath);
    Text.writeString(out, forceCloseSentinelPath == null ? ""
        : forceCloseSentinelPath);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    conf = new Configuration(false);
    conf.readFields(in);
    src = new Path(Text.readString(in));
    dst = new Path(Text.readString(in));
    mount = Text.readString(in);
    forceCloseOpenFiles = in.readBoolean();
    useMountReadOnly = in.readBoolean();
    mapNum = in.readInt();
    bandwidthLimit = in.readInt();
    trashOpt = TrashOption.values()[in.readInt()];
    delayDuration = in.readLong();
    diffThreshold = in.readInt();
    stopAfterInitialCopy = in.readBoolean();
    startFromIncremental = in.readBoolean();
    stopOnSmallDiff = in.readBoolean();
    timeWindowSentinelPath = emptyToNull(Text.readString(in));
    forceCloseSentinelPath = emptyToNull(Text.readString(in));
  }

  private static String emptyToNull(String value) {
    return value == null || value.isEmpty() ? null : value;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }
    FedBalanceContext bc = (FedBalanceContext) obj;
    return new EqualsBuilder()
        .append(src, bc.src)
        .append(dst, bc.dst)
        .append(mount, bc.mount)
        .append(forceCloseOpenFiles, bc.forceCloseOpenFiles)
        .append(useMountReadOnly, bc.useMountReadOnly)
        .append(mapNum, bc.mapNum)
        .append(bandwidthLimit, bc.bandwidthLimit)
        .append(trashOpt, bc.trashOpt)
        .append(delayDuration, bc.delayDuration)
        .append(diffThreshold, bc.diffThreshold)
        .append(stopAfterInitialCopy, bc.stopAfterInitialCopy)
        .append(startFromIncremental, bc.startFromIncremental)
        .append(stopOnSmallDiff, bc.stopOnSmallDiff)
        .append(timeWindowSentinelPath, bc.timeWindowSentinelPath)
        .append(forceCloseSentinelPath, bc.forceCloseSentinelPath)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(src)
        .append(dst)
        .append(mount)
        .append(forceCloseOpenFiles)
        .append(useMountReadOnly)
        .append(mapNum)
        .append(bandwidthLimit)
        .append(trashOpt)
        .append(delayDuration)
        .append(diffThreshold)
        .append(stopAfterInitialCopy)
        .append(startFromIncremental)
        .append(stopOnSmallDiff)
        .append(timeWindowSentinelPath)
        .append(forceCloseSentinelPath)
        .build();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Move ").append(src).append(" to ").append(dst);
    if (useMountReadOnly) {
      builder.append(" using router mode, mount point=").append(mount)
          .append(".");
    } else {
      builder.append(" using normal federation mode.");
    }
    builder.append(" Submit distcp job with map=").append(mapNum)
        .append(" and bandwidth=").append(bandwidthLimit).append(".");
    builder.append(" When the diff count is no greater than ")
        .append(diffThreshold);
    if (forceCloseOpenFiles) {
      builder.append(", force close all open files.");
    } else {
      builder.append(", wait until there is no open files.");
    }
    switch (trashOpt) {
    case DELETE:
      builder.append(" Delete the src after the job is complete.");
      break;
    case TRASH:
      builder.append(" Move the src to trash after the job is complete.");
      break;
    default:
      break;
    }
    builder.append(" Delay duration is ").append(delayDuration).append("ms.");
    builder.append(" Stop after initial copy is ")
        .append(stopAfterInitialCopy).append(".");
    builder.append(" Start from incremental stage is ")
        .append(startFromIncremental).append(".");
    builder.append(" Stop on small diff is ").append(stopOnSmallDiff)
        .append(".");
    builder.append(" Time window sentinel path is ")
        .append(timeWindowSentinelPath).append(".");
    builder.append(" Force close sentinel path is ")
        .append(forceCloseSentinelPath).append(".");
    return builder.toString();
  }

  public static class Builder {
    private final Path src;
    private final Path dst;
    private final String mount;
    private final Configuration conf;
    private boolean forceCloseOpenFiles = false;
    private boolean useMountReadOnly = false;
    private int mapNum;
    private int bandwidthLimit;
    private TrashOption trashOpt;
    private long delayDuration;
    private int diffThreshold;
    private boolean stopAfterInitialCopy;
    private boolean startFromIncremental;
    private boolean stopOnSmallDiff;
    private String timeWindowSentinelPath;
    private String forceCloseSentinelPath;

    /**
     * This class helps building the FedBalanceContext.
     *
     * @param src the source path in the source sub-cluster.
     * @param dst the target path in the target sub-cluster.
     * @param mount the mount point to be balanced.
     * @param conf the configuration.
     */
    public Builder(Path src, Path dst, String mount, Configuration conf) {
      this.src = src;
      this.dst = dst;
      this.mount = mount;
      this.conf = conf;
    }

    /**
     * Force close open files.
     * @param value true if force close all the open files.
     * @return the builder.
     */
    public Builder setForceCloseOpenFiles(boolean value) {
      this.forceCloseOpenFiles = value;
      return this;
    }

    /**
     * Use mount point readonly to disable write.
     * @param value true if disabling write by setting mount point readonly.
     * @return the builder.
     */
    public Builder setUseMountReadOnly(boolean value) {
      this.useMountReadOnly = value;
      return this;
    }

    /**
     * The map number of the distcp job.
     * @param value the map number of the distcp.
     * @return the builder.
     */
    public Builder setMapNum(int value) {
      this.mapNum = value;
      return this;
    }

    /**
     * The bandwidth limit of the distcp job(MB).
     * @param value the bandwidth.
     * @return the builder.
     */
    public Builder setBandwidthLimit(int value) {
      this.bandwidthLimit = value;
      return this;
    }

    /**
     * Specify the trash behaviour after all the data is sync to the target.
     * @param value the trash option.
     * @return the builder.
     */
    public Builder setTrash(TrashOption value) {
      this.trashOpt = value;
      return this;
    }

    /**
     * Specify the delayed duration when the procedures need to retry.
     * @param value the delay duration.
     * @return the builder.
     */
    public Builder setDelayDuration(long value) {
      this.delayDuration = value;
      return this;
    }

    /**
     * Specify the threshold of diff entries.
     * @param value the diff threshold.
     * @return the builder.
     */
    public Builder setDiffThreshold(int value) {
      this.diffThreshold = value;
      return this;
    }

    /**
     * Specify whether to stop after the initial DistCp job succeeds.
     * @param value true if stopping after initial copy.
     * @return the builder.
     */
    public Builder setStopAfterInitialCopy(boolean value) {
      this.stopAfterInitialCopy = value;
      return this;
    }

    /**
     * Specify whether to start from the incremental DistCp stage.
     * @param value true if starting from the incremental DistCp stage.
     * @return the builder.
     */
    public Builder setStartFromIncremental(boolean value) {
      this.startFromIncremental = value;
      return this;
    }

    /**
     * Specify whether to stop when a small diff cannot move to final copy.
     * @param value true if stopping on small diff.
     * @return the builder.
     */
    public Builder setStopOnSmallDiff(boolean value) {
      this.stopOnSmallDiff = value;
      return this;
    }

    /**
     * Specify the sentinel path that gates leaving the incremental DistCp
     * stage.
     * @param value the sentinel path.
     * @return the builder.
     */
    public Builder setTimeWindowSentinelPath(String value) {
      this.timeWindowSentinelPath = emptyToNull(value);
      return this;
    }

    /**
     * Specify the sentinel path that allows force-closing open files.
     * @param value the sentinel path.
     * @return the builder.
     */
    public Builder setForceCloseSentinelPath(String value) {
      this.forceCloseSentinelPath = emptyToNull(value);
      return this;
    }

    /**
     * Build the FedBalanceContext.
     *
     * @return the FedBalanceContext obj.
     */
    public FedBalanceContext build() {
      FedBalanceContext context = new FedBalanceContext();
      context.src = this.src;
      context.dst = this.dst;
      context.mount = this.mount;
      context.conf = this.conf;
      context.forceCloseOpenFiles = this.forceCloseOpenFiles;
      context.useMountReadOnly = this.useMountReadOnly;
      context.mapNum = this.mapNum;
      context.bandwidthLimit = this.bandwidthLimit;
      context.trashOpt = this.trashOpt;
      context.delayDuration = this.delayDuration;
      context.diffThreshold = this.diffThreshold;
      context.stopAfterInitialCopy = this.stopAfterInitialCopy;
      context.startFromIncremental = this.startFromIncremental;
      context.stopOnSmallDiff = this.stopOnSmallDiff;
      context.timeWindowSentinelPath = this.timeWindowSentinelPath;
      context.forceCloseSentinelPath = this.forceCloseSentinelPath;
      return context;
    }
  }
}
