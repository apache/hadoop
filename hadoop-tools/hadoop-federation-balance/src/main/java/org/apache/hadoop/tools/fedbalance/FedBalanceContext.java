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
  /* Whether to preserve ACLs in submitted DistCp jobs. */
  private boolean preserveAcl = true;
  /* Whether to preserve modification/access times in submitted DistCp jobs. */
  private boolean preserveTimes;
  /* DistCp copy strategy. */
  private String distCpStrategy;
  /* Number of DistCp listStatus threads. */
  private int numListstatusThreads;

  private Configuration conf;

  public FedBalanceContext() {}

  /**
   * Get the configuration used by this context.
   *
   * @return configuration used by this context.
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Get the source path.
   *
   * @return source path.
   */
  public Path getSrc() {
    return src;
  }

  /**
   * Get the destination path.
   *
   * @return destination path.
   */
  public Path getDst() {
    return dst;
  }

  /**
   * Get the mount point.
   *
   * @return mount point.
   */
  public String getMount() {
    return mount;
  }

  /**
   * Get whether open files should be force closed.
   *
   * @return true if open files should be force closed.
   */
  public boolean getForceCloseOpenFiles() {
    return forceCloseOpenFiles;
  }

  /**
   * Get whether the mount point should be made read-only.
   *
   * @return true if the mount point should be made read-only.
   */
  public boolean getUseMountReadOnly() {
    return useMountReadOnly;
  }

  /**
   * Get the maximum number of maps for submitted DistCp jobs.
   *
   * @return maximum number of maps for submitted DistCp jobs.
   */
  public int getMapNum() {
    return mapNum;
  }

  /**
   * Get the per-map bandwidth limit in MB.
   *
   * @return per-map bandwidth limit in MB.
   */
  public int getBandwidthLimit() {
    return bandwidthLimit;
  }

  /**
   * Get the snapshot diff threshold.
   *
   * @return snapshot diff threshold.
   */
  public int getDiffThreshold() {
    return diffThreshold;
  }

  /**
   * Get whether ACLs should be preserved when supported.
   *
   * @return true if ACLs should be preserved when supported.
   */
  public boolean getPreserveAcl() {
    return preserveAcl;
  }

  /**
   * Get whether file timestamps should be preserved.
   *
   * @return true if file timestamps should be preserved.
   */
  public boolean getPreserveTimes() {
    return preserveTimes;
  }

  /**
   * Get the DistCp copy strategy.
   *
   * @return DistCp copy strategy, or null to use the DistCp default.
   */
  public String getDistCpStrategy() {
    return distCpStrategy;
  }

  /**
   * Get the number of DistCp listStatus threads.
   *
   * @return number of DistCp listStatus threads, or 0 for DistCp default.
   */
  public int getNumListstatusThreads() {
    return numListstatusThreads;
  }

  /**
   * Get the source trash behavior.
   *
   * @return source trash behavior.
   */
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
    out.writeBoolean(preserveAcl);
    out.writeBoolean(preserveTimes);
    Text.writeString(out, distCpStrategy == null ? "" : distCpStrategy);
    out.writeInt(numListstatusThreads);
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
    preserveAcl = in.readBoolean();
    preserveTimes = in.readBoolean();
    distCpStrategy = emptyToNull(Text.readString(in));
    numListstatusThreads = in.readInt();
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
        .append(preserveAcl, bc.preserveAcl)
        .append(preserveTimes, bc.preserveTimes)
        .append(distCpStrategy, bc.distCpStrategy)
        .append(numListstatusThreads, bc.numListstatusThreads)
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
        .append(preserveAcl)
        .append(preserveTimes)
        .append(distCpStrategy)
        .append(numListstatusThreads)
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
    builder.append(" Preserve ACL is ").append(preserveAcl).append(".");
    builder.append(" Preserve times is ").append(preserveTimes).append(".");
    builder.append(" DistCp strategy is ").append(distCpStrategy).append(".");
    builder.append(" DistCp listStatus threads is ")
        .append(numListstatusThreads).append(".");
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
    private boolean preserveAcl = true;
    private boolean preserveTimes;
    private String distCpStrategy;
    private int numListstatusThreads;

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
     * Specify whether ACLs should be preserved by DistCp.
     * @param value true if preserving ACLs.
     * @return the builder.
     */
    public Builder setPreserveAcl(boolean value) {
      this.preserveAcl = value;
      return this;
    }

    /**
     * Specify whether file times should be preserved by DistCp.
     * @param value true if preserving file times.
     * @return the builder.
     */
    public Builder setPreserveTimes(boolean value) {
      this.preserveTimes = value;
      return this;
    }

    /**
     * Specify the DistCp copy strategy.
     * @param value the DistCp copy strategy.
     * @return the builder.
     */
    public Builder setDistCpStrategy(String value) {
      this.distCpStrategy = emptyToNull(value);
      return this;
    }

    /**
     * Specify the DistCp listStatus thread count.
     * @param value the DistCp listStatus thread count.
     * @return the builder.
     */
    public Builder setNumListstatusThreads(int value) {
      this.numListstatusThreads = value;
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
      context.preserveAcl = this.preserveAcl;
      context.preserveTimes = this.preserveTimes;
      context.distCpStrategy = this.distCpStrategy;
      context.numListstatusThreads = this.numListstatusThreads;
      return context;
    }
  }
}
