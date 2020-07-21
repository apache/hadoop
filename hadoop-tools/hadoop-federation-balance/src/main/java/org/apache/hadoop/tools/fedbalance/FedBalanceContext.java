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
        .build();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("FedBalance context:");
    builder.append(" src=").append(src);
    builder.append(", dst=").append(dst);
    if (useMountReadOnly) {
      builder.append(", router-mode=true");
      builder.append(", mount-point=").append(mount);
    } else {
      builder.append(", router-mode=false");
    }
    builder.append(", forceCloseOpenFiles=").append(forceCloseOpenFiles);
    builder.append(", trash=").append(trashOpt.name());
    builder.append(", map=").append(mapNum);
    builder.append(", bandwidth=").append(bandwidthLimit);
    builder.append(", delayDuration=").append(delayDuration);
    return builder.toString();
  }

  static class Builder {
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

    /**
     * This class helps building the FedBalanceContext.
     *
     * @param src the source path in the source sub-cluster.
     * @param dst the target path in the target sub-cluster.
     * @param mount the mount point to be balanced.
     * @param conf the configuration.
     */
    Builder(Path src, Path dst, String mount, Configuration conf) {
      this.src = src;
      this.dst = dst;
      this.mount = mount;
      this.conf = conf;
    }

    /**
     * Force close open files.
     * @param value true if force close all the open files.
     */
    public Builder setForceCloseOpenFiles(boolean value) {
      this.forceCloseOpenFiles = value;
      return this;
    }

    /**
     * Use mount point readonly to disable write.
     * @param value true if disabling write by setting mount point readonly.
     */
    public Builder setUseMountReadOnly(boolean value) {
      this.useMountReadOnly = value;
      return this;
    }

    /**
     * The map number of the distcp job.
     * @param value the map number of the distcp.
     */
    public Builder setMapNum(int value) {
      this.mapNum = value;
      return this;
    }

    /**
     * The bandwidth limit of the distcp job(MB).
     * @param value the bandwidth.
     */
    public Builder setBandwidthLimit(int value) {
      this.bandwidthLimit = value;
      return this;
    }

    /**
     * Specify the trash behaviour after all the data is sync to the target.
     * @param value the trash option.
     * */
    public Builder setTrash(TrashOption value) {
      this.trashOpt = value;
      return this;
    }

    /**
     * Specify the delayed duration when the procedures need to retry.
     */
    public Builder setDelayDuration(long value) {
      this.delayDuration = value;
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
      return context;
    }
  }
}