package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * Holds configuration items for Ratis/Raft server.
 */
@ConfigGroup(prefix = "hdds.ratis.server")
public class RatisServerConfiguration {

  private int numSnapshotsRetained;

  @Config(key = "num.snapshots.retained",
      type = ConfigType.INT,
      defaultValue = "5",
      tags = {ConfigTag.STORAGE},
      description = "Config parameter to specify number of old snapshots " +
          "retained at the Ratis leader.")
  public void setNumSnapshotsRetained(int numSnapshotsRetained) {
    this.numSnapshotsRetained = numSnapshotsRetained;
  }

  public int getNumSnapshotsRetained() {
    return numSnapshotsRetained;
  }

}
