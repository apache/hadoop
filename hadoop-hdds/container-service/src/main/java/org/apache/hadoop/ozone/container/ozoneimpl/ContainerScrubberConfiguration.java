/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * This class defines configuration parameters for container scrubber.
 **/
@ConfigGroup(prefix = "hdds.containerscrub")
public class ContainerScrubberConfiguration {
  private boolean enabled;
  private long metadataScanInterval;
  private long dataScanInterval;
  private long bandwidthPerVolume;

  @Config(key = "enabled",
      type = ConfigType.BOOLEAN,
      defaultValue = "false",
      tags = {ConfigTag.STORAGE},
      description = "Config parameter to enable container scrubber.")
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isEnabled() {
    return enabled;
  }

  @Config(key = "metadata.scan.interval",
      type = ConfigType.TIME,
      defaultValue = "3h",
      tags = {ConfigTag.STORAGE},
      description = "Config parameter define time interval in milliseconds" +
          " between two metadata scans by container scrubber.")
  public void setMetadataScanInterval(long metadataScanInterval) {
    this.metadataScanInterval = metadataScanInterval;
  }

  public long getMetadataScanInterval() {
    return metadataScanInterval;
  }

  @Config(key = "data.scan.interval",
      type = ConfigType.TIME,
      defaultValue = "1m",
      tags = { ConfigTag.STORAGE },
      description = "Minimum time interval between two iterations of container"
          + " data scanning.  If an iteration takes less time than this, the"
          + " scanner will wait before starting the next iteration."
  )
  public void setDataScanInterval(long dataScanInterval) {
    this.dataScanInterval = dataScanInterval;
  }

  public long getDataScanInterval() {
    return dataScanInterval;
  }

  @Config(key = "volume.bytes.per.second",
      type = ConfigType.LONG,
      defaultValue = "1048576",
      tags = {ConfigTag.STORAGE},
      description = "Config parameter to throttle I/O bandwidth used"
          + " by scrubber per volume.")
  public void setBandwidthPerVolume(long bandwidthPerVolume) {
    this.bandwidthPerVolume = bandwidthPerVolume;
  }

  public long getBandwidthPerVolume() {
    return bandwidthPerVolume;
  }
}
