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
