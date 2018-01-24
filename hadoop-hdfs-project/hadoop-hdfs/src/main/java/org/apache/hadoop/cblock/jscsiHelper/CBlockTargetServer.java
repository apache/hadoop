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
package org.apache.hadoop.cblock.jscsiHelper;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.cblock.proto.MountVolumeResponse;
import org.apache.hadoop.cblock.util.KeyUtil;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.scm.XceiverClientManager;
import org.jscsi.target.Configuration;
import org.jscsi.target.Target;
import org.jscsi.target.TargetServer;

import java.io.IOException;
import java.util.HashMap;

/**
 * This class extends JSCSI target server, which is a ISCSI target that can be
 * recognized by a remote machine with ISCSI installed.
 */
public final class CBlockTargetServer extends TargetServer {
  private final OzoneConfiguration conf;
  private final CBlockManagerHandler cBlockManagerHandler;
  private final XceiverClientManager xceiverClientManager;
  private final ContainerCacheFlusher containerCacheFlusher;
  private final CBlockTargetMetrics metrics;

  public CBlockTargetServer(OzoneConfiguration ozoneConfig,
      Configuration jscsiConf,
      CBlockManagerHandler cBlockManagerHandler,
      CBlockTargetMetrics metrics)
      throws IOException {
    super(jscsiConf);
    this.cBlockManagerHandler = cBlockManagerHandler;
    this.xceiverClientManager = new XceiverClientManager(ozoneConfig);
    this.conf = ozoneConfig;
    this.containerCacheFlusher = new ContainerCacheFlusher(this.conf,
        xceiverClientManager, metrics);
    this.metrics = metrics;
    LOGGER.info("Starting flusher thread.");
    Thread flushListenerThread = new Thread(containerCacheFlusher);
    flushListenerThread.setDaemon(true);
    flushListenerThread.start();
  }

  public static void main(String[] args) throws Exception {
  }

  @Override
  public boolean isValidTargetName(String checkTargetName) {
    if (!KeyUtil.isValidVolumeKey(checkTargetName)) {
      return false;
    }
    String userName = KeyUtil.getUserNameFromVolumeKey(checkTargetName);
    String volumeName = KeyUtil.getVolumeFromVolumeKey(checkTargetName);
    if (userName == null || volumeName == null) {
      return false;
    }
    try {
      MountVolumeResponse result =
          cBlockManagerHandler.mountVolume(userName, volumeName);
      if (!result.getIsValid()) {
        LOGGER.error("Not a valid volume:" + checkTargetName);
        return false;
      }
      String volumeKey = KeyUtil.getVolumeKey(result.getUserName(),
          result.getVolumeName());
      if (!targets.containsKey(volumeKey)) {
        LOGGER.info("Mounting Volume. username: {} volume:{}",
            userName, volumeName);
        CBlockIStorageImpl ozoneStore = CBlockIStorageImpl.newBuilder()
            .setUserName(userName)
            .setVolumeName(volumeName)
            .setVolumeSize(result.getVolumeSize())
            .setBlockSize(result.getBlockSize())
            .setContainerList(result.getContainerList())
            .setClientManager(xceiverClientManager)
            .setConf(this.conf)
            .setFlusher(containerCacheFlusher)
            .setCBlockTargetMetrics(metrics)
            .build();
        Target target = new Target(volumeKey, volumeKey, ozoneStore);
        targets.put(volumeKey, target);
      }
    } catch (IOException e) {
      LOGGER.error("Can not connect to server when validating target!"
          + e.getMessage());
    }
    return targets.containsKey(checkTargetName);
  }

  @Override
  public String[] getTargetNames() {
    try {
      if (cBlockManagerHandler != null) {
        return cBlockManagerHandler.listVolumes().
            stream().map(
              volumeInfo -> volumeInfo.getUserName() + ":" + volumeInfo
                .getVolumeName()).toArray(String[]::new);
      } else {
        return new String[0];
      }
    } catch (IOException e) {
      LOGGER.error("Can't list existing volumes", e);
      return new String[0];
    }
  }

  @VisibleForTesting
  public HashMap<String, Target> getTargets() {
    return targets;
  }
}
