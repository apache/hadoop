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
package org.apache.hadoop.ozone;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeServicePlugin;
import org.apache.hadoop.hdsl.HdslUtils;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.hdsl.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine
    .DatanodeStateMachine;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Datanode service plugin to start the HDSL container services.
 */
public class HdslDatanodeService implements DataNodeServicePlugin {

  private static final Logger LOG = LoggerFactory.getLogger(
      HdslDatanodeService.class);

  private final boolean isOzoneEnabled;

  private Configuration conf;
  private DatanodeDetails datanodeDetails;
  private DatanodeStateMachine datanodeStateMachine;

  public HdslDatanodeService() {
    try {
      OzoneConfiguration.activate();
      this.conf = new OzoneConfiguration();
      this.isOzoneEnabled = HdslUtils.isHdslEnabled(conf);
      if (isOzoneEnabled) {
        this.datanodeDetails = getDatanodeDetails(conf);
        String hostname = DataNode.getHostName(conf);
        String ip = InetAddress.getByName(hostname).getHostAddress();
        this.datanodeDetails.setHostName(hostname);
        this.datanodeDetails.setIpAddress(ip);
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't start the HDSL datanode plugin", e);
    }
  }

  @Override
  public void start(Object service) {
    if (isOzoneEnabled) {
      try {
        DataNode dataNode = (DataNode) service;
        datanodeDetails.setInfoPort(dataNode.getInfoPort());
        datanodeDetails.setInfoSecurePort(dataNode.getInfoSecurePort());
        datanodeStateMachine = new DatanodeStateMachine(datanodeDetails, conf);
        datanodeStateMachine.startDaemon();
      } catch (IOException e) {
        throw new RuntimeException("Can't start the HDSL datanode plugin", e);
      }
    }
  }

  /**
   * Returns ContainerNodeIDProto or null in case of Error.
   *
   * @return ContainerNodeIDProto
   */
  private static DatanodeDetails getDatanodeDetails(Configuration conf)
      throws IOException {
    String idFilePath = HdslUtils.getDatanodeIdFilePath(conf);
    if (idFilePath == null || idFilePath.isEmpty()) {
      LOG.error("A valid file path is needed for config setting {}",
          ScmConfigKeys.OZONE_SCM_DATANODE_ID);
      throw new IllegalArgumentException(ScmConfigKeys.OZONE_SCM_DATANODE_ID +
          " must be defined. See" +
          " https://wiki.apache.org/hadoop/Ozone#Configuration" +
          " for details on configuring Ozone.");
    }

    Preconditions.checkNotNull(idFilePath);
    File idFile = new File(idFilePath);
    if (idFile.exists()) {
      return ContainerUtils.readDatanodeDetailsFrom(idFile);
    } else {
      // There is no datanode.id file, this might be the first time datanode
      // is started.
      String datanodeUuid = UUID.randomUUID().toString();
      return DatanodeDetails.newBuilder().setUuid(datanodeUuid).build();
    }
  }

  /**
   *
   * Return DatanodeDetails if set, return null otherwise.
   *
   * @return DatanodeDetails
   */
  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  @InterfaceAudience.Private
  public DatanodeStateMachine getDatanodeStateMachine() {
    return datanodeStateMachine;
  }

  @Override
  public void stop() {
    if (datanodeStateMachine != null) {
      datanodeStateMachine.stopDaemon();
    }
  }

  @Override
  public void close() throws IOException {
  }
}
