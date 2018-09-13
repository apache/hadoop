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

package org.apache.hadoop.ozone.container.common.interfaces;


import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerType;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;

/**
 * Dispatcher sends ContainerCommandRequests to Handler. Each Container Type
 * should have an implementation for Handler.
 */
public abstract class Handler {

  protected final Configuration conf;
  protected final ContainerSet containerSet;
  protected final VolumeSet volumeSet;
  protected String scmID;
  protected final ContainerMetrics metrics;

  protected Handler(Configuration config, ContainerSet contSet,
      VolumeSet volumeSet, ContainerMetrics containerMetrics) {
    conf = config;
    containerSet = contSet;
    this.volumeSet = volumeSet;
    this.metrics = containerMetrics;
  }

  public static Handler getHandlerForContainerType(ContainerType containerType,
      Configuration config, ContainerSet contSet, VolumeSet volumeSet,
                                                   ContainerMetrics metrics) {
    switch (containerType) {
    case KeyValueContainer:
      return new KeyValueHandler(config, contSet, volumeSet, metrics);
    default:
      throw new IllegalArgumentException("Handler for ContainerType: " +
        containerType + "doesn't exist.");
    }
  }

  public abstract ContainerCommandResponseProto handle(
      ContainerCommandRequestProto msg, Container container);

  /**
   * Import container data from a raw input stream.
   */
  public abstract Container importContainer(
      long containerID,
      long maxSize,
      FileInputStream rawContainerStream,
      TarContainerPacker packer)
      throws IOException;

  public void setScmID(String scmId) {
    this.scmID = scmId;
  }

}
