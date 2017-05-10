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
package org.apache.hadoop.ozone.container.common.helpers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerInfo;

/**
 * Container Report iterates the closed containers and sends a container report
 * to SCM.
 * <p>
 * The protobuf counter part of this class looks like this.
 * message ContainerInfo {
 * required string containerName = 1;
 * required string finalhash = 2;
 * optional int64 size = 3;
 * optional int64 keycount = 4;
 * }
 */
public class ContainerReport {
  private static final int UNKNOWN = -1;
  private final String containerName;
  private final String finalhash;
  private long size;
  private long keyCount;

  /**
   * Constructs the ContainerReport.
   *
   * @param containerName - Container Name.
   * @param finalhash - Final Hash.
   */
  public ContainerReport(String containerName, String finalhash) {
    this.containerName = containerName;
    this.finalhash = finalhash;
    this.size = UNKNOWN;
    this.keyCount = UNKNOWN;
  }

  /**
   * Gets a containerReport from protobuf class.
   *
   * @param info - ContainerInfo.
   * @return - ContainerReport.
   */
  public static ContainerReport getFromProtoBuf(ContainerInfo info) {
    Preconditions.checkNotNull(info);
    ContainerReport report = new ContainerReport(info.getContainerName(),
        info.getFinalhash());
    if (info.hasSize()) {
      report.setSize(info.getSize());
    }
    if (info.hasKeycount()) {
      report.setKeyCount(info.getKeycount());
    }
    return report;
  }

  /**
   * Gets the container name.
   *
   * @return - Name
   */
  public String getContainerName() {
    return containerName;
  }

  /**
   * Returns the final signature for this container.
   *
   * @return - hash
   */
  public String getFinalhash() {
    return finalhash;
  }

  /**
   * Returns a positive number it is a valid number, -1 if not known.
   *
   * @return size or -1
   */
  public long getSize() {
    return size;
  }

  /**
   * Sets the size of the container on disk.
   *
   * @param size - int
   */
  public void setSize(long size) {
    this.size = size;
  }

  /**
   * Gets number of keys in the container if known.
   *
   * @return - Number of keys or -1 for not known.
   */
  public long getKeyCount() {
    return keyCount;
  }

  /**
   * Sets the key count.
   *
   * @param keyCount - Key Count
   */
  public void setKeyCount(long keyCount) {
    this.keyCount = keyCount;
  }

  /**
   * Gets a containerInfo protobuf message from ContainerReports.
   *
   * @return ContainerInfo
   */
  public ContainerInfo getProtoBufMessage() {
    return ContainerInfo.newBuilder()
        .setContainerName(this.getContainerName())
        .setKeycount(this.getKeyCount())
        .setSize(this.getSize())
        .setFinalhash(this.getFinalhash())
        .build();
  }
}
