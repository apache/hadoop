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
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerInfo;


/**
 * Container Report iterates the closed containers and sends a container report
 * to SCM.
 */
public class ContainerReport {
  private static final int UNKNOWN = -1;
  private final String finalhash;
  private long size;
  private long keyCount;
  private long bytesUsed;
  private long readCount;
  private long writeCount;
  private long readBytes;
  private long writeBytes;
  private long containerID;

  public long getContainerID() {
    return containerID;
  }

  public void setContainerID(long containerID) {
    this.containerID = containerID;
  }

  /**
   * Constructs the ContainerReport.
   *
   * @param containerID - Container ID.
   * @param finalhash - Final Hash.
   */
  public ContainerReport(long containerID, String finalhash) {
    this.containerID = containerID;
    this.finalhash = finalhash;
    this.size = UNKNOWN;
    this.keyCount = UNKNOWN;
    this.bytesUsed = 0L;
    this.readCount = 0L;
    this.readBytes = 0L;
    this.writeCount = 0L;
    this.writeBytes = 0L;
  }

  /**
   * Gets a containerReport from protobuf class.
   *
   * @param info - ContainerInfo.
   * @return - ContainerReport.
   */
  public static ContainerReport getFromProtoBuf(ContainerInfo info) {
    Preconditions.checkNotNull(info);
    ContainerReport report = new ContainerReport(info.getContainerID(),
        info.getFinalhash());
    if (info.hasSize()) {
      report.setSize(info.getSize());
    }
    if (info.hasKeyCount()) {
      report.setKeyCount(info.getKeyCount());
    }
    if (info.hasUsed()) {
      report.setBytesUsed(info.getUsed());
    }
    if (info.hasReadCount()) {
      report.setReadCount(info.getReadCount());
    }
    if (info.hasReadBytes()) {
      report.setReadBytes(info.getReadBytes());
    }
    if (info.hasWriteCount()) {
      report.setWriteCount(info.getWriteCount());
    }
    if (info.hasWriteBytes()) {
      report.setWriteBytes(info.getWriteBytes());
    }

    report.setContainerID(info.getContainerID());
    return report;
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

  public long getReadCount() {
    return readCount;
  }

  public void setReadCount(long readCount) {
    this.readCount = readCount;
  }

  public long getWriteCount() {
    return writeCount;
  }

  public void setWriteCount(long writeCount) {
    this.writeCount = writeCount;
  }

  public long getReadBytes() {
    return readBytes;
  }

  public void setReadBytes(long readBytes) {
    this.readBytes = readBytes;
  }

  public long getWriteBytes() {
    return writeBytes;
  }

  public void setWriteBytes(long writeBytes) {
    this.writeBytes = writeBytes;
  }

  public long getBytesUsed() {
    return bytesUsed;
  }

  public void setBytesUsed(long bytesUsed) {
    this.bytesUsed = bytesUsed;
  }

  /**
   * Gets a containerInfo protobuf message from ContainerReports.
   *
   * @return ContainerInfo
   */
  public ContainerInfo getProtoBufMessage() {
    return ContainerInfo.newBuilder()
        .setKeyCount(this.getKeyCount())
        .setSize(this.getSize())
        .setUsed(this.getBytesUsed())
        .setReadCount(this.getReadCount())
        .setReadBytes(this.getReadBytes())
        .setWriteCount(this.getWriteCount())
        .setWriteBytes(this.getWriteBytes())
        .setFinalhash(this.getFinalhash())
        .setContainerID(this.getContainerID())
        .build();
  }
}
