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

import static java.lang.Math.max;

/**
 * KeyValueContainer Report iterates the closed containers and sends a
 * container report to SCM.
 */
public class KeyValueContainerReport extends ContainerReport{
  private long deleteTransactionId;

  /**
   * Constructs the KeyValueContainerReport.
   *
   * @param containerID - Container ID.
   * @param finalhash - Final Hash.
   */
  public KeyValueContainerReport(long containerID, String finalhash) {
    super(containerID, finalhash);
    this.deleteTransactionId = 0;
  }

  /**
   * Sets the deleteTransactionId if it is greater than existing.
   * @param transactionId - deleteTransactionId
   */
  public void updateDeleteTransactionId(long transactionId) {
    this.deleteTransactionId = max(transactionId, deleteTransactionId);
  }

  /**
   * Gets the deleteTransactionId.
   * @return - deleteTransactionId.
   */
  public long getDeleteTransactionId() {
    return this.deleteTransactionId;
  }

  /**
   * Gets a containerReport from protobuf class.
   *
   * @param info - ContainerInfo.
   * @return - ContainerReport.
   */
  public static KeyValueContainerReport getFromProtoBuf(ContainerInfo info) {
    Preconditions.checkNotNull(info);
    KeyValueContainerReport report = new KeyValueContainerReport(
        info.getContainerID(), info.getFinalhash());
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
    if (info.hasDeleteTransactionId()) {
      report.updateDeleteTransactionId(info.getDeleteTransactionId());
    }
    report.setContainerID(info.getContainerID());
    return report;
  }

  /**
   * Gets a containerInfo protobuf message from ContainerReports.
   *
   * @return ContainerInfo
   */
  @Override
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
        .setDeleteTransactionId(this.getDeleteTransactionId())
        .build();
  }
}
