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

package org.apache.hadoop.fs.azurebfs.utils;

import org.assertj.core.api.Assertions;

import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.ReadType;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SPLIT_NO_LIMIT;

/**
 * Used to validate correlation identifiers provided during testing against
 * values that get associated with a request through its TracingContext instance
 */
public class TracingHeaderValidator implements Listener {
  private String clientCorrelationId;
  private String fileSystemId;
  private String primaryRequestId = EMPTY_STRING;
  private boolean needsPrimaryRequestId;
  private String streamID = "";
  private FSOperationType operation;
  private int retryNum;
  private TracingHeaderFormat format;

  private static final String GUID_PATTERN = "^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$";
  private String ingressHandler = null;
  private String position = String.valueOf(0);
  private ReadType readType = ReadType.UNKNOWN_READ;

  private Integer operatedBlobCount = null;

  @Override
  public void callTracingHeaderValidator(String tracingContextHeader,
      TracingHeaderFormat format) {
    this.format = format;
    validateTracingHeader(tracingContextHeader);
  }

  @Override
  public TracingHeaderValidator getClone() {
    TracingHeaderValidator tracingHeaderValidator = new TracingHeaderValidator(
        clientCorrelationId, fileSystemId, operation, needsPrimaryRequestId,
        retryNum, streamID);
    tracingHeaderValidator.primaryRequestId = primaryRequestId;
    tracingHeaderValidator.ingressHandler = ingressHandler;
    tracingHeaderValidator.position = position;
    tracingHeaderValidator.readType = readType;
    tracingHeaderValidator.operatedBlobCount = operatedBlobCount;
    return tracingHeaderValidator;
  }

  public TracingHeaderValidator(String clientCorrelationId, String fileSystemId,
      FSOperationType operation, boolean needsPrimaryRequestId, int retryNum) {
    this.clientCorrelationId = clientCorrelationId;
    this.fileSystemId = fileSystemId;
    this.operation = operation;
    this.retryNum = retryNum;
    this.needsPrimaryRequestId = needsPrimaryRequestId;
  }

  public TracingHeaderValidator(String clientCorrelationId, String fileSystemId,
      FSOperationType operation, boolean needsPrimaryRequestId, int retryNum,
      String streamID) {
    this(clientCorrelationId, fileSystemId, operation, needsPrimaryRequestId,
        retryNum);
    this.streamID = streamID;
  }

  private void validateTracingHeader(String tracingContextHeader) {
    String[] idList = tracingContextHeader.split(":", SPLIT_NO_LIMIT);
    validateBasicFormat(idList);
    if (format != TracingHeaderFormat.ALL_ID_FORMAT) {
      return;
    }

    // Validate Operated Blob Count
    if (operatedBlobCount != null) {
      Assertions.assertThat(Integer.parseInt(idList[10]))
          .describedAs("OperatedBlobCount is incorrect")
          .isEqualTo(operatedBlobCount);
    }

    // Validate Primary Request ID
    if (!primaryRequestId.isEmpty() && !idList[4].isEmpty()) {
      Assertions.assertThat(idList[4])
          .describedAs("PrimaryReqID should be common for these requests")
          .isEqualTo(primaryRequestId);
    }

    // Validate Stream ID
    if (!streamID.isEmpty()) {
      Assertions.assertThat(idList[5])
          .describedAs("Stream id should be common for these requests")
          .isEqualTo(streamID);
    }
  }

  private void validateBasicFormat(String[] idList) {
    // Validate Version and Number of fields in the header
    Assertions.assertThat(idList[0]).describedAs("Version should be present")
        .isEqualTo(TracingHeaderVersion.getCurrentVersion().toString());
    int expectedSize = 0;
    if (format == TracingHeaderFormat.ALL_ID_FORMAT) {
      expectedSize = TracingHeaderVersion.getCurrentVersion().getFieldCount();
    } else if (format == TracingHeaderFormat.TWO_ID_FORMAT) {
      expectedSize = 3;
    } else {
      Assertions.assertThat(idList).describedAs("header should have 1 element")
          .hasSize(1);
      Assertions.assertThat(idList[0])
          .describedAs("Client request ID is a guid").matches(GUID_PATTERN);
      return;
    }
    Assertions.assertThat(idList)
        .describedAs("header should have " + expectedSize + " elements")
        .hasSize(expectedSize);

    // Validate Client Correlation ID
    if (clientCorrelationId.matches("[a-zA-Z0-9-]*")) {
      Assertions.assertThat(idList[1])
          .describedAs("Correlation ID should match config")
          .isEqualTo(clientCorrelationId);
    } else {
      Assertions.assertThat(idList[1])
          .describedAs("Invalid config should be replaced with empty string")
          .isEmpty();
    }

    // Validate Client Request ID
    Assertions.assertThat(idList[2]).describedAs("Client request ID is a guid")
        .matches(GUID_PATTERN);

    if (format != TracingHeaderFormat.ALL_ID_FORMAT) {
      return;
    }

    // Validate FileSystem ID
    Assertions.assertThat(idList[3]).describedAs("Filesystem ID incorrect")
        .isEqualTo(fileSystemId);

    // Validate Primary Request ID
    if (needsPrimaryRequestId && !operation
        .equals(FSOperationType.READ)) {
      Assertions.assertThat(idList[4]).describedAs("should have primaryReqId")
          .isNotEmpty();
    }

    // Validate Operation Type
    Assertions.assertThat(idList[6]).describedAs("Operation name incorrect")
        .isEqualTo(operation.toString());

    // Validate Retry Header
    if (idList[7].contains("_")) {
      idList[7] = idList[7].split("_")[0];
    }
    int retryCount = Integer.parseInt(idList[7]);
    Assertions.assertThat(retryCount)
        .describedAs("Retry was required due to issue on server side")
        .isEqualTo(retryNum);
  }

  /**
   * Sets the value of expected Hadoop operation
   * @param operation Hadoop operation code (String of two characters)
   */
  @Override
  public void setOperation(FSOperationType operation) {
    this.operation = operation;
  }

  @Override
  public void updatePrimaryRequestID(String primaryRequestId) {
    this.primaryRequestId = primaryRequestId;
  }

  @Override
  public void updateIngressHandler(String ingressHandler) {
    this.ingressHandler = ingressHandler;
  }

  @Override
  public void updatePosition(String position) {
    this.position = position;
  }

  @Override
  public void updateReadType(ReadType readType) {
    this.readType = readType;
  }

  /**
   * Sets the value of the number of blobs operated on.
   * @param operatedBlobCount number of blobs operated on
   */
  public void setOperatedBlobCount(Integer operatedBlobCount) {
    this.operatedBlobCount = operatedBlobCount;
  }
}
