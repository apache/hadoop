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

import org.apache.hadoop.fs.azurebfs.constants.HdfsOperationConstants;

public class TracingHeaderValidator implements Listener {
  private String clientCorrelationID;
  private String fileSystemID;
  private String primaryRequestID = "";
  private boolean needsPrimaryRequestID;
  private String streamID = "";
  private String operation;
  private int retryNum;
  private TracingContextFormat format;

  private static final String GUID_PATTERN = "^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$";

  @Override
  public void callTracingHeaderValidator(String tracingContextHeader,
      TracingContextFormat format) {
    this.format = format;
    validateTracingHeader(tracingContextHeader);
  }

  @Override
  public TracingHeaderValidator getClone() {
    TracingHeaderValidator tracingHeaderValidator = new TracingHeaderValidator(
        clientCorrelationID, fileSystemID, operation, needsPrimaryRequestID,
        retryNum, streamID);
    tracingHeaderValidator.primaryRequestID = primaryRequestID;
    return tracingHeaderValidator;
  }

  public TracingHeaderValidator(String clientCorrelationID, String fileSystemID,
      String operation, boolean needsPrimaryRequestID, int retryNum) {
    this.clientCorrelationID = clientCorrelationID;
    this.fileSystemID = fileSystemID;
    this.operation = operation;
    this.retryNum = retryNum;
    this.needsPrimaryRequestID = needsPrimaryRequestID;
  }

  public TracingHeaderValidator(String clientCorrelationID, String fileSystemID,
      String operation, boolean needsPrimaryRequestID, int retryNum,
      String streamID) {
    this(clientCorrelationID, fileSystemID, operation, needsPrimaryRequestID,
        retryNum);
    this.streamID = streamID;
  }

  private void validateTracingHeader(String tracingContextHeader) {
    String[] idList = tracingContextHeader.split(":");
    validateBasicFormat(idList);
    if (format != TracingContextFormat.ALL_ID_FORMAT) {
      return;
    }
    if (!primaryRequestID.isEmpty() && !idList[3].isEmpty()) {
      Assertions.assertThat(idList[3])
          .describedAs("PrimaryReqID should be common for these requests")
          .isEqualTo(primaryRequestID);
    }
    if (!streamID.isEmpty()) {
      Assertions.assertThat(idList[4])
          .describedAs("Stream id should be common for these requests")
          .isEqualTo(streamID);
    }
  }

  private void validateBasicFormat(String[] idList) {
    if (format == TracingContextFormat.ALL_ID_FORMAT) {
      Assertions.assertThat(idList)
          .describedAs("header should have 7 elements").hasSize(7);
    } else if (format == TracingContextFormat.TWO_ID_FORMAT) {
      Assertions.assertThat(idList)
          .describedAs("header should have 2 elements").hasSize(2);
    } else {
      Assertions.assertThat(idList).describedAs("header should have 1 element")
          .hasSize(1);
      Assertions.assertThat(idList[0])
          .describedAs("Client request ID is a guid").matches(GUID_PATTERN);
      return;
    }

    if (clientCorrelationID.matches("[a-zA-Z0-9-]*")) {
      Assertions.assertThat(idList[0])
          .describedAs("Correlation ID should match config")
          .isEqualTo(clientCorrelationID);
    } else {
      Assertions.assertThat(idList[0])
          .describedAs("Invalid config should be replaced with empty string")
          .isEmpty();
    }
    Assertions.assertThat(idList[1]).describedAs("Client request ID is a guid")
        .matches(GUID_PATTERN);

    if (format != TracingContextFormat.ALL_ID_FORMAT) {
      return;
    }

    Assertions.assertThat(idList[2]).describedAs("Filesystem ID incorrect")
        .isEqualTo(fileSystemID);
    if (needsPrimaryRequestID && !operation
        .equals(HdfsOperationConstants.READ)) {
      Assertions.assertThat(idList[3]).describedAs("should have primaryReqId")
          .isNotEmpty();
    }
    Assertions.assertThat(idList[5]).describedAs("Operation name incorrect")
        .isEqualTo(operation);
    int retryCount = Integer.parseInt(idList[6]);
    Assertions.assertThat(retryCount)
        .describedAs("Retry was required due to issue on server side")
        .isEqualTo(retryNum);
  }

  @Override
  public void setOperation(String operation) {
    this.operation = operation;
  }

  @Override
  public void updatePrimaryRequestID(String primaryRequestID) {
    this.primaryRequestID = primaryRequestID;
  }
}
