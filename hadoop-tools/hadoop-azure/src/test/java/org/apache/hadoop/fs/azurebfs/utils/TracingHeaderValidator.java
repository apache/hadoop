package org.apache.hadoop.fs.azurebfs.utils;

import org.assertj.core.api.Assertions;

public class TracingHeaderValidator implements Listener {
  String clientCorrelationID;
  String fileSystemID;
  String primaryRequestID = "";
  boolean needsPrimaryRequestID;
  String streamID = "";
  String operation;
  int retryNum;
  String GUID_PATTERN = "[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}";
  // client-req-id as per docs: ^[{(]?[0-9a-f]{8}[-]?([0-9a-f]{4}[-]?)
  // {3}[0-9a-f]{12}[)}]?$

  @Override
  public void afterOp(String tracingContextHeader) {
    validateTracingHeader(tracingContextHeader);
  }

  @Override
  public void updatePrimaryRequestID(String primaryRequestID) {
    this.primaryRequestID = primaryRequestID;
  }

  @Override
  public TracingHeaderValidator getClone() {
    TracingHeaderValidator tracingHeaderValidator =
        new TracingHeaderValidator(clientCorrelationID, fileSystemID,
            operation, needsPrimaryRequestID, retryNum, streamID);
    tracingHeaderValidator.primaryRequestID = primaryRequestID;
    return tracingHeaderValidator;
  }

  public TracingHeaderValidator(String clientCorrelationID,
      String fileSystemID, String operation,
      boolean needsPrimaryRequestID, int retryNum) {
    this.clientCorrelationID = clientCorrelationID;
    this.fileSystemID = fileSystemID;
    this.operation = operation;
    this.retryNum = retryNum;
    this.needsPrimaryRequestID = needsPrimaryRequestID;
  }

  public TracingHeaderValidator(String clientCorrelationID,
      String fileSystemID, String operation, boolean needsPrimaryRequestID,
      int retryNum, String streamID) {
    this(clientCorrelationID, fileSystemID, operation, needsPrimaryRequestID,
        retryNum);
    this.streamID = streamID;
  }

  private void validateTracingHeader(String tracingContextHeader) {
    String[] id_list = tracingContextHeader.split(":");
    validateBasicFormat(id_list);
    if (needsPrimaryRequestID) {
      Assertions.assertThat(id_list[3])
          .describedAs("PrimaryReqID should be common for these requests")
          .isEqualTo(primaryRequestID);
      Assertions.assertThat(id_list[2]).describedAs(
          "FilesystemID should be same for requests with same filesystem")
          .isEqualTo(fileSystemID);
    }
      if (!streamID.isEmpty()) {
        System.out.println("check stream" + streamID);
        Assertions.assertThat(id_list[4])
            .describedAs("Stream id should be common for these requests")
            .isEqualTo(streamID);
      }
  }

  private void validateBasicFormat(String[] id_list) {
    Assertions.assertThat(id_list)
        .describedAs("header should have 7 elements").hasSize(7);

    if(clientCorrelationID.matches("[a-zA-Z0-9-]*")) {
      Assertions.assertThat(id_list[0]).describedAs("Correlation ID should match config")
          .isEqualTo(clientCorrelationID);
    } else {
      Assertions.assertThat(id_list[0])
          .describedAs("Invalid config should be replaced with empty string")
          .isEmpty();
    }
    Assertions.assertThat(id_list[1])
        .describedAs("Client request ID is a guid")
        .matches(GUID_PATTERN);
    Assertions.assertThat(id_list[2]).describedAs("Filesystem ID incorrect")
        .isEqualTo(fileSystemID);
    Assertions.assertThat(id_list[5]).describedAs("Operation name incorrect")
        .isEqualTo(operation);
    int retryCount = Integer.parseInt(id_list[6]);
    Assertions.assertThat(retryCount)
        .describedAs("Retry count incorrect")
        .isEqualTo(retryNum);
  }
}
