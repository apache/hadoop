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

package org.apache.hadoop.fs.azurebfs.utils;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.services.AbfsClient;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;

/**
 * The TracingContext class to correlate Store requests using unique
 * identifiers and resources common to requests (e.g. filesystem, stream)
 *
 * Implementing new HDFS method:
 * Create TracingContext instance in method of outer layer of
 * ABFS driver (AzureBlobFileSystem/AbfsInputStream/AbfsOutputStream), to be
 * passed through ABFS layers up to AbfsRestOperation.
 *
 * Add new operations to HdfsOperationConstants file.
 *
 * PrimaryRequestId can be enabled for individual HDFS API that invoke
 * multiple Store calls.
 *
 * Testing:
 * Pass an instance of TracingHeaderValidator to registerListener() of ABFS
 * filesystem/stream class before calling the API in tests.
 */

public class TracingContext {
  private final String clientCorrelationID;
  private final String fileSystemID;
  private String clientRequestID = EMPTY_STRING;
  private String primaryRequestID;
  private String streamID;
  private int retryCount;
  private String hadoopOpName;
  private final TracingContextFormat format;
  private Listener listener = null;

  private static final Logger LOG = LoggerFactory.getLogger(AbfsClient.class);
  public static final int MAX_CLIENT_CORRELATION_ID_LENGTH = 72;
  public static final String CLIENT_CORRELATION_ID_PATTERN = "[a-zA-Z0-9-]*";

  /**
   * Initialize TracingContext
   * @param clientCorrelationID Provided over config by client
   * @param fileSystemID Unique guid for AzureBlobFileSystem instance
   * @param hadoopOpName Code indicating the high-level Hadoop operation that
   *                    triggered the current Store request
   * @param tracingContextFormat Format of IDs to be printed in header and logs
   * @param listener Holds instance of TracingHeaderValidator during testing,
   *                null otherwise
   */
  public TracingContext(String clientCorrelationID, String fileSystemID,
      String hadoopOpName, TracingContextFormat tracingContextFormat,
      Listener listener) {
    this.fileSystemID = fileSystemID;
    this.hadoopOpName = hadoopOpName;
    this.clientCorrelationID = validateClientCorrelationID(clientCorrelationID);
    streamID = EMPTY_STRING;
    retryCount = 0;
    primaryRequestID = EMPTY_STRING;
    format = tracingContextFormat;
    this.listener = listener;
  }

  public TracingContext(String clientCorrelationID, String fileSystemID,
      String hadoopOpName, boolean needsPrimaryReqId,
      TracingContextFormat tracingContextFormat, Listener listener) {
    this(clientCorrelationID, fileSystemID, hadoopOpName, tracingContextFormat,
        listener);
    primaryRequestID = needsPrimaryReqId ? UUID.randomUUID().toString() : "";
    if (listener != null) {
      listener.updatePrimaryRequestID(primaryRequestID);
    }
  }

  public TracingContext(TracingContext originalTracingContext) {
    this.fileSystemID = originalTracingContext.fileSystemID;
    this.streamID = originalTracingContext.streamID;
    this.clientCorrelationID = originalTracingContext.clientCorrelationID;
    this.hadoopOpName = originalTracingContext.hadoopOpName;
    this.retryCount = 0;
    this.primaryRequestID = originalTracingContext.primaryRequestID;
    this.format = originalTracingContext.format;
    if (originalTracingContext.listener != null) {
      this.listener = originalTracingContext.listener.getClone();
    }
  }

  public String validateClientCorrelationID(String clientCorrelationID) {
    if ((clientCorrelationID.length() > MAX_CLIENT_CORRELATION_ID_LENGTH)
        || (!clientCorrelationID.matches(CLIENT_CORRELATION_ID_PATTERN))) {
      LOG.debug(
          "Invalid config provided; correlation id not included in header.");
      return EMPTY_STRING;
    }
    return clientCorrelationID;
  }

  public void generateClientRequestID() {
    clientRequestID = UUID.randomUUID().toString();
  }

  public void setPrimaryRequestID() {
    primaryRequestID = UUID.randomUUID().toString();
    if (listener != null) {
      listener.updatePrimaryRequestID(primaryRequestID);
    }
  }

  public void setStreamID(String stream) {
    streamID = stream;
  }

  public void setOperation(String operation) {
    this.hadoopOpName = operation;
  }

  public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }

  public void setListener(Listener listener) {
    this.listener = listener;
  }

  public String toString() {
    String header;
    switch (format) {
    case ALL_ID_FORMAT:
      header =
          clientCorrelationID + ":" + clientRequestID + ":" + fileSystemID + ":"
              + primaryRequestID + ":" + streamID + ":" + hadoopOpName + ":"
              + retryCount;
      break;
    case TWO_ID_FORMAT:
      header = clientCorrelationID + ":" + clientRequestID;
      break;
    default:
      header = clientRequestID; //case SINGLE_ID_FORMAT
    }
    if (listener != null) { //testing
      listener.callTracingHeaderValidator(header, format);
    }
    return header;
  }

}
