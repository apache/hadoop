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

import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.ReadType;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_HYPHEN;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;

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
 * PrimaryRequestId can be enabled for individual Hadoop API that invoke
 * multiple Store calls.
 *
 * Testing:
 * Pass an instance of TracingHeaderValidator to registerListener() of ABFS
 * filesystem/stream class before calling the API in tests.
 */

public class TracingContext {
  private final String clientCorrelationID;  // passed over config by client
  private final String fileSystemID;  // GUID for fileSystem instance
  private String clientRequestId = EMPTY_STRING;  // GUID per http request
  //Optional, non-empty for methods that trigger two or more Store calls
  private String primaryRequestId;
  private String streamID;  // appears per stream instance (read/write ops)
  private int retryCount;  // retry number as recorded by AbfsRestOperation
  private FSOperationType opType;  // two-lettered code representing Hadoop op
  private final TracingHeaderFormat format;  // header ID display options
  private Listener listener = null;  // null except when testing
  //final concatenated ID list set into x-ms-client-request-id header
  private String header = EMPTY_STRING;
  private String ingressHandler = EMPTY_STRING;
  private String position = EMPTY_STRING; // position of read/write in remote file
  private String metricResults = EMPTY_STRING;
  private String metricHeader = EMPTY_STRING;
  private ReadType readType = ReadType.UNKNOWN_READ;

  /**
   * If {@link #primaryRequestId} is null, this field shall be set equal
   * to the last part of the {@link #clientRequestId}'s UUID
   * in {@link #constructHeader(AbfsHttpOperation, String, String)} only on the
   * first API call for an operation. Subsequent retries for that operation
   * will not change this field. In case {@link  #primaryRequestId} is non-null,
   * this field shall not be set.
   */
  private String primaryRequestIdForRetry = EMPTY_STRING;
  private Integer operatedBlobCount = 0; // Only relevant for rename-delete over blob endpoint where it will be explicitly set.

  private static final Logger LOG = LoggerFactory.getLogger(AbfsClient.class);
  public static final int MAX_CLIENT_CORRELATION_ID_LENGTH = 72;
  public static final String CLIENT_CORRELATION_ID_PATTERN = "[a-zA-Z0-9-]*";

  /**
   * Initialize TracingContext
   * @param clientCorrelationID Provided over config by client
   * @param fileSystemID Unique guid for AzureBlobFileSystem instance
   * @param opType Code indicating the high-level Hadoop operation that
   *                    triggered the current Store request
   * @param tracingHeaderFormat Format of IDs to be printed in header and logs
   * @param listener Holds instance of TracingHeaderValidator during testing,
   *                null otherwise
   */
  public TracingContext(String clientCorrelationID, String fileSystemID,
      FSOperationType opType, TracingHeaderFormat tracingHeaderFormat,
      Listener listener) {
    this.fileSystemID = fileSystemID;
    this.opType = opType;
    this.clientCorrelationID = clientCorrelationID;
    streamID = EMPTY_STRING;
    retryCount = 0;
    primaryRequestId = EMPTY_STRING;
    format = tracingHeaderFormat;
    this.listener = listener;
  }

  public TracingContext(String clientCorrelationID, String fileSystemID,
      FSOperationType opType, boolean needsPrimaryReqId,
      TracingHeaderFormat tracingHeaderFormat, Listener listener) {
    this(clientCorrelationID, fileSystemID, opType, tracingHeaderFormat,
        listener);
    primaryRequestId = needsPrimaryReqId ? UUID.randomUUID().toString() : "";
    if (listener != null) {
      listener.updatePrimaryRequestID(primaryRequestId);
    }
  }

  public TracingContext(String clientCorrelationID, String fileSystemID,
      FSOperationType opType, boolean needsPrimaryReqId,
      TracingHeaderFormat tracingHeaderFormat, Listener listener, String metricResults) {
    this(clientCorrelationID, fileSystemID, opType, needsPrimaryReqId, tracingHeaderFormat,
        listener);
    this.metricResults = metricResults;
  }


  public TracingContext(TracingContext originalTracingContext) {
    this.fileSystemID = originalTracingContext.fileSystemID;
    this.streamID = originalTracingContext.streamID;
    this.clientCorrelationID = originalTracingContext.clientCorrelationID;
    this.opType = originalTracingContext.opType;
    this.retryCount = 0;
    this.primaryRequestId = originalTracingContext.primaryRequestId;
    this.format = originalTracingContext.format;
    this.position = originalTracingContext.getPosition();
    this.ingressHandler = originalTracingContext.getIngressHandler();
    this.operatedBlobCount = originalTracingContext.operatedBlobCount;
    if (originalTracingContext.listener != null) {
      this.listener = originalTracingContext.listener.getClone();
    }
    this.metricResults = originalTracingContext.metricResults;
    this.readType = originalTracingContext.readType;
  }
  public static String validateClientCorrelationID(String clientCorrelationID) {
    if ((clientCorrelationID.length() > MAX_CLIENT_CORRELATION_ID_LENGTH)
        || (!clientCorrelationID.matches(CLIENT_CORRELATION_ID_PATTERN))) {
      LOG.debug(
          "Invalid config provided; correlation id not included in header.");
      return EMPTY_STRING;
    }
    return clientCorrelationID;
  }

  public void setPrimaryRequestID() {
    primaryRequestId = UUID.randomUUID().toString();
    if (listener != null) {
      listener.updatePrimaryRequestID(primaryRequestId);
    }
  }

  public void setStreamID(String stream) {
    streamID = stream;
  }

  public void setOperation(FSOperationType operation) {
    this.opType = operation;
  }

  public int getRetryCount() {
    return retryCount;
  }

  public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }

  public void setListener(Listener listener) {
    this.listener = listener;
  }

  /**
   * Concatenate all components separated by (:) into a string and set into
   * X_MS_CLIENT_REQUEST_ID header of the http operation
   * Following are the components in order of concatenation:
   * <ul>
   *   <li>version - not present for versions less than v1</li>
   *   <li>clientCorrelationId</li>
   *   <li>clientRequestId</li>
   *   <li>fileSystemId</li>
   *   <li>primaryRequestId</li>
   *   <li>streamId</li>
   *   <li>opType</li>
   *   <li>retryHeader - this contains retryCount, failureReason and retryPolicy underscore separated</li>
   *   <li>ingressHandler</li>
   *   <li>position of read/write in the remote file</li>
   *   <li>operatedBlobCount - number of blobs operated on by this request</li>
   *   <li>operationSpecificHeader - different operation types can publish info relevant to that operation</li>
   *   <li>httpOperationHeader - suffix for network library used</li>
   * </ul>
   * @param httpOperation AbfsHttpOperation instance to set header into
   *                      connection
   * @param previousFailure Failure seen before this API trigger on same operation
   * from AbfsClient.
   * @param retryPolicyAbbreviation Retry policy used to get retry interval before this
   * API trigger on same operation from AbfsClient
   */
  public void constructHeader(AbfsHttpOperation httpOperation, String previousFailure, String retryPolicyAbbreviation) {
    clientRequestId = UUID.randomUUID().toString();
    switch (format) {
    case ALL_ID_FORMAT:
      header = TracingHeaderVersion.getCurrentVersion() + COLON
          + clientCorrelationID + COLON
          + clientRequestId + COLON
          + fileSystemID + COLON
          + getPrimaryRequestIdForHeader(retryCount > 0) + COLON
          + streamID + COLON
          + opType + COLON
          + getRetryHeader(previousFailure, retryPolicyAbbreviation) + COLON
          + ingressHandler + COLON
          + position + COLON
          + operatedBlobCount + COLON
          + getOperationSpecificHeader(opType) + COLON
          + httpOperation.getTracingContextSuffix();

      metricHeader += !(metricResults.trim().isEmpty()) ? metricResults  : EMPTY_STRING;
      break;
    case TWO_ID_FORMAT:
      header = TracingHeaderVersion.getCurrentVersion() + COLON
          + clientCorrelationID + COLON + clientRequestId;
      metricHeader += !(metricResults.trim().isEmpty()) ? metricResults  : EMPTY_STRING;
      break;
    default:
      //case SINGLE_ID_FORMAT
      header = TracingHeaderVersion.getCurrentVersion() + COLON
          + clientRequestId;
      metricHeader += !(metricResults.trim().isEmpty()) ? metricResults  : EMPTY_STRING;
    }
    if (listener != null) { //for testing
      listener.callTracingHeaderValidator(header, format);
    }
    httpOperation.setRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID, header);
    if (!metricHeader.equals(EMPTY_STRING)) {
      httpOperation.setRequestProperty(HttpHeaderConfigurations.X_MS_FECLIENT_METRICS, metricHeader);
    }
    /*
    * In case the primaryRequestId is an empty-string and if it is the first try to
    * API call (previousFailure shall be null), maintain the last part of clientRequestId's
    * UUID in primaryRequestIdForRetry. This field shall be used as primaryRequestId part
    * of the x-ms-client-request-id header in case of retry of the same API-request.
    */
    if (primaryRequestId.isEmpty() && previousFailure == null) {
      String[] clientRequestIdParts = clientRequestId.split(String.valueOf(CHAR_HYPHEN));
      primaryRequestIdForRetry = clientRequestIdParts[
          clientRequestIdParts.length - 1];
    }
  }

  /**
   * Provide value to be used as primaryRequestId part of x-ms-client-request-id header.
   * @param isRetry define if it's for a retry case.
   * @return {@link #primaryRequestIdForRetry}:If the {@link #primaryRequestId}
   * is an empty-string, and it's a retry iteration.
   * {@link #primaryRequestId} for other cases.
   */
  private String getPrimaryRequestIdForHeader(final Boolean isRetry) {
    if (!primaryRequestId.isEmpty() || !isRetry) {
      return primaryRequestId;
    }
    return primaryRequestIdForRetry;
  }

  /**
   * Get the retry header string in format retryCount_failureReason_retryPolicyAbbreviation
   * retryCount is always there and 0 for first request.
   * failureReason is null for first request
   * retryPolicyAbbreviation is only present when request fails with ConnectionTimeout
   * @param previousFailure Previous failure reason, null if not a retried request
   * @param retryPolicyAbbreviation Abbreviation of retry policy used to get retry interval
   * @return String representing the retry header
   */
  private String getRetryHeader(final String previousFailure, String retryPolicyAbbreviation) {
    String retryHeader = String.format("%d", retryCount);
    if (previousFailure == null) {
      return retryHeader;
    }
    if (CONNECTION_TIMEOUT_ABBREVIATION.equals(previousFailure) && retryPolicyAbbreviation != null) {
      return String.format("%s_%s_%s", retryHeader, previousFailure, retryPolicyAbbreviation);
    }
    return String.format("%s_%s", retryHeader, previousFailure);
  }

  /**
   * Get the operation specific header for the current operation type.
   * @param opType The operation type for which the header is needed
   * @return String representing the operation specific header
   */
  private String getOperationSpecificHeader(FSOperationType opType) {
    // Similar header can be added for other operations in the future.
    switch (opType) {
      case READ:
        return getReadSpecificHeader();
      default:
        return EMPTY_STRING; // no operation specific header
    }
  }

  /**
   * Get the operation specific header for read operations.
   * @return String representing the read specific header
   */
  private String getReadSpecificHeader() {
    // More information on read can be added to this header in the future.
    // As underscore separated values.
    String readHeader = String.format("%s", readType.toString());
    return readHeader;
  }

  public void setOperatedBlobCount(Integer count) {
    operatedBlobCount = count;
  }

  public FSOperationType getOpType() {
    return opType;
  }

  /**
   * Return header representing the request associated with the tracingContext
   * @return Header string set into X_MS_CLIENT_REQUEST_ID
   */
  public String getHeader() {
    return header;
  }

  /**
   * Gets the ingress handler.
   *
   * @return the ingress handler as a String.
   */
  public String getIngressHandler() {
    return ingressHandler;
  }

  /**
   * Gets the position.
   *
   * @return the position as a String.
   */
  public String getPosition() {
    return position;
  }

  /**
   * Sets the ingress handler.
   *
   * @param ingressHandler the ingress handler to set, must not be null.
   */
  public void setIngressHandler(final String ingressHandler) {
    this.ingressHandler = ingressHandler;
    if (listener != null) {
      listener.updateIngressHandler(ingressHandler);
    }
  }

  /**
   * Sets the position.
   *
   * @param position the position to set, must not be null.
   */
  public void setPosition(final String position) {
    this.position = position;
    if (listener != null) {
      listener.updatePosition(position);
    }
  }

  /**
   * Sets the read type for the current operation.
   * @param readType the read type to set, must not be null.
   */
  public void setReadType(ReadType readType) {
    this.readType = readType;
    if (listener != null) {
      listener.updateReadType(readType);
    }
  }
}
