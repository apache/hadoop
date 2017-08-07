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

package org.apache.hadoop.fs.azure;

import com.microsoft.azure.storage.ErrorReceivingResponseEvent;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RequestResult;
import com.microsoft.azure.storage.ResponseReceivedEvent;
import com.microsoft.azure.storage.SendingRequestEvent;
import com.microsoft.azure.storage.StorageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;

import java.net.HttpURLConnection;

/**
 * Throttles Azure Storage read and write operations to achieve maximum
 * throughput by minimizing errors.  The errors occur when the account ingress
 * or egress limits are exceeded and the server-side throttles requests.
 * Server-side throttling causes the retry policy to be used, but the retry
 * policy sleeps for long periods of time causing the total ingress or egress
 * throughput to be as much as 35% lower than optimal.  The retry policy is also
 * after the fact, in that it applies after a request fails.  On the other hand,
 * the client-side throttling implemented here happens before requests are made
 * and sleeps just enough to minimize errors, allowing optimal ingress and/or
 * egress throughput.
 */
@InterfaceAudience.Private
final class ClientThrottlingIntercept {
  private static final Logger LOG = LoggerFactory.getLogger(
      ClientThrottlingIntercept.class);
  private static ClientThrottlingIntercept singleton = null;
  private ClientThrottlingAnalyzer readThrottler = null;
  private ClientThrottlingAnalyzer writeThrottler = null;

  // Hide default constructor
  private ClientThrottlingIntercept() {
    readThrottler = new ClientThrottlingAnalyzer("read");
    writeThrottler = new ClientThrottlingAnalyzer("write");
    LOG.debug("Client-side throttling is enabled for the WASB file system.");
  }

  static synchronized void initializeSingleton() {
    if (singleton == null) {
      singleton = new ClientThrottlingIntercept();
    }
  }

  static void hook(OperationContext context) {
    context.getErrorReceivingResponseEventHandler().addListener(
        new ErrorReceivingResponseEventHandler());
    context.getSendingRequestEventHandler().addListener(
        new SendingRequestEventHandler());
    context.getResponseReceivedEventHandler().addListener(
        new ResponseReceivedEventHandler());
  }

  private static void updateMetrics(HttpURLConnection conn,
                                    RequestResult result) {
    BlobOperationDescriptor.OperationType operationType
        = BlobOperationDescriptor.getOperationType(conn);
    int status = result.getStatusCode();
    long contentLength = 0;
    // If the socket is terminated prior to receiving a response, the HTTP
    // status may be 0 or -1.  A status less than 200 or greater than or equal
    // to 500 is considered an error.
    boolean isFailedOperation = (status < HttpURLConnection.HTTP_OK
        || status >= java.net.HttpURLConnection.HTTP_INTERNAL_ERROR);

    switch (operationType) {
      case AppendBlock:
      case PutBlock:
      case PutPage:
        contentLength = BlobOperationDescriptor.getContentLengthIfKnown(conn,
            operationType);
        if (contentLength > 0) {
          singleton.writeThrottler.addBytesTransferred(contentLength,
              isFailedOperation);
        }
        break;
      case GetBlob:
        contentLength = BlobOperationDescriptor.getContentLengthIfKnown(conn,
            operationType);
        if (contentLength > 0) {
          singleton.readThrottler.addBytesTransferred(contentLength,
              isFailedOperation);
        }
        break;
      default:
        break;
    }
  }

  /**
   * Called when a network error occurs before the HTTP status and response
   * headers are received. Client-side throttling uses this to collect metrics.
   *
   * @param event The connection, operation, and request state.
   */
  public static void errorReceivingResponse(ErrorReceivingResponseEvent event) {
    updateMetrics((HttpURLConnection) event.getConnectionObject(),
        event.getRequestResult());
  }

  /**
   * Called before the Azure Storage SDK sends a request. Client-side throttling
   * uses this to suspend the request, if necessary, to minimize errors and
   * maximize throughput.
   *
   * @param event The connection, operation, and request state.
   */
  public static void sendingRequest(SendingRequestEvent event) {
    BlobOperationDescriptor.OperationType operationType
        = BlobOperationDescriptor.getOperationType(
            (HttpURLConnection) event.getConnectionObject());
    switch (operationType) {
      case GetBlob:
        singleton.readThrottler.suspendIfNecessary();
        break;
      case AppendBlock:
      case PutBlock:
      case PutPage:
        singleton.writeThrottler.suspendIfNecessary();
        break;
      default:
        break;
    }
  }

  /**
   * Called after the Azure Storage SDK receives a response. Client-side
   * throttling uses this to collect metrics.
   *
   * @param event The connection, operation, and request state.
   */
  public static void responseReceived(ResponseReceivedEvent event) {
    updateMetrics((HttpURLConnection) event.getConnectionObject(),
        event.getRequestResult());
  }

  /**
   * The ErrorReceivingResponseEvent is fired when the Azure Storage SDK
   * encounters a network error before the HTTP status and response headers are
   * received.
   */
  @InterfaceAudience.Private
  static class ErrorReceivingResponseEventHandler
      extends StorageEvent<ErrorReceivingResponseEvent> {

    /**
     * Called when a network error occurs before the HTTP status and response
     * headers are received.  Client-side throttling uses this to collect
     * metrics.
     *
     * @param event The connection, operation, and request state.
     */
    @Override
    public void eventOccurred(ErrorReceivingResponseEvent event) {
      singleton.errorReceivingResponse(event);
    }
  }

  /**
   * The SendingRequestEvent is fired before the Azure Storage SDK sends a
   * request.
   */
  @InterfaceAudience.Private
  static class SendingRequestEventHandler
      extends StorageEvent<SendingRequestEvent> {

    /**
     * Called before the Azure Storage SDK sends a request. Client-side
     * throttling uses this to suspend the request, if necessary, to minimize
     * errors and maximize throughput.
     *
     * @param event The connection, operation, and request state.
     */
    @Override
    public void eventOccurred(SendingRequestEvent event) {
      singleton.sendingRequest(event);
    }
  }

  /**
   * The ResponseReceivedEvent is fired after the Azure Storage SDK receives a
   * response.
   */
  @InterfaceAudience.Private
  static class ResponseReceivedEventHandler
      extends StorageEvent<ResponseReceivedEvent> {

    /**
     * Called after the Azure Storage SDK receives a response. Client-side
     * throttling uses this
     * to collect metrics.
     *
     * @param event The connection, operation, and request state.
     */
    @Override
    public void eventOccurred(ResponseReceivedEvent event) {
      singleton.responseReceived(event);
    }
  }
}
