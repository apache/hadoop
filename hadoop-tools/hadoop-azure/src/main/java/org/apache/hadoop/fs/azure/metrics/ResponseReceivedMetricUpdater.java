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

package org.apache.hadoop.fs.azure.metrics;

import java.net.HttpURLConnection;

import org.apache.hadoop.classification.InterfaceAudience;

import com.microsoft.azure.storage.Constants.HeaderConstants;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RequestResult;
import com.microsoft.azure.storage.ResponseReceivedEvent;
import com.microsoft.azure.storage.StorageEvent;


/**
 * An event listener to the ResponseReceived event from Azure Storage that will
 * update metrics appropriately when it gets that event.
 */
@InterfaceAudience.Private
public final class ResponseReceivedMetricUpdater extends StorageEvent<ResponseReceivedEvent> {

  private final AzureFileSystemInstrumentation instrumentation;
  private final BandwidthGaugeUpdater blockUploadGaugeUpdater;

  private ResponseReceivedMetricUpdater(OperationContext operationContext,
      AzureFileSystemInstrumentation instrumentation,
      BandwidthGaugeUpdater blockUploadGaugeUpdater) {
    this.instrumentation = instrumentation;
    this.blockUploadGaugeUpdater = blockUploadGaugeUpdater;
  }

  /**
   * Hooks a new listener to the given operationContext that will update the
   * metrics for the WASB file system appropriately in response to
   * ResponseReceived events.
   *
   * @param operationContext The operationContext to hook.
   * @param instrumentation The metrics source to update.
   * @param blockUploadGaugeUpdater The blockUploadGaugeUpdater to use.
   */
  public static void hook(
      OperationContext operationContext,
      AzureFileSystemInstrumentation instrumentation,
      BandwidthGaugeUpdater blockUploadGaugeUpdater) {
    ResponseReceivedMetricUpdater listener =
        new ResponseReceivedMetricUpdater(operationContext,
            instrumentation, blockUploadGaugeUpdater);
    operationContext.getResponseReceivedEventHandler().addListener(listener);
  }

  /**
   * Get the content length of the request in the given HTTP connection.
   * @param connection The connection.
   * @return The content length, or zero if not found.
   */
  private long getRequestContentLength(HttpURLConnection connection) {
    String lengthString = connection.getRequestProperty(
        HeaderConstants.CONTENT_LENGTH);
    if (lengthString != null){
      return Long.parseLong(lengthString);
    }
    else{
      return 0;
    }
  }

  /**
   * Gets the content length of the response in the given HTTP connection.
   * @param connection The connection.
   * @return The content length.
   */
  private long getResponseContentLength(HttpURLConnection connection) {
    return connection.getContentLength();
  }

  /**
   * Handle the response-received event from Azure SDK.
   */
  @Override
  public void eventOccurred(ResponseReceivedEvent eventArg) {
    instrumentation.webResponse();
    if (!(eventArg.getConnectionObject() instanceof HttpURLConnection)) {
      // Typically this shouldn't happen, but just let it pass
      return;
    }
    HttpURLConnection connection =
        (HttpURLConnection) eventArg.getConnectionObject();
    RequestResult currentResult = eventArg.getRequestResult();
    if (currentResult == null) {
      // Again, typically shouldn't happen, but let it pass
      return;
    }

    long requestLatency = currentResult.getStopDate().getTime() 
        - currentResult.getStartDate().getTime();

    if (currentResult.getStatusCode() == HttpURLConnection.HTTP_CREATED 
        && connection.getRequestMethod().equalsIgnoreCase("PUT")) {
      // If it's a PUT with an HTTP_CREATED status then it's a successful
      // block upload.
      long length = getRequestContentLength(connection);
      if (length > 0) {
        blockUploadGaugeUpdater.blockUploaded(
            currentResult.getStartDate(),
            currentResult.getStopDate(),
            length);
        instrumentation.rawBytesUploaded(length);
        instrumentation.blockUploaded(requestLatency);
      }
    } else if (currentResult.getStatusCode() == HttpURLConnection.HTTP_PARTIAL 
        && connection.getRequestMethod().equalsIgnoreCase("GET")) {
      // If it's a GET with an HTTP_PARTIAL status then it's a successful
      // block download.
      long length = getResponseContentLength(connection);
      if (length > 0) {
        blockUploadGaugeUpdater.blockDownloaded(
            currentResult.getStartDate(),
            currentResult.getStopDate(),
            length);
        instrumentation.rawBytesDownloaded(length);
        instrumentation.blockDownloaded(requestLatency);
      }
    }
  }
}
