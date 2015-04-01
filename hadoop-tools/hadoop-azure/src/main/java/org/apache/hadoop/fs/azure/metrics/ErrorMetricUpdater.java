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

import static java.net.HttpURLConnection.HTTP_NOT_FOUND; //404
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;  //400
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR; //500

import org.apache.hadoop.classification.InterfaceAudience;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RequestResult;
import com.microsoft.azure.storage.ResponseReceivedEvent;
import com.microsoft.azure.storage.StorageEvent;


/**
 * An event listener to the ResponseReceived event from Azure Storage that will
 * update error metrics appropriately when it gets that event.
 */
@InterfaceAudience.Private
public final class ErrorMetricUpdater extends StorageEvent<ResponseReceivedEvent> {
  private final AzureFileSystemInstrumentation instrumentation;
  private final OperationContext operationContext;

  private ErrorMetricUpdater(OperationContext operationContext,
      AzureFileSystemInstrumentation instrumentation) {
    this.instrumentation = instrumentation;
    this.operationContext = operationContext;
  }

  /**
   * Hooks a new listener to the given operationContext that will update the
   * error metrics for the WASB file system appropriately in response to
   * ResponseReceived events.
   *
   * @param operationContext The operationContext to hook.
   * @param instrumentation The metrics source to update.
   */
  public static void hook(
      OperationContext operationContext,
      AzureFileSystemInstrumentation instrumentation) {
    ErrorMetricUpdater listener =
        new ErrorMetricUpdater(operationContext,
            instrumentation);
    operationContext.getResponseReceivedEventHandler().addListener(listener);
  }

  @Override
  public void eventOccurred(ResponseReceivedEvent eventArg) {
    RequestResult currentResult = operationContext.getLastResult();
    int statusCode = currentResult.getStatusCode();
    // Check if it's a client-side error: a 4xx status
    // We exclude 404 because it happens frequently during the normal
    // course of operation (each call to exists() would generate that
    // if it's not found).
    if (statusCode >= HTTP_BAD_REQUEST && statusCode < HTTP_INTERNAL_ERROR 
        && statusCode != HTTP_NOT_FOUND) {
      instrumentation.clientErrorEncountered();
    } else if (statusCode >= HTTP_INTERNAL_ERROR) {
      // It's a server error: a 5xx status. Could be an Azure Storage
      // bug or (more likely) throttling.
      instrumentation.serverErrorEncountered();
    }
  }
}
