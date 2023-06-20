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

import java.net.HttpURLConnection;
import java.security.InvalidKeyException;

import org.apache.hadoop.classification.InterfaceAudience;

import com.microsoft.azure.storage.Constants.HeaderConstants;
import com.microsoft.azure.storage.core.StorageCredentialsHelper;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.SendingRequestEvent;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageEvent;
import com.microsoft.azure.storage.StorageException;

/**
 * Manages the lifetime of binding on the operation contexts to intercept send
 * request events to Azure storage and allow concurrent OOB I/Os.
 */
@InterfaceAudience.Private
public final class SendRequestIntercept extends StorageEvent<SendingRequestEvent> {

  private static final String ALLOW_ALL_REQUEST_PRECONDITIONS = "*";

  /**
   * Hidden default constructor for SendRequestIntercept.
   */
  private SendRequestIntercept() {
  }

  /**
   * Binds a new lister to the operation context so the WASB file system can
   * appropriately intercept sends and allow concurrent OOB I/Os.  This
   * by-passes the blob immutability check when reading streams.
   *
   * @param opContext the operation context assocated with this request.
   */
  public static void bind(OperationContext opContext) {
    opContext.getSendingRequestEventHandler().addListener(new SendRequestIntercept());
  }

  /**
   * Handler which processes the sending request event from Azure SDK. The
   * handler simply sets reset the conditional header to make all read requests
   * unconditional if reads with concurrent OOB writes are allowed.
   * 
   * @param sendEvent
   *          - send event context from Windows Azure SDK.
   */
  @Override
  public void eventOccurred(SendingRequestEvent sendEvent) {

    if (!(sendEvent.getConnectionObject() instanceof HttpURLConnection)) {
      // Pass if there is no HTTP connection associated with this send
      // request.
      return;
    }

    // Capture the HTTP URL connection object and get size of the payload for
    // the request.
    HttpURLConnection urlConnection = (HttpURLConnection) sendEvent
        .getConnectionObject();

    // Determine whether this is a download request by checking that the request
    // method
    // is a "GET" operation.
    if (urlConnection.getRequestMethod().equalsIgnoreCase("GET")) {
      // If concurrent reads on OOB writes are allowed, reset the if-match
      // condition on the conditional header.
      urlConnection.setRequestProperty(HeaderConstants.IF_MATCH,
          ALLOW_ALL_REQUEST_PRECONDITIONS);
    }
  }
}
