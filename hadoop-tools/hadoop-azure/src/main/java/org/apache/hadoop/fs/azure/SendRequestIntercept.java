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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * request events to Azure storage.
 */
@InterfaceAudience.Private
public final class SendRequestIntercept extends StorageEvent<SendingRequestEvent> {

  public static final Log LOG = LogFactory.getLog(SendRequestIntercept.class);

  private static final String ALLOW_ALL_REQUEST_PRECONDITIONS = "*";
  private final StorageCredentials storageCreds;
  private final boolean allowConcurrentOOBIo;
  private final OperationContext opContext;

  /**
   * Getter returning the storage account credentials.
   * 
   * @return storageCreds - account storage credentials.
   */
  private StorageCredentials getCredentials() {
    return storageCreds;
  }

  /**
   * Query if out-of-band I/Os are allowed.
   * 
   * return allowConcurrentOOBIo - true if OOB I/O is allowed, and false
   * otherwise.
   */
  private boolean isOutOfBandIoAllowed() {
    return allowConcurrentOOBIo;
  }

  /**
   * Getter returning the operation context.
   * 
   * @return storageCreds - account storage credentials.
   */
  private OperationContext getOperationContext() {
    return opContext;
  }

  /**
   * Constructor for SendRequestThrottle.
   * 
   * @param storageCreds
   *          - storage account credentials for signing packets.
   * 
   */
  private SendRequestIntercept(StorageCredentials storageCreds,
      boolean allowConcurrentOOBIo, OperationContext opContext) {
    // Capture the send delay callback interface.
    this.storageCreds = storageCreds;
    this.allowConcurrentOOBIo = allowConcurrentOOBIo;
    this.opContext = opContext;
  }

  /**
   * Binds a new lister to the operation context so the WASB file system can
   * appropriately intercept sends. By allowing concurrent OOB I/Os, we bypass
   * the blob immutability check when reading streams.
   * 
   * @param opContext
   *          The operation context to bind to listener.
   * 
   * @param allowConcurrentOOBIo
   *          True if reads are allowed with concurrent OOB writes.
   */
  public static void bind(StorageCredentials storageCreds,
      OperationContext opContext, boolean allowConcurrentOOBIo) {
    SendRequestIntercept sendListener = new SendRequestIntercept(storageCreds,
        allowConcurrentOOBIo, opContext);
    opContext.getSendingRequestEventHandler().addListener(sendListener);
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
    if (urlConnection.getRequestMethod().equalsIgnoreCase("GET")
        && isOutOfBandIoAllowed()) {
      // If concurrent reads on OOB writes are allowed, reset the if-match
      // condition on the conditional header.
      urlConnection.setRequestProperty(HeaderConstants.IF_MATCH,
          ALLOW_ALL_REQUEST_PRECONDITIONS);

      // In the Java AzureSDK the packet is signed before firing the
      // SendRequest. Setting
      // the conditional packet header property changes the contents of the
      // packet, therefore the packet has to be re-signed.
      try {
        // Sign the request. GET's have no payload so the content length is
        // zero.
        StorageCredentialsHelper.signBlobAndQueueRequest(getCredentials(),
          urlConnection, -1L, getOperationContext());
      } catch (InvalidKeyException e) {
        // Log invalid key exception to track signing error before the send
        // fails.
        String errString = String.format(
            "Received invalid key exception when attempting sign packet."
                + " Cause: %s", e.getCause().toString());
        LOG.error(errString);
      } catch (StorageException e) {
        // Log storage exception to track signing error before the call fails.
        String errString = String.format(
            "Received storage exception when attempting to sign packet."
                + " Cause: %s", e.getCause().toString());
        LOG.error(errString);
      }
    }
  }
}
