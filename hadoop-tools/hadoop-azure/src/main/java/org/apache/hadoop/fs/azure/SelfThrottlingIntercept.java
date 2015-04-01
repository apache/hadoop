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
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RequestResult;
import com.microsoft.azure.storage.ResponseReceivedEvent;
import com.microsoft.azure.storage.SendingRequestEvent;
import com.microsoft.azure.storage.StorageEvent;

/*
 * Self throttling is implemented by hooking into send & response callbacks 
 * One instance of this class is created per operationContext so each blobUpload/blobDownload/etc.
 * 
 * Self throttling only applies to 2nd and subsequent packets of an operation.  This is a simple way to 
 * ensure it only affects bulk transfers and not every tiny request.
 * 
 * A blobDownload will involve sequential packet transmissions and so there are no concurrency concerns
 * A blobUpload will generally involve concurrent upload worker threads that share one operationContext and one throttling instance.
 *   -- we do not track the latencies for each worker thread as they are doing similar work and will rarely collide in practice.  
 *   -- concurrent access to lastE2Edelay must be protected.  
 *       -- volatile is necessary and should be sufficient to protect simple access to primitive values (java 1.5 onwards) 
 *       -- synchronized{} blocks are also used to be conservative and for easier maintenance.
 *   
 * If an operation were to perform concurrent GETs and PUTs there is the possibility of getting confused regarding
 * whether lastE2Edelay was a read or write measurement.  This scenario does not occur.
 *
 * readFactor  = target read throughput as factor of unrestricted throughput.
 * writeFactor = target write throughput as factor of unrestricted throughput.
 * 
 * As we introduce delays it is important to only measure the actual E2E latency and not the augmented latency
 * To achieve this, we fiddle the 'startDate' of the transfer tracking object.
 */


/**
 * 
 * Introduces delays in our Azure traffic to prevent overrunning the server-side throttling limits.
 *
 */
@InterfaceAudience.Private
public class SelfThrottlingIntercept {
  public static final Log LOG = LogFactory
      .getLog(SelfThrottlingIntercept.class);

  private final float readFactor;
  private final float writeFactor;
  private final OperationContext operationContext;

  // Concurrency: access to non-final members must be thread-safe
  private long lastE2Elatency;

  public SelfThrottlingIntercept(OperationContext operationContext, 
      float readFactor, float writeFactor) {
    this.operationContext = operationContext;
    this.readFactor = readFactor;
    this.writeFactor = writeFactor;
  }

  public static void hook(OperationContext operationContext, float readFactor,
      float writeFactor) {

    SelfThrottlingIntercept throttler = new SelfThrottlingIntercept(
        operationContext, readFactor, writeFactor);
    ResponseReceivedListener responseListener = throttler.new ResponseReceivedListener();
    SendingRequestListener sendingListener = throttler.new SendingRequestListener();

    operationContext.getResponseReceivedEventHandler().addListener(
        responseListener);
    operationContext.getSendingRequestEventHandler().addListener(
        sendingListener);
  }

  public void responseReceived(ResponseReceivedEvent event) {
    RequestResult result = event.getRequestResult();
    Date startDate = result.getStartDate();
    Date stopDate = result.getStopDate();
    long elapsed = stopDate.getTime() - startDate.getTime();

    synchronized (this) {
      this.lastE2Elatency = elapsed;
    }

    if (LOG.isDebugEnabled()) {
      int statusCode = result.getStatusCode();
      String etag = result.getEtag();
      HttpURLConnection urlConnection = (HttpURLConnection) event
          .getConnectionObject();
      int contentLength = urlConnection.getContentLength();
      String requestMethod = urlConnection.getRequestMethod();
      long threadId = Thread.currentThread().getId();
      LOG.debug(String
          .format(
              "SelfThrottlingIntercept:: ResponseReceived: threadId=%d, Status=%d, Elapsed(ms)=%d, ETAG=%s, contentLength=%d, requestMethod=%s",
              threadId, statusCode, elapsed, etag, contentLength, requestMethod));
    }
  }

  public void sendingRequest(SendingRequestEvent sendEvent) {
    long lastLatency;
    boolean operationIsRead; // for logging
    synchronized (this) {

      lastLatency = this.lastE2Elatency;
    }

    float sleepMultiple;
    HttpURLConnection urlConnection = (HttpURLConnection) sendEvent
        .getConnectionObject();

    // Azure REST API never uses POST, so PUT is a sufficient test for an
    // upload.
    if (urlConnection.getRequestMethod().equalsIgnoreCase("PUT")) {
      operationIsRead = false;
      sleepMultiple = (1 / writeFactor) - 1;
    } else {
      operationIsRead = true;
      sleepMultiple = (1 / readFactor) - 1;
    }

    long sleepDuration = (long) (sleepMultiple * lastLatency);
    if (sleepDuration < 0) {
      sleepDuration = 0;
    }

    if (sleepDuration > 0) {
      try {
        // Thread.sleep() is not exact but it seems sufficiently accurate for
        // our needs. If needed this could become a loop of small waits that
        // tracks actual
        // elapsed time.
        Thread.sleep(sleepDuration);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }

      // reset to avoid counting the sleep against request latency
      sendEvent.getRequestResult().setStartDate(new Date());
    }

    if (LOG.isDebugEnabled()) {
      boolean isFirstRequest = (lastLatency == 0);
      long threadId = Thread.currentThread().getId();
      LOG.debug(String
          .format(
              " SelfThrottlingIntercept:: SendingRequest:   threadId=%d, requestType=%s, isFirstRequest=%b, sleepDuration=%d",
              threadId, operationIsRead ? "read " : "write", isFirstRequest,
              sleepDuration));
    }
  }

  // simply forwards back to the main class.
  // this is necessary as our main class cannot implement two base-classes.
  @InterfaceAudience.Private
  class SendingRequestListener extends StorageEvent<SendingRequestEvent> {

    @Override
    public void eventOccurred(SendingRequestEvent event) {
      sendingRequest(event);
    }
  }

  // simply forwards back to the main class.
  // this is necessary as our main class cannot implement two base-classes.
  @InterfaceAudience.Private
  class ResponseReceivedListener extends StorageEvent<ResponseReceivedEvent> {

    @Override
    public void eventOccurred(ResponseReceivedEvent event) {
      responseReceived(event);
    }
  }
}