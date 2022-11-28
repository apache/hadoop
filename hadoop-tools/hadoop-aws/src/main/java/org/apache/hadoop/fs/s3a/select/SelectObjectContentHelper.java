/*
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

package org.apache.hadoop.fs.s3a.select;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AUtils;

import static org.apache.hadoop.fs.s3a.WriteOperationHelper.WriteOperationHelperCallbacks;

/**
 * Helper for SelectObjectContent queries against an S3 Bucket.
 */
public final class SelectObjectContentHelper {

  private SelectObjectContentHelper() {
  }

  /**
   * Execute an S3 Select operation.
   * @param writeOperationHelperCallbacks helper callbacks
   * @param source  source for selection
   * @param request Select request to issue.
   * @param action  the action for use in exception creation
   * @return the select response event stream publisher
   * @throws IOException on failure
   */
  public static SelectEventStreamPublisher select(
      WriteOperationHelperCallbacks writeOperationHelperCallbacks,
      Path source,
      SelectObjectContentRequest request,
      String action)
      throws IOException {
    try {
      Handler handler = new Handler();
      CompletableFuture<Void> selectOperationFuture =
          writeOperationHelperCallbacks.selectObjectContent(request, handler);
      return handler.eventPublisher(selectOperationFuture).join();
    } catch (Throwable e) {
      if (e instanceof CompletionException) {
        e = e.getCause();
      }
      IOException translated;
      if (e instanceof SdkException) {
        translated = S3AUtils.translateException(action, source,
            (SdkException)e);
      } else {
        translated = new IOException(e);
      }
      throw translated;
    }
  }

  private static class Handler implements SelectObjectContentResponseHandler {
    private volatile CompletableFuture<Pair<SelectObjectContentResponse,
        SdkPublisher<SelectObjectContentEventStream>>> responseAndPublisherFuture =
        new CompletableFuture<>();

    private volatile SelectObjectContentResponse response;

    public CompletableFuture<SelectEventStreamPublisher> eventPublisher(
        CompletableFuture<Void> selectOperationFuture) {
      return responseAndPublisherFuture.thenApply(p ->
          new SelectEventStreamPublisher(selectOperationFuture,
              p.getLeft(), p.getRight()));
    }

    @Override
    public void responseReceived(SelectObjectContentResponse selectObjectContentResponse) {
      this.response = selectObjectContentResponse;
    }

    @Override
    public void onEventStream(SdkPublisher<SelectObjectContentEventStream> publisher) {
      responseAndPublisherFuture.complete(Pair.of(response, publisher));
    }

    @Override
    public void exceptionOccurred(Throwable error) {
      responseAndPublisherFuture.completeExceptionally(error);
    }

    @Override
    public void complete() {
    }
  }
}
