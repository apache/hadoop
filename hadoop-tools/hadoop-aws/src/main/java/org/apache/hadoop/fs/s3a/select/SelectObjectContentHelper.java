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

public final class SelectObjectContentHelper {

  public static SelectEventStreamPublisher select(
      WriteOperationHelperCallbacks writeOperationHelperCallbacks,
      Path source,
      SelectObjectContentRequest request,
      String action)
      throws IOException {
    try {
      Handler handler = new Handler();
      CompletableFuture<Void> operationFuture =
          writeOperationHelperCallbacks.selectObjectContent(request, handler);
      return handler.eventPublisher(operationFuture).join();
    } catch (Throwable e) {
      if (e instanceof CompletionException) {
        e = e.getCause();
      }
      IOException translated;
      if (e instanceof SdkException) {
        translated = S3AUtils.translateExceptionV2(action, source.toString(),
            (SdkException)e);
      } else {
        translated = new IOException(e);
      }
      throw translated;
    }
  }

  private static class Handler implements SelectObjectContentResponseHandler {
    private volatile CompletableFuture<Pair<SelectObjectContentResponse,
        SdkPublisher<SelectObjectContentEventStream>>> future =
        new CompletableFuture<>();

    private volatile SelectObjectContentResponse response;

    public CompletableFuture<SelectEventStreamPublisher> eventPublisher(
        CompletableFuture<Void> operationFuture) {
      return future.thenApply(p ->
          new SelectEventStreamPublisher(operationFuture,
              p.getLeft(), p.getRight()));
    }

    @Override
    public void responseReceived(SelectObjectContentResponse response) {
      this.response = response;
    }

    @Override
    public void onEventStream(SdkPublisher<SelectObjectContentEventStream> publisher) {
      future.complete(Pair.of(response, publisher));
    }

    @Override
    public void exceptionOccurred(Throwable error) {
      future.completeExceptionally(error);
    }

    @Override
    public void complete() {
    }
  }
}
