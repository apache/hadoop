package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.fs.s3a.impl.model.ObjectInputStreamCallbacks;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.util.LambdaUtils;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

import static java.util.Objects.requireNonNull;

public class InputStreamCallbacksImpl implements ObjectInputStreamCallbacks {

  /**
   * Audit span to activate before each call.
   */
  private final AuditSpan auditSpan;

  private final S3AStore store;

  private final S3AFileSystemOperations fsHandler;

  private static final Logger LOG = LoggerFactory.getLogger(InputStreamCallbacksImpl.class);

  private final ThreadPoolExecutor unboundedThreadPool;

  /**
   * Create.
   * @param auditSpan Audit span to activate before each call.
   */
  public InputStreamCallbacksImpl(final AuditSpan auditSpan, final S3AStore store,
      S3AFileSystemOperations fsHandler, ThreadPoolExecutor unboundedThreadPool) {
    this.auditSpan = requireNonNull(auditSpan);
    this.store = requireNonNull(store);
    this.fsHandler = requireNonNull(fsHandler);
    this.unboundedThreadPool = requireNonNull(unboundedThreadPool);
  }

  /**
   * Closes the audit span.
   */
  @Override
  public void close()  {
    auditSpan.close();
  }

  @Override
  public GetObjectRequest.Builder newGetRequestBuilder(final String key) {
    // active the audit span used for the operation
    try (AuditSpan span = auditSpan.activate()) {
      return store.getRequestFactory().newGetObjectRequestBuilder(key);
    }
  }

  @Override
  public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request) throws
      IOException {
    // active the audit span used for the operation
    try (AuditSpan span = auditSpan.activate()) {
      return fsHandler.getObject(store, request, store.getRequestFactory());
    }
  }

  @Override
  public <T> CompletableFuture<T> submit(final CallableRaisingIOE<T> operation) {
    CompletableFuture<T> result = new CompletableFuture<>();
    unboundedThreadPool.submit(() ->
        LambdaUtils.eval(result, () -> {
          LOG.debug("Starting submitted operation in {}", auditSpan.getSpanId());
          try (AuditSpan span = auditSpan.activate()) {
            return operation.apply();
          } finally {
            LOG.debug("Completed submitted operation in {}", auditSpan.getSpanId());
          }
        }));
    return result;
  }
}
