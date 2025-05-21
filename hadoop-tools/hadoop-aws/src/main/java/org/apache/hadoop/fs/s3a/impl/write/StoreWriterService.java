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

package org.apache.hadoop.fs.s3a.impl.write;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.ProgressableProgressListener;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3ADataBlocks;
import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.fs.s3a.UploadInfo;
import org.apache.hadoop.fs.s3a.api.IORateLimiting;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.impl.ClientManager;
import org.apache.hadoop.fs.s3a.impl.InternalConstants;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.impl.UploadContentProviders;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Preconditions;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.S3AUtils.extractException;
import static org.apache.hadoop.fs.s3a.S3AUtils.getPutRequestLength;
import static org.apache.hadoop.fs.s3a.Statistic.MULTIPART_UPLOAD_LIST;
import static org.apache.hadoop.fs.s3a.Statistic.MULTIPART_UPLOAD_PART_PUT;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_MULTIPART_UPLOAD_ABORTED;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_MULTIPART_UPLOAD_INITIATED;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_MULTIPART_UPLOAD_LIST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_PUT_REQUESTS;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.CONTENT_TYPE_OCTET_STREAM;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.MULTIPART_ABORT_WRITE_CAPACITY;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfOperation;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfSupplier;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.util.Preconditions.checkState;

/**
 * Store Writing Operations.
 * The service is not ready to use until
 * {@link #bind(S3AStore, ClientManager, IORateLimiting)}
 * is invoked and the service started.
 */
public class StoreWriterService extends AbstractService
    implements StoreWriter {

  private static final Logger LOG =
      LoggerFactory.getLogger(StoreWriterService.class);

  /**
   * Store for some statistics invocations.
   */
  private S3AStore store;

  /**
   * Rate limiter (likely to be the store).
   */
  private IORateLimiting limiting;

  /**
   * SDK client.
   */
  private ClientManager clientManager;

  /**
   * Create the Service with the service name {@link #STORE_WRITER}.
   */
  public StoreWriterService() {
    this(STORE_WRITER);
  }

  /**
   * Constructor.
   * @param name service name
   */
  public StoreWriterService(final String name) {
    super(name);
  }

  /**
   * Bind to dependencies.
   * This MUST be called before service start
   * @param aStore store
   * @param manager sdk client manager
   * @param rateLimiting rate limiting.
   */
  public void bind(
      final S3AStore aStore,
      final ClientManager manager,
      final IORateLimiting rateLimiting) {
    this.store = requireNonNull(aStore);
    this.clientManager = manager;
    this.limiting = aStore;
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    requireNonNull(store);
  }

  /**
   * Check the service is running.
   * @throws IllegalStateException if not in STARTED.
   */
  public void checkRunning() throws IllegalStateException {
    Preconditions.checkState(isInState(STATE.STARTED),
        "Store is in state %s", getServiceState());
  }


  /**
   * Start a transfer-manager managed async PUT of an object,
   * incrementing the put requests and put bytes
   * counters.
   * <p>
   * It does not update the other counters,
   * as existing code does that as progress callbacks come in.
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * <p>
   * Because the operation is async, any stream supplied in the request
   * must reference data (files, buffers) which stay valid until the upload
   * completes.
   * Retry policy: N/A: the transfer manager is performing the upload.
   * Auditing: must be inside an audit span.
   * @param putObjectRequest the request
   * @param file the file to be uploaded
   * @param listener the progress listener for the request
   * @return the upload initiated
   * @throws IOException if transfer manager creation failed.
   */
  @Override
  @Retries.OnceRaw
  public UploadInfo putObject(
      PutObjectRequest putObjectRequest,
      File file,
      ProgressableProgressListener listener) throws IOException {
    checkRunning();
    long len = getPutRequestLength(putObjectRequest);
    LOG.debug("PUT {} bytes to {} via transfer manager ", len, putObjectRequest.key());
    store.incrementPutStartStatistics(len);

    FileUpload upload = store.getOrCreateTransferManager().uploadFile(
        UploadFileRequest.builder()
            .putObjectRequest(putObjectRequest)
            .source(file)
            .addTransferListener(listener)
            .build());

    return new UploadInfo(upload, len);
  }

  @Retries.OnceRaw("For PUT; post-PUT actions are RetryExceptionsSwallowed")
  @Override
  public PutObjectResponse putObjectDirect(PutObjectRequest putObjectRequest,
      PutObjectOptions putOptions,
      S3ADataBlocks.BlockUploadData uploadData,
      DurationTrackerFactory trackerFactory)
      throws SdkException {
    checkRunning();
    long len = putObjectRequest.contentLength();

    checkState(len >= 0, "Cannot PUT object of unknown length");
    LOG.debug("PUT {} bytes to {}", len, putObjectRequest.key());
    store.incrementPutStartStatistics(len);
    final UploadContentProviders.BaseContentProvider<?> provider =
        uploadData.getContentProvider();
    try {
      PutObjectResponse response =
          trackDurationOfSupplier(store.nonNullDurationTrackerFactory(trackerFactory),
              OBJECT_PUT_REQUESTS.getSymbol(),
              () -> getS3ClientUnchecked().putObject(putObjectRequest,
                  RequestBody.fromContentProvider(
                      provider,
                      provider.getSize(),
                      CONTENT_TYPE_OCTET_STREAM)));
      store.incrementPutCompletedStatistics(true, len);
      return response;
    } catch (SdkException e) {
      store.incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  /**
   * Wait for an upload to complete.
   * If the upload (or its result collection) failed, this is where
   * the failure is raised as an AWS exception.
   * Calls {@link S3AStore#incrementPutCompletedStatistics(boolean, long)}
   * to update the statistics.
   * @param key destination key
   * @param uploadInfo upload to wait for
   * @return the upload result
   * @throws IOException IO failure
   * @throws CancellationException if the wait() was cancelled
   */
  @Retries.OnceTranslated
  public CompletedFileUpload waitForUploadCompletion(String key, UploadInfo uploadInfo)
      throws IOException {
    checkRunning();
    FileUpload upload = uploadInfo.getFileUpload();
    try {
      CompletedFileUpload result = upload.completionFuture().join();
      store.incrementPutCompletedStatistics(true, uploadInfo.getLength());
      return result;
    } catch (CompletionException e) {
      LOG.info("Interrupted: aborting upload");
      store.incrementPutCompletedStatistics(false, uploadInfo.getLength());
      throw extractException("upload", key, e);
    }
  }

  @Retries.OnceRaw
  @Override
  public CreateMultipartUploadResponse initiateMultipartUpload(
      CreateMultipartUploadRequest request) {
    checkRunning();
    LOG.debug("Initiate multipart upload to {}", request.key());
    return trackDurationOfSupplier(store.getDurationTrackerFactory(),
        OBJECT_MULTIPART_UPLOAD_INITIATED.getSymbol(),
        () -> getS3ClientUnchecked().createMultipartUpload(request));
  }

  @Retries.OnceRaw
  @Override
  public UploadPartResponse uploadPart(
      final UploadPartRequest request,
      final RequestBody body,
      @Nullable final DurationTrackerFactory trackerFactory)
      throws AwsServiceException, UncheckedIOException {
    checkRunning();
    long len = request.contentLength();
    store.incrementPutStartStatistics(len);
    try {
      UploadPartResponse uploadPartResponse = trackDurationOfSupplier(
          store.nonNullDurationTrackerFactory(trackerFactory),
          MULTIPART_UPLOAD_PART_PUT.getSymbol(), () ->
              getS3ClientUnchecked().uploadPart(request, body));
      store.incrementPutCompletedStatistics(true, len);
      return uploadPartResponse;
    } catch (AwsServiceException e) {
      store.incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  @Retries.OnceRaw
  @Override
  public CompleteMultipartUploadResponse completeMultipartUpload(
      CompleteMultipartUploadRequest request) {
    return getS3ClientUnchecked().completeMultipartUpload(request);
  }

  @Retries.OnceTranslated
  @Override
  public AbortMultipartUploadResponse abortMultipartUpload(MultipartUpload upload) {
    checkRunning();

    String destKey = upload.key();
    String uploadId = upload.uploadId();
    if (LOG.isDebugEnabled()) {
      DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      LOG.debug("Aborting multipart upload {} to {} initiated by {} on {}",
          uploadId, destKey, upload.initiator(),
          df.format(Date.from(upload.initiated())));
    }
    return abortMultipartUpload(destKey, uploadId);
  }

  @Retries.OnceRaw
  @Override
  public AbortMultipartUploadResponse abortMultipartUpload(String destKey, String uploadId)
      throws AwsServiceException {
    checkRunning();
    LOG.debug("Aborting multipart upload {} to {}", uploadId, destKey);
    return trackDurationOfSupplier(store.getInstrumentation(),
        OBJECT_MULTIPART_UPLOAD_ABORTED.getSymbol(), () -> {
          limiting.acquireWriteCapacity(MULTIPART_ABORT_WRITE_CAPACITY);
          return getS3ClientUnchecked().abortMultipartUpload(
              store.getRequestFactory().newAbortMultipartUploadRequestBuilder(
                  destKey,
                  uploadId).build());
        });
  }

  @Retries.RetryTranslated
  @Override
  public void abortOutstandingMultipartUploads(long seconds, @Nullable String prefix, int maxKeys,
      StoreContext context)
      throws IOException {
    checkArgument(seconds >= 0);
    checkRunning();
    Instant purgeBefore = Instant.now().minusSeconds(seconds);
    LOG.debug("Purging outstanding multipart uploads older than {}",
        purgeBefore);
    context.getInvoker().retry("Purging multipart uploads",
        context.getBucket(), true,
        () -> {
          RemoteIterator<MultipartUpload> uploadIterator =
              listMultipartUploads(
                  context,
                  prefix,
                  maxKeys);

          while (uploadIterator.hasNext()) {
            MultipartUpload upload = uploadIterator.next();
            if (upload.initiated().compareTo(purgeBefore) < 0) {
              abortMultipartUpload(upload);
            }
          }
        });
  }

  @Retries.RetryTranslated
  @Override
  public List<MultipartUpload> listMultipartUploads(@Nullable String prefix)
      throws IOException {
    checkRunning();

    // add a trailing / if needed.
    String p = qualifyPrefix(prefix);
    return store.createStoreContext().getInvoker().retry("listMultipartUploads", p, true, () -> {
      final ListMultipartUploadsRequest request = store.getRequestFactory()
          .newListMultipartUploadsRequestBuilder(p).build();
      return trackDuration(store.getInstrumentation(), MULTIPART_UPLOAD_LIST.getSymbol(), () -> {
        limiting.acquireReadCapacity(InternalConstants.MULTIPART_LIST_READ_CAPACITY);
        return getS3ClientUnchecked().listMultipartUploads(request).uploads();
      });
    });
  }

  @Override
  @Retries.RetryTranslated
  public RemoteIterator<MultipartUpload> listMultipartUploads(
      final StoreContext storeContext,
      @Nullable String prefix,
      int maxKeys)
      throws IOException {
    checkRunning();

    // span is picked up retained in the listing.
    return new UploadIterator(storeContext,
        maxKeys,
        qualifyPrefix(prefix));
  }

  /**
   * Add a trailing / if needed.
   * @param prefix prefix; may be null or empty
   * @return the prefix to use in the listing operation.
   */
  private static String qualifyPrefix(@Nullable final String prefix) {
    if (prefix != null && !prefix.isEmpty() && !prefix.endsWith("/")) {
      return prefix + "/";
    } else {
      return prefix;
    }
  }

  /**
   * Simple RemoteIterator wrapper for AWS `listMultipartUpload` API.
   * Iterates over batches of multipart upload metadata listings.
   * All requests are in the StoreContext's active span
   * at the time the iterator was constructed.
   */
  public final class ListingIterator implements
      RemoteIterator<ListMultipartUploadsResponse> {

    private final String prefix;

    private final RequestFactory requestFactory;

    private final int maxKeys;

    private final Invoker invoker;

    private final AuditSpan auditSpan;

    private final StoreContext storeContext;

    /**
     * Most recent listing results.
     */
    private ListMultipartUploadsResponse listing;

    /**
     * Indicator that this is the first listing.
     */
    private boolean firstListing = true;

    /**
     * Count of list calls made.
     */
    private int listCount = 0;

    ListingIterator(final StoreContext storeContext,
        @Nullable String prefix,
        int maxKeys) throws IOException {
      this.storeContext = storeContext;
      this.requestFactory = storeContext.getRequestFactory();
      this.maxKeys = maxKeys;
      this.prefix = prefix;
      this.invoker = storeContext.getInvoker();
      this.auditSpan = storeContext.getActiveAuditSpan();

      // request the first listing.
      requestNextBatch();
    }

    /**
     * Iterator has data if it is either is the initial iteration, or
     * the last listing obtained was incomplete.
     * @throws IOException not thrown by this implementation.
     */
    @Override
    public boolean hasNext() throws IOException {
      if (listing == null) {
        // shouldn't happen, but don't trust AWS SDK
        return false;
      } else {
        return firstListing || listing.isTruncated();
      }
    }

    /**
     * Get next listing. First call, this returns initial set (possibly
     * empty) obtained from S3. Subsequent calls my block on I/O or fail.
     * @return next upload listing.
     * @throws IOException if S3 operation fails.
     * @throws NoSuchElementException if there are no more uploads.
     */
    @Override
    @Retries.RetryTranslated
    public ListMultipartUploadsResponse next() throws IOException {
      if (firstListing) {
        firstListing = false;
      } else {
        if (listing == null || !listing.isTruncated()) {
          // nothing more to request: fail.
          throw new NoSuchElementException("No more uploads under " + prefix);
        }
        // need to request a new set of objects.
        requestNextBatch();
      }
      return listing;
    }

    @Override
    public String toString() {
      return "Upload iterator: prefix " + prefix
          + "; list count " + listCount
          + "; upload count " +
          (listing != null ? listing.uploads().size() : "n/a")
          + "; isTruncated=" +
          (listing != null ? listing.isTruncated() : "n/a");
    }

    @Retries.RetryTranslated
    private void requestNextBatch() throws IOException {
      checkRunning();

      try (AuditSpan span = auditSpan.activate()) {
        ListMultipartUploadsRequest.Builder requestBuilder =
            requestFactory.newListMultipartUploadsRequestBuilder(prefix);
        if (!firstListing) {
          requestBuilder.keyMarker(listing.nextKeyMarker());
          requestBuilder.uploadIdMarker(listing.nextUploadIdMarker());
        }
        requestBuilder.maxUploads(maxKeys);

        ListMultipartUploadsRequest request = requestBuilder.build();

        LOG.debug("[{}], Requesting next {} uploads prefix {}, " +
                "next key {}, next upload id {}", listCount, maxKeys, prefix,
            request.keyMarker(), request.uploadIdMarker());
        listCount++;

        listing = invoker.retry("listMultipartUploads", prefix, true,
            trackDurationOfOperation(storeContext.getInstrumentation(),
                OBJECT_MULTIPART_UPLOAD_LIST.getSymbol(),
                () -> {
                  limiting.acquireReadCapacity(InternalConstants.MULTIPART_LIST_READ_CAPACITY);
                  return getS3ClientUnchecked().listMultipartUploads(requestBuilder.build());
                }));
        LOG.debug("Listing found {} upload(s)",
            listing.uploads().size());
        LOG.debug("New listing state: {}", this);
      }
    }
  }

  private S3Client getS3ClientUnchecked() {
    return clientManager.getOrCreateS3ClientUnchecked();
  }

  /**
   * Iterator over multipart uploads.
   */
  public final class UploadIterator
      implements RemoteIterator<MultipartUpload> {

    /**
     * Iterator for issuing new upload list requests from
     * where the previous one ended.
     */
    private final ListingIterator lister;

    /** Iterator over the current listing. */
    private ListIterator<MultipartUpload> batchIterator;

    /**
     * Construct an iterator to list uploads under a path.
     * @param storeContext store context
     * @param maxKeys max # of keys to list per batch
     * @param prefix prefix
     * @throws IOException listing failure.
     */
    @Retries.RetryTranslated
    public UploadIterator(
        final StoreContext storeContext,
        int maxKeys,
        @Nullable String prefix)
        throws IOException {

      lister = new ListingIterator(storeContext, prefix, maxKeys);
      requestNextBatch();
    }

    @Override
    @Retries.RetryTranslated
    public boolean hasNext() throws IOException {
      return (batchIterator.hasNext() || requestNextBatch());
    }

    @Override
    @Retries.RetryTranslated
    public MultipartUpload next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return batchIterator.next();
    }

    @Retries.RetryTranslated
    private boolean requestNextBatch() throws IOException {
      if (lister.hasNext()) {
        // Current listing: the last upload listing we fetched.
        ListMultipartUploadsResponse listing = lister.next();
        batchIterator = listing.uploads().listIterator();
        return batchIterator.hasNext();
      }
      return false;
    }
  }

}
