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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.store.audit.AuditSpan;

import static org.apache.hadoop.fs.s3a.Statistic.MULTIPART_UPLOAD_LIST;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfOperation;


/**
 * MultipartUtils upload-specific functions for use by S3AFileSystem and Hadoop
 * CLI.
 * The Audit span active when
 * {@link #listMultipartUploads(StoreContext, AmazonS3, String, int)}
 * was invoked is retained for all subsequent operations.
 */
public final class MultipartUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(MultipartUtils.class);

  /** Not instantiated. */
  private MultipartUtils() { }

  /**
   * List outstanding multipart uploads.
   * Package private: S3AFileSystem and tests are the users of this.
   *
   * @param storeContext store context
   * @param s3 AmazonS3 client to use.
   * @param prefix optional key prefix to narrow search.  If null then whole
   *               bucket will be searched.
   * @param maxKeys maximum batch size to request at a time from S3.
   * @return an iterator of matching uploads
   */
  static MultipartUtils.UploadIterator listMultipartUploads(
      final StoreContext storeContext,
      AmazonS3 s3,
      @Nullable String prefix,
      int maxKeys)
      throws IOException {
    return new MultipartUtils.UploadIterator(storeContext,
        s3,
        maxKeys,
        prefix);
  }

  /**
   * Simple RemoteIterator wrapper for AWS `listMultipartUpload` API.
   * Iterates over batches of multipart upload metadata listings.
   * All requests are in the StoreContext's active span
   * at the time the iterator was constructed.
   */
  static class ListingIterator implements
      RemoteIterator<MultipartUploadListing> {

    private final String prefix;

    private final RequestFactory requestFactory;

    private final int maxKeys;
    private final AmazonS3 s3;
    private final Invoker invoker;

    private final AuditSpan auditSpan;

    private final StoreContext storeContext;

    /**
     * Most recent listing results.
     */
    private MultipartUploadListing listing;

    /**
     * Indicator that this is the first listing.
     */
    private boolean firstListing = true;

    /**
     * Count of list calls made.
     */
    private int listCount = 0;

    ListingIterator(final StoreContext storeContext,
        AmazonS3 s3,
        @Nullable String prefix,
        int maxKeys) throws IOException {
      this.storeContext = storeContext;
      this.s3 = s3;
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
    public MultipartUploadListing next() throws IOException {
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
          + "; upload count " + listing.getMultipartUploads().size()
          + "; isTruncated=" + listing.isTruncated();
    }

    @Retries.RetryTranslated
    private void requestNextBatch() throws IOException {
      try (AuditSpan span = auditSpan.activate()) {
        ListMultipartUploadsRequest req = requestFactory
            .newListMultipartUploadsRequest(prefix);
        if (!firstListing) {
          req.setKeyMarker(listing.getNextKeyMarker());
          req.setUploadIdMarker(listing.getNextUploadIdMarker());
        }
        req.setMaxUploads(maxKeys);

        LOG.debug("[{}], Requesting next {} uploads prefix {}, " +
            "next key {}, next upload id {}", listCount, maxKeys, prefix,
            req.getKeyMarker(), req.getUploadIdMarker());
        listCount++;

        listing = invoker.retry("listMultipartUploads", prefix, true,
            trackDurationOfOperation(storeContext.getInstrumentation(),
                MULTIPART_UPLOAD_LIST.getSymbol(),
                () -> s3.listMultipartUploads(req)));
        LOG.debug("Listing found {} upload(s)",
            listing.getMultipartUploads().size());
        LOG.debug("New listing state: {}", this);
      }
    }
  }

  /**
   * Iterator over multipart uploads. Similar to
   * {@link org.apache.hadoop.fs.s3a.Listing.FileStatusListingIterator}, but
   * iterates over pending uploads instead of existing objects.
   */
  public static class UploadIterator
      implements RemoteIterator<MultipartUpload> {

    /**
     * Iterator for issuing new upload list requests from
     * where the previous one ended.
     */
    private ListingIterator lister;
    /** Current listing: the last upload listing we fetched. */
    private MultipartUploadListing listing;
    /** Iterator over the current listing. */
    private ListIterator<MultipartUpload> batchIterator;

    @Retries.RetryTranslated
    public UploadIterator(
        final StoreContext storeContext,
        AmazonS3 s3,
        int maxKeys,
        @Nullable String prefix)
        throws IOException {

      lister = new ListingIterator(storeContext, s3, prefix,
          maxKeys);
      requestNextBatch();
    }

    @Override
    public boolean hasNext() throws IOException {
      return (batchIterator.hasNext() || requestNextBatch());
    }

    @Override
    public MultipartUpload next() throws IOException {
      if (!hasNext())  {
        throw new NoSuchElementException();
      }
      return batchIterator.next();
    }

    private boolean requestNextBatch() throws IOException {
      if (lister.hasNext()) {
        listing = lister.next();
        batchIterator = listing.getMultipartUploads().listIterator();
        return batchIterator.hasNext();
      }
      return false;
    }
  }
}
