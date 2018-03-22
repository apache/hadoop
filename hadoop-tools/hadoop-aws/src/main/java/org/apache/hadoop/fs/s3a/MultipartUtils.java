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


/**
 * MultipartUtils upload-specific functions for use by S3AFileSystem and Hadoop
 * CLI.
 */
public final class MultipartUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(MultipartUtils.class);

  /** Not instantiated. */
  private MultipartUtils() { }

  /**
   * List outstanding multipart uploads.
   * Package private: S3AFileSystem and tests are the users of this.
   * @param s3 AmazonS3 client to use.
   * @param bucketName name of S3 bucket to use.
   * @param maxKeys maximum batch size to request at a time from S3.
   * @param prefix optional key prefix to narrow search.  If null then whole
   *               bucket will be searched.
   * @return an iterator of matching uploads
   */
  static MultipartUtils.UploadIterator listMultipartUploads(AmazonS3 s3,
      Invoker invoker, String bucketName, int maxKeys, @Nullable String prefix)
      throws IOException {
    return new MultipartUtils.UploadIterator(s3, invoker, bucketName, maxKeys,
        prefix);
  }

  /**
   * Simple RemoteIterator wrapper for AWS `listMultipartUpload` API.
   * Iterates over batches of multipart upload metadata listings.
   */
  static class ListingIterator implements
      RemoteIterator<MultipartUploadListing> {

    private final String bucketName;
    private final String prefix;
    private final int maxKeys;
    private final AmazonS3 s3;
    private final Invoker invoker;

    /**
     * Most recent listing results.
     */
    private MultipartUploadListing listing;

    /**
     * Indicator that this is the first listing.
     */
    private boolean firstListing = true;

    private int listCount = 1;

    ListingIterator(AmazonS3 s3, Invoker invoker, String bucketName,
        int maxKeys, @Nullable String prefix) throws IOException {
      this.s3 = s3;
      this.bucketName = bucketName;
      this.maxKeys = maxKeys;
      this.prefix = prefix;
      this.invoker = invoker;

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
      return "Upload iterator: prefix " + prefix + "; list count " +
          listCount + "; isTruncated=" + listing.isTruncated();
    }

    @Retries.RetryTranslated
    private void requestNextBatch() throws IOException {
      ListMultipartUploadsRequest req =
          new ListMultipartUploadsRequest(bucketName);
      if (prefix != null) {
        req.setPrefix(prefix);
      }
      if (!firstListing) {
        req.setKeyMarker(listing.getNextKeyMarker());
        req.setUploadIdMarker(listing.getNextUploadIdMarker());
      }
      req.setMaxUploads(listCount);

      LOG.debug("[{}], Requesting next {} uploads prefix {}, " +
          "next key {}, next upload id {}", listCount, maxKeys, prefix,
          req.getKeyMarker(), req.getUploadIdMarker());
      listCount++;

      listing = invoker.retry("listMultipartUploads", prefix, true,
          () -> s3.listMultipartUploads(req));
      LOG.debug("New listing state: {}", this);
    }
  }

  /**
   * Iterator over multipart uploads. Similar to
   * {@link org.apache.hadoop.fs.s3a.Listing.FileStatusListingIterator}, but
   * iterates over pending uploads instead of existing objects.
   */
  public static class UploadIterator
      implements RemoteIterator<MultipartUpload> {

    private ListingIterator lister;
    /** Current listing: the last upload listing we fetched. */
    private MultipartUploadListing listing;
    /** Iterator over the current listing. */
    private ListIterator<MultipartUpload> batchIterator;

    @Retries.RetryTranslated
    public UploadIterator(AmazonS3 s3, Invoker invoker, String bucketName,
        int maxKeys, @Nullable String prefix)
        throws IOException {

      lister = new ListingIterator(s3, invoker, bucketName, maxKeys, prefix);
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
