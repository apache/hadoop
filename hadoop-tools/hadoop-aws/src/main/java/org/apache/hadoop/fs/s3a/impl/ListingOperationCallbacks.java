package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.S3ListRequest;
import org.apache.hadoop.fs.s3a.S3ListResult;

/**
 * These are all the callbacks which
 * {@link org.apache.hadoop.fs.s3a.Listing} operations
 * need, derived from the actual appropriate S3AFileSystem
 * methods.
 */
public interface ListingOperationCallbacks {

  /**
   * Initiate a {@code listObjects} operation, incrementing metrics
   * in the process.
   *
   * Retry policy: retry untranslated.
   * @param request request to initiate
   * @return the results
   * @throws IOException if the retry invocation raises one (it shouldn't).
   */
  @Retries.RetryRaw
  S3ListResult listObjects(
          S3ListRequest request)
          throws IOException;

  /**
   * List the next set of objects.
   * Retry policy: retry untranslated.
   * @param request last list objects request to continue
   * @param prevResult last paged result to continue from
   * @return the next result object
   * @throws IOException none, just there for retryUntranslated.
   */
  @Retries.RetryRaw
  S3ListResult continueListObjects(
          S3ListRequest request,
          S3ListResult prevResult)
          throws IOException;

  /**
   * Build a {@link S3ALocatedFileStatus} from a {@link FileStatus} instance.
   * @param status file status
   * @return a located status with block locations set up from this FS.
   * @throws IOException IO Problems.
   */
  S3ALocatedFileStatus toLocatedFileStatus(
          S3AFileStatus status)
          throws IOException;
  /**
   * Create a {@code ListObjectsRequest} request against this bucket,
   * with the maximum keys returned in a query set by {@link ContextAccessors#getMaxKeys()}.
   * @param key key for request
   * @param delimiter any delimiter
   * @return the request
   */
  public S3ListRequest createListObjectsRequest(
          String key,
          String delimiter);
}
