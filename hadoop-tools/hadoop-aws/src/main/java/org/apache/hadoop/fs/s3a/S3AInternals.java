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
import java.nio.file.AccessDeniedException;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.store.audit.AuditEntryPoint;

/**
 * This is an unstable interface for access to S3A Internal state, S3 operations
 * and the S3 client connector itself.
 */
@InterfaceStability.Unstable
@InterfaceAudience.LimitedPrivate("testing/diagnostics")
public interface S3AInternals {

  /**
   * Returns the S3 client used by this filesystem.
   * Will log at debug.
   * <p>
   * <i>Warning</i>
   * This bypasses core S3A operations, including auditing.
   * It is intended for use in testing, diagnostics and for accessing
   * operations not available through the S3A connector itself.
   * <p>
   * Unless audit spans are created through the S3AFileSystem, make
   * sure that {@code fs.s3a.audit.reject.out.of.span.operations} is
   * set to false.
   * <p>
   * Mocking note: this is the same S3Client as is used by the owning
   * filesystem; changes to this client will be reflected by changes
   * in the behavior of that filesystem.
   * @param reason a justification for requesting access.
   * @return S3Client
   */
  S3Client getAmazonS3Client(String reason);

  /**
   * Get the region of a bucket.
   * Invoked from StoreContext; consider an entry point.
   * @return the region in which a bucket is located
   * @throws AccessDeniedException if the caller lacks permission.
   * @throws IOException on any failure.
   */
  @Retries.RetryTranslated
  @AuditEntryPoint
  String getBucketLocation() throws IOException;

  /**
   * Get the region of a bucket; fixing up the region so it can be used
   * in the builders of other AWS clients.
   * Requires the caller to have the AWS role permission
   * {@code s3:GetBucketLocation}.
   * Retry policy: retrying, translated.
   * @param bucketName the name of the bucket
   * @return the region in which a bucket is located
   * @throws AccessDeniedException if the caller lacks permission.
   * @throws IOException on any failure.
   */
  @AuditEntryPoint
  @Retries.RetryTranslated
  String getBucketLocation(String bucketName) throws IOException;

  /**
   * Low-level call to get at the object metadata.
   * Auditing: An audit entry point.
   * @param path path to the object. This will be qualified.
   * @return metadata
   * @throws IOException IO and object access problems.
   */
  @AuditEntryPoint
  @Retries.RetryTranslated
  HeadObjectResponse getObjectMetadata(Path path) throws IOException;

  /**
   * Get a shared copy of the AWS credentials, with its reference
   * counter updated.
   * Caller is required to call {@code close()} on this after
   * they have finished using it.
   * @param purpose what is this for? This is for logging
   * @return a reference to shared credentials.
   */
  AWSCredentialProviderList shareCredentials(String purpose);

  /**
   * Request bucket metadata.
   * @return the metadata
   * @throws UnknownStoreException the bucket is absent
   * @throws IOException  any other problem talking to S3
   */
  @AuditEntryPoint
  @Retries.RetryTranslated
  HeadBucketResponse getBucketMetadata() throws IOException;

  /**
   * Is multipart copy enabled?
   * @return true if the transfer manager is used to copy files.
   */
  boolean isMultipartCopyEnabled();

  /**
   * Abort multipart uploads under a path.
   * @param path path to abort uploads under.
   * @return a count of aborts
   * @throws IOException trouble; FileNotFoundExceptions are swallowed.
   */
  @AuditEntryPoint
  @Retries.RetryTranslated
  long abortMultipartUploads(Path path) throws IOException;
}
