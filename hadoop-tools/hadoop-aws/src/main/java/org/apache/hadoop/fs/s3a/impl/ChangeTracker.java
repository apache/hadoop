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

package org.apache.hadoop.fs.s3a.impl;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.model.CopyResult;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.NoVersionAttributeException;
import org.apache.hadoop.fs.s3a.RemoteFileChangedException;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.statistics.ChangeTrackerStatistics;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;

/**
 * Change tracking for input streams: the version ID or etag of the object is
 * tracked and compared on open/re-open.  An initial version ID or etag may or
 * may not be available, depending on usage (e.g. if S3Guard is utilized).
 *
 * Self-contained for testing and use in different streams.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ChangeTracker {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChangeTracker.class);

  /** {@code 412 Precondition Failed} (HTTP/1.1 - RFC 2616) */
  public static final int SC_PRECONDITION_FAILED = 412;
  public static final String CHANGE_REPORTED_BY_S3 = "Change reported by S3";

  /** Policy to use. */
  private final ChangeDetectionPolicy policy;

  /**
   * URI of file being read.
   */
  private final String uri;

  /**
   * Mismatch counter; expected to be wired up to StreamStatistics except
   * during testing.
   */
  private final ChangeTrackerStatistics versionMismatches;

  /**
   * Revision identifier (e.g. eTag or versionId, depending on change
   * detection policy).
   */
  private String revisionId;

  /**
   * Create a change tracker.
   * @param uri URI of object being tracked
   * @param policy policy to track.
   * @param versionMismatches reference to the version mismatch counter
   * @param s3ObjectAttributes attributes of the object, potentially including
   * an eTag or versionId to match depending on {@code policy}
   */
  public ChangeTracker(final String uri,
      final ChangeDetectionPolicy policy,
      final ChangeTrackerStatistics versionMismatches,
      final S3ObjectAttributes s3ObjectAttributes) {
    this.policy = checkNotNull(policy);
    this.uri = uri;
    this.versionMismatches = versionMismatches;
    this.revisionId = policy.getRevisionId(s3ObjectAttributes);
    if (revisionId != null) {
      LOG.debug("Tracker {} has revision ID for object at {}: {}",
          policy, uri, revisionId);
    }
  }

  public String getRevisionId() {
    return revisionId;
  }

  public ChangeDetectionPolicy.Source getSource() {
    return policy.getSource();
  }

  @VisibleForTesting
  public long getVersionMismatches() {
    return versionMismatches.getVersionMismatches();
  }

  /**
   * Apply any revision control set by the policy if it is to be
   * enforced on the server.
   * @param request request to modify
   * @return true iff a constraint was added.
   */
  public boolean maybeApplyConstraint(
      final GetObjectRequest request) {

    if (policy.getMode() == ChangeDetectionPolicy.Mode.Server
        && revisionId != null) {
      policy.applyRevisionConstraint(request, revisionId);
      return true;
    }
    return false;
  }

  /**
   * Apply any revision control set by the policy if it is to be
   * enforced on the server.
   * @param request request to modify
   * @return true iff a constraint was added.
   */
  public boolean maybeApplyConstraint(
      final CopyObjectRequest request) {

    if (policy.getMode() == ChangeDetectionPolicy.Mode.Server
        && revisionId != null) {
      policy.applyRevisionConstraint(request, revisionId);
      return true;
    }
    return false;
  }

  public boolean maybeApplyConstraint(
      final GetObjectMetadataRequest request) {

    if (policy.getMode() == ChangeDetectionPolicy.Mode.Server
        && revisionId != null) {
      policy.applyRevisionConstraint(request, revisionId);
      return true;
    }
    return false;
  }

  /**
   * Process the response from the server for validation against the
   * change policy.
   * @param object object returned; may be null.
   * @param operation operation in progress.
   * @param pos offset of read
   * @throws PathIOException raised on failure
   * @throws RemoteFileChangedException if the remote file has changed.
   */
  public void processResponse(final S3Object object,
      final String operation,
      final long pos) throws PathIOException {
    if (object == null) {
      // no object returned. Either mismatch or something odd.
      if (revisionId != null) {
        // the requirements of the change detection policy wasn't met: the
        // object was not returned.
        versionMismatches.versionMismatchError();
        throw new RemoteFileChangedException(uri, operation,
            String.format(CHANGE_REPORTED_BY_S3
                    + " during %s"
                    + " at position %s."
                    + " %s %s was unavailable",
                operation,
                pos,
                getSource(),
                getRevisionId()));
      } else {
        throw new PathIOException(uri, "No data returned from GET request");
      }
    }

    processMetadata(object.getObjectMetadata(), operation);
  }

  /**
   * Process the response from the server for validation against the
   * change policy.
   * @param copyResult result of a copy operation
   * @throws PathIOException raised on failure
   * @throws RemoteFileChangedException if the remote file has changed.
   */
  public void processResponse(final CopyResult copyResult)
      throws PathIOException {
    // ETag (sometimes, depending on encryption and/or multipart) is not the
    // same on the copied object as the original.  Version Id seems to never
    // be the same on the copy.  As such, there isn't really anything that
    // can be verified on the response, except that a revision ID is present
    // if required.
    String newRevisionId = policy.getRevisionId(copyResult);
    LOG.debug("Copy result {}: {}", policy.getSource(), newRevisionId);
    if (newRevisionId == null && policy.isRequireVersion()) {
      throw new NoVersionAttributeException(uri, String.format(
          "Change detection policy requires %s",
          policy.getSource()));
    }
  }

  /**
   * Process an exception generated against the change policy.
   * If the exception indicates the file has changed, this method throws
   * {@code RemoteFileChangedException} with the original exception as the
   * cause.
   * @param e the exception
   * @param operation the operation performed when the exception was
   * generated (e.g. "copy", "read", "select").
   * @throws RemoteFileChangedException if the remote file has changed.
   */
  public void processException(SdkBaseException e, String operation) throws
      RemoteFileChangedException {
    if (e instanceof AmazonServiceException) {
      AmazonServiceException serviceException = (AmazonServiceException) e;
      // This isn't really going to be hit due to
      // https://github.com/aws/aws-sdk-java/issues/1644
      if (serviceException.getStatusCode() == SC_PRECONDITION_FAILED) {
        versionMismatches.versionMismatchError();
        throw new RemoteFileChangedException(uri, operation, String.format(
            RemoteFileChangedException.PRECONDITIONS_FAILED
                + " on %s."
                + " Version %s was unavailable",
            getSource(),
            getRevisionId()),
            serviceException);
      }
    }
  }

  /**
   * Process metadata response from server for validation against the change
   * policy.
   * @param metadata metadata returned from server
   * @param operation operation in progress
   * @throws PathIOException raised on failure
   * @throws RemoteFileChangedException if the remote file has changed.
   */
  public void processMetadata(final ObjectMetadata metadata,
      final String operation) throws PathIOException {
    final String newRevisionId = policy.getRevisionId(metadata, uri);
    processNewRevision(newRevisionId, operation, -1);
  }

  /**
   * Validate a revision from the server against our expectations.
   * @param newRevisionId new revision.
   * @param operation operation in progress
   * @param pos offset in the file; -1 for "none"
   * @throws PathIOException raised on failure
   * @throws RemoteFileChangedException if the remote file has changed.
   */
  private void processNewRevision(final String newRevisionId,
      final String operation, final long pos) throws PathIOException {
    if (newRevisionId == null && policy.isRequireVersion()) {
      throw new NoVersionAttributeException(uri, String.format(
          "Change detection policy requires %s",
          policy.getSource()));
    }
    if (revisionId == null) {
      // revisionId may be null on first (re)open. Pin it so change can be
      // detected if object has been updated
      LOG.debug("Setting revision ID for object at {}: {}",
          uri, newRevisionId);
      revisionId = newRevisionId;
    } else if (!revisionId.equals(newRevisionId)) {
      LOG.debug("Revision ID changed from {} to {}",
          revisionId, newRevisionId);
      ImmutablePair<Boolean, RemoteFileChangedException> pair =
          policy.onChangeDetected(
              revisionId,
              newRevisionId,
              uri,
              pos,
              operation,
              versionMismatches.getVersionMismatches());
      if (pair.left) {
        // an mismatch has occurred: note it.
        versionMismatches.versionMismatchError();
      }
      if (pair.right != null) {
        // there's an exception to raise: do it
        throw pair.right;
      }
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "ChangeTracker{");
    sb.append(policy);
    sb.append(", revisionId='").append(revisionId).append('\'');
    sb.append('}');
    return sb.toString();
  }

}
