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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.model.CopyResult;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.statistics.impl.CountingChangeTracker;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy.CHANGE_DETECTED;
import static org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy.createPolicy;
import static org.apache.hadoop.fs.s3a.impl.ChangeTracker.CHANGE_REPORTED_BY_S3;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test {@link ChangeTracker}.
 */
public class TestStreamChangeTracker extends HadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestStreamChangeTracker.class);

  public static final String BUCKET = "bucket";

  public static final String OBJECT = "object";

  public static final String DEST_OBJECT = "new_object";

  public static final String URI = "s3a://" + BUCKET + "/" + OBJECT;

  public static final Path PATH = new Path(URI);

  @Test
  public void testVersionCheckingHandlingNoVersions() throws Throwable {
    LOG.info("If an endpoint doesn't return versions, that's OK");
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Client,
        ChangeDetectionPolicy.Source.VersionId,
        false);
    assertFalse("Tracker should not have applied contraints " + tracker,
        tracker.maybeApplyConstraint(newGetObjectRequest()));
    tracker.processResponse(
        newResponse(null, null),
        "", 0);
    assertTrackerMismatchCount(tracker, 0);
  }

  @Test
  public void testVersionCheckingHandlingNoVersionsVersionRequired()
      throws Throwable {
    LOG.info("If an endpoint doesn't return versions but we are configured to"
        + "require them");
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Client,
        ChangeDetectionPolicy.Source.VersionId,
        true);
    expectNoVersionAttributeException(tracker, newResponse(null, null),
        "policy requires VersionId");
  }

  @Test
  public void testEtagCheckingWarn() throws Throwable {
    LOG.info("If an endpoint doesn't return errors, that's OK");
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Warn,
        ChangeDetectionPolicy.Source.ETag,
        false);
    assertFalse("Tracker should not have applied constraints " + tracker,
        tracker.maybeApplyConstraint(newGetObjectRequest()));
    tracker.processResponse(
        newResponse("e1", null),
        "", 0);
    tracker.processResponse(
        newResponse("e1", null),
        "", 0);
    tracker.processResponse(
        newResponse("e2", null),
        "", 0);
    assertTrackerMismatchCount(tracker, 1);
    // subsequent error triggers doesn't trigger another warning
    tracker.processResponse(
        newResponse("e2", null),
        "", 0);
    assertTrackerMismatchCount(tracker, 1);
  }

  @Test
  public void testVersionCheckingOnClient() throws Throwable {
    LOG.info("Verify the client-side version checker raises exceptions");
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Client,
        ChangeDetectionPolicy.Source.VersionId,
        false);
    assertFalse("Tracker should not have applied constraints " + tracker,
        tracker.maybeApplyConstraint(newGetObjectRequest()));
    tracker.processResponse(
        newResponse(null, "rev1"),
        "", 0);
    assertTrackerMismatchCount(tracker, 0);
    assertRevisionId(tracker, "rev1");
    GetObjectRequest request = newGetObjectRequest();
    expectChangeException(tracker,
        newResponse(null, "rev2"), "change detected");
    // mismatch was noted (so gets to FS stats)
    assertTrackerMismatchCount(tracker, 1);

    // another read causes another exception
    expectChangeException(tracker,
        newResponse(null, "rev2"), "change detected");
    // mismatch was noted again
    assertTrackerMismatchCount(tracker, 2);
  }

  @Test
  public void testVersionCheckingOnServer() throws Throwable {
    LOG.info("Verify the client-side version checker handles null-ness");
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Server,
        ChangeDetectionPolicy.Source.VersionId,
        false);
    assertFalse("Tracker should not have applied contraints " + tracker,
        tracker.maybeApplyConstraint(newGetObjectRequest()));
    tracker.processResponse(
        newResponse(null, "rev1"),
        "", 0);
    assertTrackerMismatchCount(tracker, 0);
    assertRevisionId(tracker, "rev1");
    GetObjectRequest request = newGetObjectRequest();
    assertConstraintApplied(tracker, request);
    // now, the tracker expects a null response
    expectChangeException(tracker, null, CHANGE_REPORTED_BY_S3);
    assertTrackerMismatchCount(tracker, 1);

    // now, imagine the server doesn't trigger a failure due to some
    // bug in its logic
    // we should still react to the reported value
    expectChangeException(tracker,
        newResponse(null, "rev2"),
        CHANGE_DETECTED);
  }

  @Test
  public void testVersionCheckingUpfrontETag() throws Throwable {
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Server,
        ChangeDetectionPolicy.Source.ETag,
        false,
        objectAttributes("etag1", "versionid1"));

    assertEquals("etag1", tracker.getRevisionId());
  }

  @Test
  public void testVersionCheckingUpfrontVersionId() throws Throwable {
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Server,
        ChangeDetectionPolicy.Source.VersionId,
        false,
        objectAttributes("etag1", "versionid1"));

    assertEquals("versionid1", tracker.getRevisionId());
  }

  @Test
  public void testVersionCheckingETagCopyServer() throws Throwable {
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Server,
        ChangeDetectionPolicy.Source.VersionId,
        false,
        objectAttributes("etag1", "versionid1"));
    assertConstraintApplied(tracker, newCopyObjectRequest());
  }

  @Test
  public void testVersionCheckingETagCopyClient() throws Throwable {
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Client,
        ChangeDetectionPolicy.Source.VersionId,
        false,
        objectAttributes("etag1", "versionid1"));
    assertFalse("Tracker should not have applied contraints " + tracker,
        tracker.maybeApplyConstraint(newCopyObjectRequest()));
  }

  @Test
  public void testCopyVersionIdRequired() throws Throwable {
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Client,
        ChangeDetectionPolicy.Source.VersionId,
        true,
        objectAttributes("etag1", "versionId"));

    expectNoVersionAttributeException(tracker, newCopyResult("etag1",
        null),
        "policy requires VersionId");
  }

  @Test
  public void testCopyETagRequired() throws Throwable {
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Client,
        ChangeDetectionPolicy.Source.ETag,
        true,
        objectAttributes("etag1", "versionId"));

    expectNoVersionAttributeException(tracker, newCopyResult(null,
        "versionId"),
        "policy requires ETag");
  }

  @Test
  public void testCopyVersionMismatch() throws Throwable {
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Server,
        ChangeDetectionPolicy.Source.ETag,
        true,
        objectAttributes("etag", "versionId"));

    // 412 is translated to RemoteFileChangedException
    // note: this scenario is never currently hit due to
    // https://github.com/aws/aws-sdk-java/issues/1644
    AmazonServiceException awsException =
        new AmazonServiceException("aws exception");
    awsException.setStatusCode(ChangeTracker.SC_PRECONDITION_FAILED);
    expectChangeException(tracker, awsException, "copy",
        RemoteFileChangedException.PRECONDITIONS_FAILED);

    // processing another type of exception does nothing
    tracker.processException(new SdkBaseException("foo"), "copy");
  }

  protected void assertConstraintApplied(final ChangeTracker tracker,
      final GetObjectRequest request) {
    assertTrue("Tracker should have applied contraints " + tracker,
        tracker.maybeApplyConstraint(request));
  }

  protected void assertConstraintApplied(final ChangeTracker tracker,
      final CopyObjectRequest request) throws PathIOException {
    assertTrue("Tracker should have applied contraints " + tracker,
        tracker.maybeApplyConstraint(request));
  }

  protected RemoteFileChangedException expectChangeException(
      final ChangeTracker tracker,
      final S3Object response,
      final String message) throws Exception {
    return expectException(tracker, response, message,
        RemoteFileChangedException.class);
  }

  protected RemoteFileChangedException expectChangeException(
      final ChangeTracker tracker,
      final SdkBaseException exception,
      final String operation,
      final String message) throws Exception {
    return expectException(tracker, exception, operation, message,
        RemoteFileChangedException.class);
  }

  protected PathIOException expectNoVersionAttributeException(
      final ChangeTracker tracker,
      final S3Object response,
      final String message) throws Exception {
    return expectException(tracker, response, message,
        NoVersionAttributeException.class);
  }

  protected PathIOException expectNoVersionAttributeException(
      final ChangeTracker tracker,
      final CopyResult response,
      final String message) throws Exception {
    return expectException(tracker, response, message,
        NoVersionAttributeException.class);
  }

  protected <T extends Exception> T expectException(
      final ChangeTracker tracker,
      final S3Object response,
      final String message,
      final Class<T> clazz) throws Exception {
    return intercept(
        clazz,
        message,
        () -> {
          tracker.processResponse(response, "", 0);
          return tracker;
        });
  }

  protected <T extends Exception> T expectException(
      final ChangeTracker tracker,
      final CopyResult response,
      final String message,
      final Class<T> clazz) throws Exception {
    return intercept(
        clazz,
        message,
        () -> {
          tracker.processResponse(response);
          return tracker;
        });
  }

  protected <T extends Exception> T expectException(
      final ChangeTracker tracker,
      final SdkBaseException exception,
      final String operation,
      final String message,
      final Class<T> clazz) throws Exception {
    return intercept(
        clazz,
        message,
        () -> {
          tracker.processException(exception, operation);
          return tracker;
        });
  }

  protected void assertRevisionId(final ChangeTracker tracker,
      final String revId) {
    assertEquals("Wrong revision ID in " + tracker,
        revId, tracker.getRevisionId());
  }


  protected void assertTrackerMismatchCount(
      final ChangeTracker tracker,
      final int expectedCount) {
    assertEquals("counter in tracker " + tracker,
        expectedCount, tracker.getVersionMismatches());
  }

  /**
   * Create tracker.
   * Contains standard assertions(s).
   * @return the tracker.
   */
  protected ChangeTracker newTracker(final ChangeDetectionPolicy.Mode mode,
      final ChangeDetectionPolicy.Source source, boolean requireVersion) {
    return newTracker(mode, source, requireVersion,
        objectAttributes(null, null));
  }

  /**
   * Create tracker.
   * Contains standard assertions(s).
   * @return the tracker.
   */
  protected ChangeTracker newTracker(final ChangeDetectionPolicy.Mode mode,
      final ChangeDetectionPolicy.Source source, boolean requireVersion,
      S3ObjectAttributes objectAttributes) {
    ChangeDetectionPolicy policy = createPolicy(
        mode,
        source,
        requireVersion);
    ChangeTracker tracker = new ChangeTracker(URI, policy,
        new CountingChangeTracker(), objectAttributes);
    if (objectAttributes.getVersionId() == null
        && objectAttributes.getETag() == null) {
      assertFalse("Tracker should not have applied constraints " + tracker,
          tracker.maybeApplyConstraint(newGetObjectRequest()));
    }
    return tracker;
  }

  private GetObjectRequest newGetObjectRequest() {
    return new GetObjectRequest(BUCKET, OBJECT);
  }

  private CopyObjectRequest newCopyObjectRequest() {
    return new CopyObjectRequest(BUCKET, OBJECT, BUCKET, DEST_OBJECT);
  }

  private CopyResult newCopyResult(String eTag, String versionId) {
    CopyResult copyResult = new CopyResult();
    copyResult.setSourceBucketName(BUCKET);
    copyResult.setSourceKey(OBJECT);
    copyResult.setDestinationBucketName(BUCKET);
    copyResult.setDestinationKey(DEST_OBJECT);
    copyResult.setETag(eTag);
    copyResult.setVersionId(versionId);
    return copyResult;
  }

  private S3Object newResponse(String etag, String versionId) {
    ObjectMetadata md = new ObjectMetadata();
    if (etag != null) {
      md.setHeader(Headers.ETAG, etag);
    }
    if (versionId != null) {
      md.setHeader(Headers.S3_VERSION_ID, versionId);
    }
    S3Object response = emptyResponse();
    response.setObjectMetadata(md);
    return response;
  }

  private S3Object emptyResponse() {
    S3Object response = new S3Object();
    response.setBucketName(BUCKET);
    response.setKey(OBJECT);
    return response;
  }

  private S3ObjectAttributes objectAttributes(
      String etag, String versionId) {
    return new S3ObjectAttributes(BUCKET,
        PATH,
        OBJECT,
        null,
        null,
        etag,
        versionId,
        0);
  }
}
