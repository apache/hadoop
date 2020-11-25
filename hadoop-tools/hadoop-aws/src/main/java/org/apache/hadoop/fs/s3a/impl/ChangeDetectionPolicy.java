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

import java.util.Locale;

import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.model.CopyResult;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.RemoteFileChangedException;

import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Object change detection policy.
 * Determines which attribute is used to detect change and what to do when
 * change is detected.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class ChangeDetectionPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChangeDetectionPolicy.class);

  @VisibleForTesting
  public static final String CHANGE_DETECTED = "change detected on client";

  private final Mode mode;
  private final boolean requireVersion;

  /**
   * Version support is only warned about once per S3A instance.
   * This still means that on a long-lived application which destroys
   * filesystems it'll appear once-per-query in the logs, but at least
   * it will not appear once per file read.
   */
  private final LogExactlyOnce logNoVersionSupport = new LogExactlyOnce(LOG);

  /**
   * The S3 object attribute used to detect change.
   */
  public enum Source {
    ETag(CHANGE_DETECT_SOURCE_ETAG),
    VersionId(CHANGE_DETECT_SOURCE_VERSION_ID),
    /** you can't ask for this explicitly outside of tests. */
    None("none");

    private final String source;

    Source(String source) {
      this.source = source;
    }

    private static Source fromString(String trimmed) {
      for (Source value : values()) {
        if (value.source.equals(trimmed)) {
          return value;
        }
      }
      LOG.warn("Unrecognized " + CHANGE_DETECT_SOURCE + " value: \"{}\"",
          trimmed);
      return fromString(CHANGE_DETECT_SOURCE_DEFAULT);
    }

    static Source fromConfiguration(Configuration configuration) {
      String trimmed = configuration.get(CHANGE_DETECT_SOURCE,
          CHANGE_DETECT_SOURCE_DEFAULT).trim()
          .toLowerCase(Locale.ENGLISH);
      return fromString(trimmed);
    }
  }

  /**
   * What to do when change is detected.
   */
  public enum Mode {
    /** Client side validation. */
    Client(CHANGE_DETECT_MODE_CLIENT),
    /** Server side validation. */
    Server(CHANGE_DETECT_MODE_SERVER),
    /** Warn but continue. */
    Warn(CHANGE_DETECT_MODE_WARN),
    /** No checks. */
    None(CHANGE_DETECT_MODE_NONE);

    private final String mode;

    Mode(String mode) {
      this.mode = mode;
    }

    private static Mode fromString(String trimmed) {
      for (Mode value : values()) {
        if (value.mode.equals(trimmed)) {
          return value;
        }
      }
      LOG.warn("Unrecognized " + CHANGE_DETECT_MODE + " value: \"{}\"",
          trimmed);
      return fromString(CHANGE_DETECT_MODE_DEFAULT);
    }

    static Mode fromConfiguration(Configuration configuration) {
      String trimmed = configuration.get(CHANGE_DETECT_MODE,
          CHANGE_DETECT_MODE_DEFAULT)
          .trim()
          .toLowerCase(Locale.ENGLISH);
      return fromString(trimmed);
    }
  }

  protected ChangeDetectionPolicy(Mode mode, boolean requireVersion) {
    this.mode = mode;
    this.requireVersion = requireVersion;
  }

  public Mode getMode() {
    return mode;
  }

  public abstract Source getSource();

  public boolean isRequireVersion() {
    return requireVersion;
  }

  public LogExactlyOnce getLogNoVersionSupport() {
    return logNoVersionSupport;
  }

  /**
   * Reads the change detection policy from Configuration.
   *
   * @param configuration the configuration
   * @return the policy
   */
  public static ChangeDetectionPolicy getPolicy(Configuration configuration) {
    Mode mode = Mode.fromConfiguration(configuration);
    Source source = Source.fromConfiguration(configuration);
    boolean requireVersion = configuration.getBoolean(
        CHANGE_DETECT_REQUIRE_VERSION, CHANGE_DETECT_REQUIRE_VERSION_DEFAULT);
    return createPolicy(mode, source, requireVersion);
  }

  /**
   * Create a policy.
   * @param mode mode pf checks
   * @param source source of change
   * @param requireVersion throw exception when no version available?
   * @return the policy
   */
  @VisibleForTesting
  public static ChangeDetectionPolicy createPolicy(final Mode mode,
      final Source source, final boolean requireVersion) {
    switch (source) {
    case ETag:
      return new ETagChangeDetectionPolicy(mode, requireVersion);
    case VersionId:
      return new VersionIdChangeDetectionPolicy(mode, requireVersion);
    default:
      return new NoChangeDetection();
    }
  }

  /**
   * String value for logging.
   * @return source and mode.
   */
  @Override
  public String toString() {
    return "Policy " + getSource() + "/" + getMode();
  }

  /**
   * Pulls the attribute this policy uses to detect change out of the S3 object
   * metadata.  The policy generically refers to this attribute as
   * {@code revisionId}.
   *
   * @param objectMetadata the s3 object metadata
   * @param uri the URI of the object
   * @return the revisionId string as interpreted by this policy, or potentially
   * null if the attribute is unavailable (such as when the policy says to use
   * versionId but object versioning is not enabled for the bucket).
   */
  public abstract String getRevisionId(ObjectMetadata objectMetadata,
      String uri);

  /**
   * Like {{@link #getRevisionId(ObjectMetadata, String)}}, but retrieves the
   * revision identifier from {@link S3ObjectAttributes}.
   *
   * @param s3Attributes the object attributes
   * @return the revisionId string as interpreted by this policy, or potentially
   * null if the attribute is unavailable (such as when the policy says to use
   * versionId but object versioning is not enabled for the bucket).
   */
  public abstract String getRevisionId(S3ObjectAttributes s3Attributes);

  /**
   * Like {{@link #getRevisionId(ObjectMetadata, String)}}, but retrieves the
   * revision identifier from {@link CopyResult}.
   *
   * @param copyResult the copy result
   * @return the revisionId string as interpreted by this policy, or potentially
   * null if the attribute is unavailable (such as when the policy says to use
   * versionId but object versioning is not enabled for the bucket).
   */
  public abstract String getRevisionId(CopyResult copyResult);

  /**
   * Applies the given {@link #getRevisionId(ObjectMetadata, String) revisionId}
   * as a server-side qualification on the {@code GetObjectRequest}.
   *
   * @param request the request
   * @param revisionId the revision id
   */
  public abstract void applyRevisionConstraint(GetObjectRequest request,
      String revisionId);

  /**
   * Applies the given {@link #getRevisionId(ObjectMetadata, String) revisionId}
   * as a server-side qualification on the {@code CopyObjectRequest}.
   *
   * @param request the request
   * @param revisionId the revision id
   */
  public abstract void applyRevisionConstraint(CopyObjectRequest request,
      String revisionId);

  /**
   * Applies the given {@link #getRevisionId(ObjectMetadata, String) revisionId}
   * as a server-side qualification on the {@code GetObjectMetadataRequest}.
   *
   * @param request the request
   * @param revisionId the revision id
   */
  public abstract void applyRevisionConstraint(GetObjectMetadataRequest request,
      String revisionId);

  /**
   * Takes appropriate action based on {@link #getMode() mode} when a change has
   * been detected.
   *
   * @param revisionId the expected revision id
   * @param newRevisionId the detected revision id
   * @param uri the URI of the object being accessed
   * @param position the position being read in the object
   * @param operation the operation being performed on the object (e.g. open or
   * re-open) that triggered the change detection
   * @param timesAlreadyDetected number of times a change has already been
   * detected on the current stream
   * @return a pair of: was a change detected, and any exception to throw.
   * If the change was detected, this updates a counter in the stream
   * statistics; If an exception was returned it is thrown after the counter
   * update.
   */
  public ImmutablePair<Boolean, RemoteFileChangedException> onChangeDetected(
      String revisionId,
      String newRevisionId,
      String uri,
      long position,
      String operation,
      long timesAlreadyDetected) {
    String positionText = position >= 0 ? (" at " + position) : "";
    switch (mode) {
    case None:
      // something changed; we don't care.
      return new ImmutablePair<>(false, null);
    case Warn:
      if (timesAlreadyDetected == 0) {
        // only warn on the first detection to avoid a noisy log
        LOG.warn(
            String.format(
                "%s change detected on %s %s%s. Expected %s got %s",
                getSource(), operation, uri, positionText, revisionId,
                newRevisionId));
        return new ImmutablePair<>(true, null);
      }
      return new ImmutablePair<>(false, null);
    case Client:
    case Server:
    default:
      // mode == Client or Server; will trigger on version failures
      // of getObjectMetadata even on server.
      return new ImmutablePair<>(true,
          new RemoteFileChangedException(uri,
              operation,
              String.format("%s "
                      + CHANGE_DETECTED
                      + " during %s%s."
                    + " Expected %s got %s",
              getSource(), operation, positionText, revisionId, newRevisionId)));
    }
  }

  /**
   * Change detection policy based on {@link ObjectMetadata#getETag() eTag}.
   */
  static class ETagChangeDetectionPolicy extends ChangeDetectionPolicy {

    ETagChangeDetectionPolicy(Mode mode, boolean requireVersion) {
      super(mode, requireVersion);
    }

    @Override
    public String getRevisionId(ObjectMetadata objectMetadata, String uri) {
      return objectMetadata.getETag();
    }

    @Override
    public String getRevisionId(S3ObjectAttributes s3Attributes) {
      return s3Attributes.getETag();
    }

    @Override
    public String getRevisionId(CopyResult copyResult) {
      return copyResult.getETag();
    }

    @Override
    public void applyRevisionConstraint(GetObjectRequest request,
        String revisionId) {
      if (revisionId != null) {
        LOG.debug("Restricting get request to etag {}", revisionId);
        request.withMatchingETagConstraint(revisionId);
      } else {
        LOG.debug("No etag revision ID to use as a constraint");
      }
    }

    @Override
    public void applyRevisionConstraint(CopyObjectRequest request,
        String revisionId) {
      if (revisionId != null) {
        LOG.debug("Restricting copy request to etag {}", revisionId);
        request.withMatchingETagConstraint(revisionId);
      } else {
        LOG.debug("No etag revision ID to use as a constraint");
      }
    }

    @Override
    public void applyRevisionConstraint(GetObjectMetadataRequest request,
        String revisionId) {
      LOG.debug("Unable to restrict HEAD request to etag; will check later");
    }

    @Override
    public Source getSource() {
      return Source.ETag;
    }

    @Override
    public String toString() {
      return "ETagChangeDetectionPolicy mode=" + getMode();
    }

  }

  /**
   * Change detection policy based on
   * {@link ObjectMetadata#getVersionId() versionId}.
   */
  static class VersionIdChangeDetectionPolicy extends
      ChangeDetectionPolicy {

    VersionIdChangeDetectionPolicy(Mode mode, boolean requireVersion) {
      super(mode, requireVersion);
    }

    @Override
    public String getRevisionId(ObjectMetadata objectMetadata, String uri) {
      String versionId = objectMetadata.getVersionId();
      if (versionId == null) {
        // this policy doesn't work if the bucket doesn't have object versioning
        // enabled (which isn't by default)
        getLogNoVersionSupport().warn(
            CHANGE_DETECT_MODE + " set to " + Source.VersionId
                + " but no versionId available while reading {}. "
                + "Ensure your bucket has object versioning enabled. "
                + "You may see inconsistent reads.",
            uri);
      }
      return versionId;
    }

    @Override
    public String getRevisionId(S3ObjectAttributes s3Attributes) {
      return s3Attributes.getVersionId();
    }

    @Override
    public String getRevisionId(CopyResult copyResult) {
      return copyResult.getVersionId();
    }

    @Override
    public void applyRevisionConstraint(GetObjectRequest request,
        String revisionId) {
      if (revisionId != null) {
        LOG.debug("Restricting get request to version {}", revisionId);
        request.withVersionId(revisionId);
      } else {
        LOG.debug("No version ID to use as a constraint");
      }
    }

    @Override
    public void applyRevisionConstraint(CopyObjectRequest request,
        String revisionId) {
      if (revisionId != null) {
        LOG.debug("Restricting copy request to version {}", revisionId);
        request.withSourceVersionId(revisionId);
      } else {
        LOG.debug("No version ID to use as a constraint");
      }
    }

    @Override
    public void applyRevisionConstraint(GetObjectMetadataRequest request,
        String revisionId) {
      if (revisionId != null) {
        LOG.debug("Restricting metadata request to version {}", revisionId);
        request.withVersionId(revisionId);
      } else {
        LOG.debug("No version ID to use as a constraint");
      }
    }

    @Override
    public Source getSource() {
      return Source.VersionId;
    }

    @Override
    public String toString() {
      return "VersionIdChangeDetectionPolicy mode=" + getMode();
    }
  }

  /**
   * Don't check for changes.
   */
  static class NoChangeDetection extends ChangeDetectionPolicy {

    NoChangeDetection() {
      super(Mode.None, false);
    }

    @Override
    public Source getSource() {
      return Source.None;
    }

    @Override
    public String getRevisionId(final ObjectMetadata objectMetadata,
        final String uri) {
      return null;
    }

    @Override
    public String getRevisionId(final S3ObjectAttributes s3ObjectAttributes) {
      return null;
    }

    @Override
    public String getRevisionId(CopyResult copyResult) {
      return null;
    }

    @Override
    public void applyRevisionConstraint(final GetObjectRequest request,
        final String revisionId) {

    }

    @Override
    public void applyRevisionConstraint(CopyObjectRequest request,
        String revisionId) {

    }

    @Override
    public void applyRevisionConstraint(GetObjectMetadataRequest request,
        String revisionId) {

    }

    @Override
    public String toString() {
      return "NoChangeDetection";
    }

  }
}
