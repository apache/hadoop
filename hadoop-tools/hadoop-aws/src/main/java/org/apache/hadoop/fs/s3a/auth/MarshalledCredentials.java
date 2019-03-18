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

package org.apache.hadoop.fs.s3a.auth;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.auth.delegation.DelegationTokenIOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SESSION_TOKEN;

/**
 * Stores the credentials for a session or for a full login.
 * This structure is {@link Writable}, so can be marshalled inside a
 * delegation token.
 *
 * The class is designed so that keys inside are kept non-null; to be
 * unset just set them to the empty string. This is to simplify marshalling.
 *
 * <i>Important: Add no references to any AWS SDK class, to
 * ensure it can be safely deserialized whenever the relevant token
 * identifier of a token type declared in this JAR is examined.</i>
 */
@InterfaceAudience.Private
public final class MarshalledCredentials implements Writable, Serializable {

  /**
   * Error text on invalid non-empty credentials: {@value}.
   */
  @VisibleForTesting
  public static final String INVALID_CREDENTIALS
      = "Invalid AWS credentials";

  /**
   * How long can any of the secrets be: {@value}.
   * This is much longer than the current tokens, but leaves space for
   * future enhancements.
   */
  private static final int MAX_SECRET_LENGTH = 8192;

  private static final long serialVersionUID = 8444610385533920692L;

  /**
   * Access key of IAM account.
   */
  private String accessKey = "";

  /**
   * Secret key of IAM account.
   */
  private String secretKey = "";

  /**
   * Optional session token.
   * If non-empty: the credentials can be converted into
   * session credentials.
   */
  private String sessionToken = "";

  /**
   * ARN of a role. Purely for diagnostics.
   */
  private String roleARN = "";

  /**
   * Expiry time milliseconds in UTC; the {@code Java.Util.Date} value.
   * 0 means "does not expire/unknown".
   */
  private long expiration;

  /**
   * Constructor.
   */
  public MarshalledCredentials() {
  }

  /**
   * Create from a set of properties.
   * No expiry time is expected/known here.
   * @param accessKey access key
   * @param secretKey secret key
   * @param sessionToken session token
   */
  public MarshalledCredentials(
      final String accessKey,
      final String secretKey,
      final String sessionToken) {
    this();
    this.accessKey = requireNonNull(accessKey);
    this.secretKey = requireNonNull(secretKey);
    this.sessionToken = sessionToken == null ? "" : sessionToken;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public String getSessionToken() {
    return sessionToken;
  }

  /**
   * Expiration; will be 0 for none known.
   * @return any expiration timestamp
   */
  public long getExpiration() {
    return expiration;
  }

  public void setExpiration(final long expiration) {
    this.expiration = expiration;
  }

  /**
   * Get a temporal representing the time of expiration, if there
   * is one.
   * This is here to wrap up expectations about timestamps and zones.
   * @return the expiration time.
   */
  public Optional<OffsetDateTime> getExpirationDateTime() {
    return expiration == 0
        ? Optional.empty()
        : Optional.of(
            OffsetDateTime.ofInstant(
                new Date(expiration).toInstant(),
                ZoneOffset.UTC));
  }

  public String getRoleARN() {
    return roleARN;
  }

  public void setRoleARN(String roleARN) {
    this.roleARN = requireNonNull(roleARN);
  }

  public void setAccessKey(final String accessKey) {
    this.accessKey = requireNonNull(accessKey, "access key");
  }

  public void setSecretKey(final String secretKey) {
    this.secretKey = requireNonNull(secretKey, "secret key");
  }

  public void setSessionToken(final String sessionToken) {
    this.sessionToken = requireNonNull(sessionToken, "session token");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MarshalledCredentials that = (MarshalledCredentials) o;
    return expiration == that.expiration &&
        Objects.equals(accessKey, that.accessKey) &&
        Objects.equals(secretKey, that.secretKey) &&
        Objects.equals(sessionToken, that.sessionToken) &&
        Objects.equals(roleARN, that.roleARN);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accessKey, secretKey, sessionToken, roleARN,
        expiration);
  }

  /**
   * String value MUST NOT include any secrets.
   * @return a string value for logging.
   */
  @Override
  public String toString() {
    if (isEmpty()) {
      return "Empty credentials";
    }

    String validity = isValid(CredentialTypeRequired.AnyNonEmpty)
        ? "valid"
        : "invalid";
    if (!hasSessionToken()) {
      // full credentials have the simplest string value.
      return "full credentials (" + validity + ")";
    } else {
      // session/role credentials may have an expiry and role ARN.
      return String.format("session credentials, expiry %s; %s(%s)",
          getExpirationDateTime()
              .map(x -> x.format(DateTimeFormatter.ISO_DATE_TIME))
              .orElse("unknown"),
          (isNotEmpty(roleARN)
              ? ("role \"" + roleARN + "\" ")
              : ""),
          validity);
    }
  }

  /**
   * Is this empty: does it contain any credentials at all?
   * This test returns true if either the access key or secret key is empty.
   * @return true if there are no credentials.
   */
  public boolean isEmpty() {
    return !(isNotEmpty(accessKey) && isNotEmpty(secretKey));
  }

  /**
   * Is this a valid set of credentials tokens?
   * @param required credential type required.
   * @return true if the requirements are met.
   */
  public boolean isValid(final CredentialTypeRequired required) {
    if (accessKey == null || secretKey == null || sessionToken == null) {
      // null fields are not permitted, empty is OK for marshalling around.
      return false;
    }
    // now look at whether values are set/unset.
    boolean hasAccessAndSecretKeys = isNotEmpty(accessKey)
        && isNotEmpty(secretKey);
    boolean hasSessionToken = hasSessionToken();
    switch (required) {

    case AnyIncludingEmpty:
      // this is simplest.
      return true;

    case Empty:
      // empty. ignore session value if the other keys are unset.
      return !hasAccessAndSecretKeys;

    case AnyNonEmpty:
      // just look for the access key and secret key being non-empty
      return hasAccessAndSecretKeys;

    case FullOnly:
      return hasAccessAndSecretKeys && !hasSessionToken;

    case SessionOnly:
      return hasAccessAndSecretKeys && hasSessionToken();

      // this is here to keep the IDE quiet
    default:
      return false;
    }
  }

  /**
   * Does this set of credentials have a session token.
   * @return true if there's a session token.
   */
  public boolean hasSessionToken() {
    return isNotEmpty(sessionToken);
  }

  /**
   * Write the token.
   * Only works if valid.
   * @param out stream to serialize to.
   * @throws IOException if the serialization failed.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    validate("Writing " + this + ": ",
        CredentialTypeRequired.AnyIncludingEmpty);
    Text.writeString(out, accessKey);
    Text.writeString(out, secretKey);
    Text.writeString(out, sessionToken);
    Text.writeString(out, roleARN);
    out.writeLong(expiration);
  }

  /**
   * Read in the fields.
   * @throws IOException IO problem
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    accessKey = Text.readString(in, MAX_SECRET_LENGTH);
    secretKey = Text.readString(in, MAX_SECRET_LENGTH);
    sessionToken = Text.readString(in, MAX_SECRET_LENGTH);
    roleARN = Text.readString(in, MAX_SECRET_LENGTH);
    expiration = in.readLong();
  }

  /**
   * Verify that a set of credentials is valid.
   * @throws DelegationTokenIOException if they aren't
   * @param message message to prefix errors;
   * @param typeRequired credential type required.
   */
  public void validate(final String message,
      final CredentialTypeRequired typeRequired) throws IOException {
    if (!isValid(typeRequired)) {
      throw new DelegationTokenIOException(message
          + buildInvalidCredentialsError(typeRequired));
    }
  }

  /**
   * Build an error string for when the credentials do not match
   * those required.
   * @param typeRequired credential type required.
   * @return an error string.
   */
  public String buildInvalidCredentialsError(
      final CredentialTypeRequired typeRequired) {
    if (isEmpty()) {
      return " " + MarshalledCredentialBinding.NO_AWS_CREDENTIALS;
    } else {
      return " " + INVALID_CREDENTIALS
          + " in " + toString() + " required: " + typeRequired;
    }
  }

  /**
   * Patch a configuration with the secrets.
   * This does not set any per-bucket options (it doesn't know the bucket...).
   * <i>Warning: once done the configuration must be considered sensitive.</i>
   * @param config configuration to patch
   */
  public void setSecretsInConfiguration(Configuration config) {
    config.set(ACCESS_KEY, accessKey);
    config.set(SECRET_KEY, secretKey);
    S3AUtils.setIfDefined(config, SESSION_TOKEN, sessionToken,
        "session credentials");
  }


  /**
   * Return a set of empty credentials.
   * These can be marshalled, but not used for login.
   * @return a new set of credentials.
   */
  public static MarshalledCredentials empty() {
    return new MarshalledCredentials("", "", "");
  }

  /**
   * Enumeration of credential types for use in validation methods.
   */
  public enum CredentialTypeRequired {
    /** No entry at all. */
    Empty("None"),
    /** Any credential type including "unset". */
    AnyIncludingEmpty("Full, Session or None"),
    /** Any credential type is OK. */
    AnyNonEmpty("Full or Session"),
    /** The credentials must be session or role credentials. */
    SessionOnly("Session"),
    /** Full credentials are required. */
    FullOnly("Full");

    private final String text;

    CredentialTypeRequired(final String text) {
      this.text = text;
    }

    public String getText() {
      return text;
    }

    @Override
    public String toString() {
      return getText();
    }
  }
}
