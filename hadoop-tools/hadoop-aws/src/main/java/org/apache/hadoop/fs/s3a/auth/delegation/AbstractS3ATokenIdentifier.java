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

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

import static java.util.Objects.requireNonNull;

/**
 * An S3A Delegation Token Identifier: contains the information needed
 * to talk to S3A.
 *
 * These are loaded via the service loader API an used in a map of
 * Kind to class, which is then looked up to deserialize token
 * identifiers of a given class.
 *
 * Every non-abstract class must provide
 * <ol>
 *   <li>Their unique token kind.</li>
 *   <li>An empty constructor.</li>
 *   <li>An entry in the resource file
 *   {@code /META-INF/services/org.apache.hadoop.security.token.TokenIdentifier}
 *   </li>
 * </ol>
 *
 * The base implementation contains
 * <ol>
 *   <li>The URI of the FS.</li>
 *   <li>Encryption secrets for use in the destination FS.</li>
 * </ol>
 * Subclasses are required to add whatever information is needed to authenticate
 * the user with the credential provider which their binding class will
 * provide.
 *
 * <i>Important: Add no references to any AWS SDK class, to
 * ensure it can be safely deserialized whenever the relevant token
 * identifier of a token type declared in this JAR is examined.</i>
 */
public abstract class AbstractS3ATokenIdentifier
    extends DelegationTokenIdentifier {

  /**
   * The maximum string length supported for text fields.
   */
  protected static final int MAX_TEXT_LENGTH = 8192;

  /** Canonical URI of the bucket. */
  private URI uri;

  /**
   * Encryption secrets to also marshall with any credentials.
   * Set during creation to ensure it is never null.
   */
  private EncryptionSecrets encryptionSecrets = new EncryptionSecrets();

  /**
   * Timestamp of creation.
   * This is set to the current time; it will be overridden when
   * deserializing data.
   */
  private long created = System.currentTimeMillis();

  /**
   * An origin string for diagnostics.
   */
  private String origin = "";

  /**
   * This marshalled UUID can be used in testing to verify transmission,
   * and reuse; as it is printed you can see what is happending too.
   */
  private String uuid = UUID.randomUUID().toString();

  /**
   * Constructor.
   * @param kind token kind.
   * @param uri filesystem URI.
   * @param owner token owner
   * @param origin origin text for diagnostics.
   * @param encryptionSecrets encryption secrets to set.
   */
  protected AbstractS3ATokenIdentifier(
      final Text kind,
      final URI uri,
      final Text owner,
      final String origin,
      final EncryptionSecrets encryptionSecrets) {
    this(kind, owner, new Text(), new Text(), uri);
    this.origin = requireNonNull(origin);
    this.encryptionSecrets = requireNonNull(encryptionSecrets);
  }

  /**
   * Constructor.
   * @param kind token kind.
   * @param owner token owner
   * @param renewer token renewer
   * @param realUser token real user
   * @param uri filesystem URI.
   */
  protected AbstractS3ATokenIdentifier(
      final Text kind,
      final Text owner,
      final Text renewer,
      final Text realUser,
      final URI uri) {
    super(kind, owner, renewer, realUser);
    this.uri = requireNonNull(uri);
  }

  /**
   * Build from a token.
   * This has been written for refresh operations;
   * if someone implements refresh it will be relevant.
   * @param kind token kind.
   * @param token to to build from
   * @throws IOException failure to build the identifier.
   */
  protected AbstractS3ATokenIdentifier(
      final Text kind,
      final Token<AbstractS3ATokenIdentifier> token) throws IOException {
    super(kind);
    ByteArrayInputStream bais = new ByteArrayInputStream(token.getIdentifier());
    readFields(new DataInputStream(bais));
  }

  /**
   * For subclasses to use in their own empty-constructors.
   * @param kind token kind.
   */
  protected AbstractS3ATokenIdentifier(final Text kind) {
    super(kind);
  }

  public String getBucket() {
    return uri.getHost();
  }

  public URI getUri() {
    return uri;
  }

  public String getOrigin() {
    return origin;
  }

  public void setOrigin(final String origin) {
    this.origin = origin;
  }

  public long getCreated() {
    return created;
  }

  /**
   * Write state.
   * {@link org.apache.hadoop.io.Writable#write(DataOutput)}.
   * @param out destination
   * @throws IOException failure
   */
  @Override
  public void write(final DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, uri.toString());
    Text.writeString(out, origin);
    Text.writeString(out, uuid);
    encryptionSecrets.write(out);
    out.writeLong(created);
  }

  /**
   * Read state.
   * {@link org.apache.hadoop.io.Writable#readFields(DataInput)}.
   *
   * Note: this operation gets called in toString() operations on tokens, so
   * must either always succeed, or throw an IOException to trigger the
   * catch and downgrade. RuntimeExceptions (e.g. Preconditions checks) are
   * not to be used here for this reason.)
   *
   * @param in input stream
   * @throws DelegationTokenIOException if the token binding is wrong.
   * @throws IOException IO problems.
   */
  @Override
  public void readFields(final DataInput in)
      throws DelegationTokenIOException, IOException {
    super.readFields(in);
    uri = URI.create(Text.readString(in, MAX_TEXT_LENGTH));
    origin = Text.readString(in, MAX_TEXT_LENGTH);
    uuid = Text.readString(in, MAX_TEXT_LENGTH);
    encryptionSecrets.readFields(in);
    created = in.readLong();
  }

  /**
   * Validate the token by looking at its fields.
   * @throws IOException on failure.
   */
  public void validate() throws IOException {
    if (uri == null) {
      throw new DelegationTokenIOException("No URI in " + this);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3ATokenIdentifier{");
    sb.append(getKind());
    sb.append("; uri=").append(uri);
    sb.append("; timestamp=").append(created);
    sb.append("; encryption=").append(encryptionSecrets.toString());
    sb.append("; ").append(uuid);
    sb.append("; ").append(origin);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Equality check is on superclass and UUID only.
   * @param o other.
   * @return true if the base class considers them equal and the URIs match.
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final AbstractS3ATokenIdentifier that = (AbstractS3ATokenIdentifier) o;
    return Objects.equals(uuid, that.uuid) &&
        Objects.equals(uri, that.uri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), uri);
  }

  /**
   * Return the expiry time in seconds since 1970-01-01.
   * @return the time when the session credential expire.
   */
  public long getExpiryTime() {
    return 0;
  }

  /**
   * Get the UUID of this token identifier.
   * @return a UUID.
   */
  public String getUuid() {
    return uuid;
  }

  /**
   * Get the encryption secrets.
   * @return the encryption secrets within this identifier.
   */
  public EncryptionSecrets getEncryptionSecrets() {
    return encryptionSecrets;
  }

  /**
   * Create the default origin text message with local hostname and
   * timestamp.
   * @return a string for token diagnostics.
   */
  public static String createDefaultOriginMessage() {
    return String.format("Created on %s at time %s.",
        NetUtils.getHostname(),
        java.time.Instant.now());
  }
}
