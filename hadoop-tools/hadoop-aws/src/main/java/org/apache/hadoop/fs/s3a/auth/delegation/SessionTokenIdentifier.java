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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.io.Text;

import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.SESSION_TOKEN_KIND;

/**
 * A token identifier which contains a set of AWS session credentials,
 * credentials which will be valid until they expire.
 *
 * <b>Note 1:</b>
 * There's a risk here that the reference to {@link MarshalledCredentials}
 * may trigger a transitive load of AWS classes, a load which will
 * fail if the aws SDK isn't on the classpath.
 *
 * <b>Note 2:</b>
 * This class does support subclassing, but every subclass MUST declare itself
 * to be of a different token kind.
 * Otherwise the process for decoding tokens breaks.
 */
public class SessionTokenIdentifier extends
    AbstractS3ATokenIdentifier {

  /**
   * Session credentials: initially empty but non-null.
   */
  private MarshalledCredentials marshalledCredentials
      = new MarshalledCredentials();

  /**
   * Constructor for service loader use.
   * Created with the kind {@link DelegationConstants#SESSION_TOKEN_KIND}.
   * Subclasses MUST NOT subclass this; they must provide their own
   * token kind.
   */
  public SessionTokenIdentifier() {
    super(SESSION_TOKEN_KIND);
  }

  /**
   * Constructor for subclasses.
   * @param kind kind of token identifier, for storage in the
   * token kind to implementation map.
   */
  protected SessionTokenIdentifier(final Text kind) {
    super(kind);
  }

  /**
   * Constructor.
   * @param kind token kind.
   * @param owner token owner
   * @param uri filesystem URI.
   * @param marshalledCredentials credentials to marshall
   * @param encryptionSecrets encryption secrets
   * @param origin origin text for diagnostics.
   */
  public SessionTokenIdentifier(
      final Text kind,
      final Text owner,
      final URI uri,
      final MarshalledCredentials marshalledCredentials,
      final EncryptionSecrets encryptionSecrets,
      final String origin) {
    super(kind, uri, owner, origin, encryptionSecrets);
    this.marshalledCredentials = marshalledCredentials;
  }

  /**
   * Constructor.
   * @param kind token kind.
   * @param owner token owner
   * @param renewer token renewer
   * @param realUser real user running over proxy user
   * @param uri filesystem URI.
   */
  public SessionTokenIdentifier(final Text kind,
      final Text owner,
      final Text renewer,
      final Text realUser,
      final URI uri) {
    super(kind, owner, renewer, realUser, uri);
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    super.write(out);
    marshalledCredentials.write(out);
  }

  @Override
  public void readFields(final DataInput in)
      throws IOException {
    super.readFields(in);
    marshalledCredentials.readFields(in);
  }

  /**
   * Return the expiry time in seconds since 1970-01-01.
   * @return the time when the AWS credentials expire.
   */
  @Override
  public long getExpiryTime() {
    return marshalledCredentials.getExpiration();
  }

  /**
   * Get the marshalled credentials.
   * @return marshalled AWS credentials.
   */
  public MarshalledCredentials getMarshalledCredentials() {
    return marshalledCredentials;
  }

  /**
   * Add the (sanitized) marshalled credentials to the string value.
   * @return a string value for test assertions and debugging.
   */
  @Override
  public String toString() {
    return super.toString()
        + "; " + marshalledCredentials.toString();
  }
}
