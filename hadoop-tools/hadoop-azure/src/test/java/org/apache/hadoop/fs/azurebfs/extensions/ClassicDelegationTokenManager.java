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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_DELEGATION_TOKEN_PROVIDER_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_DELEGATION_TOKEN;
import static org.apache.hadoop.fs.azurebfs.extensions.KerberizedAbfsCluster.newURI;
import static org.apache.hadoop.fs.azurebfs.extensions.StubAbfsTokenIdentifier.decodeIdentifier;

/**
 * This is a Stub DT manager for testing, one which
 * implements the the {@link CustomDelegationTokenManager} API, but
 * not the extended one.
 *
 * Member variables are updated as operations are performed, so
 * test cases can make assertions about the state of the plugin.
 */
public class ClassicDelegationTokenManager
    implements CustomDelegationTokenManager {

  private static final Logger LOG = LoggerFactory.getLogger(
      ClassicDelegationTokenManager.class);

  /**
   * Classname.
   */
  public static final String NAME
      = "org.apache.hadoop.fs.azurebfs.extensions.ClassicDelegationTokenManager";

  /**
   * If this the DT is unbound, this is used for the service kind.
   */
  public static final String UNSET = "abfs://user@unset.dfs.core.windows.net/";

  /**
   * The URI used when creating a token for an unset binding.
   */
  public static final URI UNSET_URI = newURI(UNSET);

  private URI fsURI;

  private boolean initialized;

  private boolean closed;

  private int renewals;

  private int cancellations;

  private int issued;

  private Text kind;

  private UserGroupInformation owner;

  private String canonicalServiceName;

  /**
   * Instantiate.
   */
  public ClassicDelegationTokenManager() {
  }

  @Override
  public void initialize(final Configuration configuration) throws IOException {
    initialized = true;
    owner = UserGroupInformation.getCurrentUser();
    LOG.info("Creating Stub DT manager for {}", owner.getUserName());
  }

  public void close() {
    closed = true;
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(final String renewer)
      throws IOException {
    // guarantees issued
    issued++;

    URI uri = fsURI != null ? fsURI : UNSET_URI;
    Text renewerT = new Text(renewer != null ? renewer : "");
    Token t = createToken(issued, uri, new Text(owner.getUserName()),
        renewerT);
    if (kind != null) {
      t.setKind(kind);
    }
    t.setService(createServiceText());
    LOG.info("Created token {}", t);
    return t;
  }

  public Text createServiceText() {
    return new Text(fsURI != null ? fsURI.toString() : UNSET);
  }

  /**
   * Create a token.
   *
   * @param sequenceNumber sequence number.
   * @param uri FS URI
   * @param owner FS owner
   * @param renewer renewer
   * @return a token.
   */
  public static Token<DelegationTokenIdentifier> createToken(
      final int sequenceNumber,
      final URI uri,
      final Text owner,
      final Text renewer) {
    StubAbfsTokenIdentifier id
        = new StubAbfsTokenIdentifier(uri, owner, renewer);
    id.setSequenceNumber(sequenceNumber);
    Token<DelegationTokenIdentifier> token = new Token(
        id,
        new TokenSecretManager());

    return token;
  }

  @Override
  public long renewDelegationToken(final Token<?> token) throws IOException {
    renewals++;
    decodeIdentifier(token);
    return 0;
  }

  @Override
  public void cancelDelegationToken(final Token<?> token) throws IOException {
    cancellations++;
    decodeIdentifier(token);
  }

  protected void innerBind(final URI uri, final Configuration conf)
      throws IOException {
    Preconditions.checkState(initialized, "Not initialized");
    Preconditions.checkState(fsURI == null, "already bound");
    fsURI = uri;
    canonicalServiceName = uri.toString();
    LOG.info("Bound to {}", fsURI);
  }

  public String getCanonicalServiceName() {
    return canonicalServiceName;
  }

  public void setCanonicalServiceName(final String canonicalServiceName) {
    this.canonicalServiceName = canonicalServiceName;
  }

  public URI getFsURI() {
    return fsURI;
  }

  public boolean isInitialized() {
    return initialized;
  }

  public boolean isBound() {
    return fsURI != null;
  }

  public boolean isClosed() {
    return closed;
  }

  public int getRenewals() {
    return renewals;
  }

  public int getCancellations() {
    return cancellations;
  }

  public int getIssued() {
    return issued;
  }

  public Text getKind() {
    return kind;
  }

  public void setKind(final Text kind) {
    this.kind = kind;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "StubDelegationTokenManager{");
    sb.append("fsURI=").append(fsURI);
    sb.append(", initialized=").append(initialized);
    sb.append(", closed=").append(closed);
    sb.append(", renewals=").append(renewals);
    sb.append(", cancellations=").append(cancellations);
    sb.append(", issued=").append(issued);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Patch a configuration to declare this the DT provider for a filesystem
   * built off the given configuration.
   * The ABFS Filesystem still needs to come up with security enabled.
   * @param conf configuration.
   * @return the patched configuration.
   */
  public static Configuration useClassicDTManager(Configuration conf) {
    conf.setBoolean(FS_AZURE_ENABLE_DELEGATION_TOKEN, true);
    conf.set(FS_AZURE_DELEGATION_TOKEN_PROVIDER_TYPE,
        ClassicDelegationTokenManager.NAME);
    return conf;
  }

  /**
   * Get the password to use in secret managers.
   * This is a constant; its just recalculated every time to stop findbugs
   * highlighting security risks of shared mutable byte arrays.
   * @return a password.
   */
  private static byte[] getSecretManagerPasssword() {
    return "non-password".getBytes(Charset.forName("UTF-8"));
  }

  /**
   * The secret manager always uses the same secret; the
   * factory for new identifiers is that of the token manager.
   */
  protected static class TokenSecretManager
      extends SecretManager<StubAbfsTokenIdentifier> {

    public TokenSecretManager() {
    }

    @Override
    protected byte[] createPassword(StubAbfsTokenIdentifier identifier) {
      return getSecretManagerPasssword();
    }

    @Override
    public byte[] retrievePassword(StubAbfsTokenIdentifier identifier)
        throws InvalidToken {
      return getSecretManagerPasssword();
    }

    @Override
    public StubAbfsTokenIdentifier createIdentifier() {
      return new StubAbfsTokenIdentifier();
    }
  }
}
