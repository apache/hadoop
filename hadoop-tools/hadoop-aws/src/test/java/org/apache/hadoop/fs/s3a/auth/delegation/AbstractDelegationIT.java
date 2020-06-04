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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_BINDING;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.lookupS3ADelegationToken;

/**
 * superclass class for DT tests.
 */
public abstract class AbstractDelegationIT extends AbstractS3ATestBase {

  protected static final String YARN_RM = "yarn-rm@EXAMPLE.COM";

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractDelegationIT.class);

  /**
   * Look up a token from the submitted credentials.
   * @param submittedCredentials credentials
   * @param uri URI of the FS
   * @param kind required kind of the token (which is asserted on)
   * @return the token
   * @throws IOException IO failure
   */
  public static AbstractS3ATokenIdentifier lookupToken(
      Credentials submittedCredentials,
      URI uri,
      Text kind) throws IOException {
    final Token<AbstractS3ATokenIdentifier> token =
        requireNonNull(
            lookupS3ADelegationToken(submittedCredentials, uri),
            "No Token for " + uri);
    assertEquals("Kind of token " + token,
        kind,
        token.getKind());
    AbstractS3ATokenIdentifier tid
        = token.decodeIdentifier();
    LOG.info("Found for URI {}, token {}", uri, tid);
    return tid;
  }

  /**
   * Create credentials with the DTs of the given FS.
   * @param fs filesystem
   * @return a non-empty set of credentials.
   * @throws IOException failure to create.
   */
  protected static Credentials mkTokens(final S3AFileSystem fs)
      throws IOException {
    Credentials cred = new Credentials();
    fs.addDelegationTokens(AbstractDelegationIT.YARN_RM, cred);
    return cred;
  }

  /**
   * Create and Init an FS instance.
   * @param uri URI
   * @param conf config to use
   * @return the instance
   * @throws IOException failure to create/init
   */
  protected static S3AFileSystem newS3AInstance(final URI uri,
      final Configuration conf)
      throws IOException {
    S3AFileSystem fs = new S3AFileSystem();
    fs.initialize(uri, conf);
    return fs;
  }

  /**
   * Assert that a filesystem is bound to a DT; that is: it is a delegate FS.
   * @param fs filesystem
   * @param tokenKind the kind of the token to require
   */
  protected static void assertBoundToDT(final S3AFileSystem fs,
      final Text tokenKind) {
    final S3ADelegationTokens dtSupport = fs.getDelegationTokens().get();
    assertTrue("Expected bound to a delegation token: " + dtSupport,
        dtSupport.isBoundToDT());
    assertEquals("Wrong token kind",
        tokenKind, dtSupport.getBoundDT().get().getKind());
  }

  /**
   * Assert that the number of tokens created by an FS matches the
   * expected value.
   * @param fs filesystem
   * @param expected expected creation count.
   */
  protected static void assertTokenCreationCount(final S3AFileSystem fs,
      final int expected) {
    assertEquals("DT creation count from " + fs.getDelegationTokens().get(),
        expected,
        getTokenCreationCount(fs));
  }

  /**
   * Get the token creation count of a filesystem.
   * @param fs FS
   * @return creation count
   */
  private static int getTokenCreationCount(final S3AFileSystem fs) {
    return fs.getDelegationTokens()
        .map(S3ADelegationTokens::getCreationCount)
        .get();
  }

  /**
   * Patch the current config with the DT binding.
   * @param conf configuration to patch
   * @param binding binding to use
   */
  protected void enableDelegationTokens(Configuration conf, String binding) {
    removeBaseAndBucketOverrides(conf,
        DELEGATION_TOKEN_BINDING);
    LOG.info("Enabling delegation token support for {}", binding);
    conf.set(DELEGATION_TOKEN_BINDING, binding);
  }

  /**
   * Reset UGI info.
   */
  protected void resetUGI() {
    UserGroupInformation.reset();
  }

  /**
   * Bind the provider list to the args supplied.
   * At least one must be provided, to stop the default list being
   * picked up.
   * @param config configuration to patch.
   * @param bucket bucket to clear.
   * @param providerClassnames providers
   */
  protected void bindProviderList(String bucket,
      Configuration config,
      String... providerClassnames) {
    removeBaseAndBucketOverrides(bucket, config, AWS_CREDENTIALS_PROVIDER);
    assertTrue("No providers to bind to", providerClassnames.length > 0);
    config.setStrings(AWS_CREDENTIALS_PROVIDER, providerClassnames);
  }

  /**
   * Save a DT to a file.
   * @param tokenFile destination file
   * @param token token to save
   * @throws IOException failure
   */
  protected void saveDT(final File tokenFile, final Token<?> token)
      throws IOException {
    requireNonNull(token, "Null token");
    Credentials cred = new Credentials();
    cred.addToken(token.getService(), token);

    try(DataOutputStream out = new DataOutputStream(
        new FileOutputStream(tokenFile))) {
      cred.writeTokenStorageToStream(out);
    }
  }

  /**
   * Create and init an S3a DT instance, but don't start it.
   * @param conf conf to use
   * @return a new instance
   * @throws IOException IOE
   */
  public S3ADelegationTokens instantiateDTSupport(Configuration conf)
      throws IOException {
    S3AFileSystem fs = getFileSystem();
    S3ADelegationTokens tokens = new S3ADelegationTokens();
    tokens.bindToFileSystem(
        fs.getCanonicalUri(),
        fs.createStoreContext(),
        fs.createDelegationOperations());
    tokens.init(conf);
    return tokens;
  }
}
