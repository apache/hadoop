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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_SECONDARY_BINDINGS;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_BINDING;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.ENCRYPTING_SECONDARY_TOKEN_NAME;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.FULL_SECONDARY_TOKEN_NAME;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.INJECTING_ISSUE_TOKENS;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.INJECTING_SECONDARY_TOKEN_NAME;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.SESSION_SECONDARY_TOKEN_NAME;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.getTokenServiceForKind;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.lookupS3ADelegationToken;
import static org.apache.hadoop.test.LambdaTestUtils.doAs;

/**
 * superclass class for DT tests.
 */
public abstract class AbstractDelegationIT extends AbstractS3ATestBase {

  protected static final String YARN_RM = "yarn-rm@EXAMPLE.COM";

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractDelegationIT.class);

  /**
   * Get the tail of a list.
   * @param list non-empty source list
   * @param <T> type of list
   * @return tail value
   */
  public static <T> T tail(final List<T> list) {

    int size = list.size();
    checkArgument(size > 0);
    return list.get(size - 1);
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    resetAllDTConfigOptions(conf);
    return conf;
  }

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
   * Look up a secondary token from the submitted credentials.
   * @param submittedCredentials credentials
   * @param uri URI of the FS
   * @param kind required kind of the token (which is asserted on)
   * @return the token
   * @throws IOException IO failure
   */
  public static AbstractS3ATokenIdentifier lookupSecondaryToken(
      Credentials submittedCredentials,
      URI uri,
      Text kind) throws IOException {
    final Token<AbstractS3ATokenIdentifier> token =
        requireNonNull(
            S3ADelegationTokens.lookupToken(submittedCredentials,
                getTokenServiceForKind(uri.toString(), kind.toString())),
            "No Token for " + uri);
    assertEquals("Kind of token " + token,
        kind,
        token.getKind());
    AbstractS3ATokenIdentifier tid
        = token.decodeIdentifier();
    LOG.info("Found secondary token for URI {}, token {}", uri, tid);
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
   * Clear all base and bucket options for delegation
   * token testing.
   * @param conf configuration to patch.
   */
  protected void resetAllDTConfigOptions(final Configuration conf) {
    removeBaseAndBucketOverrides(conf,
        DELEGATION_TOKEN_BINDING,
        DELEGATION_SECONDARY_BINDINGS,
        SERVER_SIDE_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_KEY,
        INJECTING_SECONDARY_TOKEN_NAME,
        INJECTING_ISSUE_TOKENS,
        FULL_SECONDARY_TOKEN_NAME,
        SESSION_SECONDARY_TOKEN_NAME,
        ENCRYPTING_SECONDARY_TOKEN_NAME);
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
    tokens.initializeTokenBinding(
        ExtensionBindingData.builder()
            .withStoreContext(fs.createStoreContext())
            .withDelegationOperations(fs.createDelegationOperations())
            .build());
    tokens.init(conf);
    return tokens;
  }

  public S3ADelegationTokens getFSDelegationTokenSupport(final S3AFileSystem fs) {
    return fs.getDelegationTokens().get();
  }

  /**
   * Convert a vargs list to an array.
   * @param args vararg list of arguments
   * @return the generated array.
   */
  String[] args(String... args) {
    return args;
  }

  /**
   * Fetch tokens from the HDFS dtutils command.
   * @param user ugi user.
   * @param tokenKind kind of primary token
   * @param tokenfile file to save to.
   * @return the credentials.
   */
  protected Credentials fetchTokensThroughDtUtils(
      final UserGroupInformation user,
      final Text tokenKind,
      final File tokenfile) throws Exception {

    ExitUtil.disableSystemExit();
    S3AFileSystem fs = getFileSystem();
    Configuration conf = fs.getConf();

    URI fsUri = fs.getUri();
    String fsurl = fsUri.toString();

    // this will create (& leak) a new FS instance as caching is disabled.
    // but as teardown destroys all filesystems for this user, it
    // gets cleaned up at the end of the test
    String tokenFilePath = tokenfile.getAbsolutePath();


    // create the tokens as
    doAs(user,
        () -> DelegationTokenFetcher.main(conf,
            args("--webservice", fsurl, tokenFilePath)));
    assertTrue("token file was not created: " + tokenfile,
        tokenfile.exists());

    // print to stdout
    String s = DelegationTokenFetcher.printTokensToString(conf,
        new Path(tokenfile.toURI()),
        false);
    LOG.info("Tokens: {}", s);
    DelegationTokenFetcher.main(conf,
        args("--print", tokenFilePath));
    DelegationTokenFetcher.main(conf,
        args("--print", "--verbose", tokenFilePath));

    // read in and retrieve token
    Credentials creds = Credentials.readTokenStorageFile(tokenfile, conf);
    AbstractS3ATokenIdentifier identifier = requireNonNull(
        lookupToken(
            creds,
            fsUri,
            tokenKind), "Token lookup");
    assertEquals("encryption secrets",
        fs.getEncryptionSecrets(),
        identifier.getEncryptionSecrets());
    assertEquals("Username of decoded token",
        user.getUserName(), identifier.getUser().getUserName());

    // renew
    DelegationTokenFetcher.main(conf, args("--renew", tokenFilePath));

    // cancel
    DelegationTokenFetcher.main(conf, args("--cancel", tokenFilePath));
    return creds;
  }
}
