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

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentialBinding;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentialProvider;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;

import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.FULL_TOKEN_KIND;

/**
 * Full credentials: they are simply passed as-is, rather than
 * converted to a session.
 * These aren't as secure; this class exists to (a) support deployments
 * where there is not STS service and (b) validate the design of
 * S3A DT support to support different managers.
 */
public class FullCredentialsTokenBinding extends
    AbstractDelegationTokenBinding {

  /**
   * Wire name of this binding includes a version marker: {@value}.
   */
  private static final String NAME = "FullCredentials/001";

  public static final String FULL_TOKEN = "Full Delegation Token";

  /**
   * Long-lived AWS credentials.
   */
  private MarshalledCredentials awsCredentials;

  /**
   * Origin of credentials.
   */
  private String credentialOrigin;

  /**
   * Constructor, uses name of {@link #name} and token kind of
   * {@link DelegationConstants#FULL_TOKEN_KIND}.
   *
   */
  public FullCredentialsTokenBinding() {
    super(NAME, FULL_TOKEN_KIND);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    loadAWSCredentials();
  }

  /**
   * Load the AWS credentials.
   * @throws IOException failure
   */
  private void loadAWSCredentials() throws IOException {
    credentialOrigin = AbstractS3ATokenIdentifier.createDefaultOriginMessage();
    Configuration conf = getConfig();
    URI uri = getCanonicalUri();
    // look for access keys to FS
    S3xLoginHelper.Login secrets = S3AUtils.getAWSAccessKeys(uri, conf);
    if (secrets.hasLogin()) {
      awsCredentials = new MarshalledCredentials(
          secrets.getUser(), secrets.getPassword(), "");
      credentialOrigin += "; source = Hadoop configuration data";
    } else {
      // if there are none, look for the environment variables.
      awsCredentials = MarshalledCredentialBinding.fromEnvironment(
          System.getenv());
      if (awsCredentials.isValid(
          MarshalledCredentials.CredentialTypeRequired.AnyNonEmpty)) {
        // valid tokens, so mark as origin
        credentialOrigin += "; source = Environment variables";
      } else {
        credentialOrigin = "no credentials in configuration or"
            + " environment variables";
      }
    }
    awsCredentials.validate(credentialOrigin +": ",
        MarshalledCredentials.CredentialTypeRequired.AnyNonEmpty);
  }

  /**
   * Serve up the credentials retrieved from configuration/environment in
   * {@link #loadAWSCredentials()}.
   * @return a credential provider for the unbonded instance.
   * @throws IOException failure to load
   */
  @Override
  public AWSCredentialProviderList deployUnbonded() throws IOException {
    requireServiceStarted();
    return new AWSCredentialProviderList(
        "Full Credentials Token Binding",
        new MarshalledCredentialProvider(
            FULL_TOKEN,
            getFileSystem().getUri(),
            getConfig(),
            awsCredentials,
            MarshalledCredentials.CredentialTypeRequired.AnyNonEmpty));
  }

  /**
   * Create a new delegation token.
   *
   * It's slightly inefficient to create a new one every time, but
   * it avoids concurrency problems with managing any singleton.
   * @param policy minimum policy to use, if known.
   * @param encryptionSecrets encryption secrets.
   * @return a DT identifier
   * @throws IOException failure
   */
  @Override
  public AbstractS3ATokenIdentifier createTokenIdentifier(
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecrets) throws IOException {
    requireServiceStarted();

    return new FullCredentialsTokenIdentifier(getCanonicalUri(),
        getOwnerText(),
        awsCredentials,
        encryptionSecrets,
        credentialOrigin);
  }

  @Override
  public AWSCredentialProviderList bindToTokenIdentifier(
      final AbstractS3ATokenIdentifier retrievedIdentifier)
      throws IOException {
    FullCredentialsTokenIdentifier tokenIdentifier =
        convertTokenIdentifier(retrievedIdentifier,
            FullCredentialsTokenIdentifier.class);
    return new AWSCredentialProviderList(
        "", new MarshalledCredentialProvider(
            FULL_TOKEN,
            getFileSystem().getUri(),
            getConfig(),
            tokenIdentifier.getMarshalledCredentials(),
            MarshalledCredentials.CredentialTypeRequired.AnyNonEmpty));
  }

  @Override
  public AbstractS3ATokenIdentifier createEmptyIdentifier() {
    return new FullCredentialsTokenIdentifier();
  }

}
