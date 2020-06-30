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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.DelegationTokenIssuer;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DURATION_LOG_AT_INFO;

/**
 * This is a token issuer which can be created to support
 * issuing secondary tokens from an S3A FS instance.
 * <p></p>
 * Usage:
 * instantiate with the policy and secrets to be used
 * when generating the token.
 * Return in the results of a call to
 * {@link DelegationTokenIssuer#getAdditionalTokenIssuers()}.
 *
 */
public class S3ATokenIssuer implements DelegationTokenIssuer {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3ATokenIssuer.class);

  private final Optional<RoleModel.Policy> policy;

  private final EncryptionSecrets encryptionSecret;

  private final Text serviceName;

  private final TokenIssueCallbacks callbacks;

  private final SecondaryDelegationToken tokenBinding;

  /**
   * Instantiate.
   * @param tokenBinding DT binding for the source of tokens.
   * @param policy role policy.
   * @param encryptionSecret encryption secrets to put in the token.
   * @param serviceName canonical service name.
   * @param callbacks issuing callbacks
   */
  public S3ATokenIssuer(
      final SecondaryDelegationToken tokenBinding,
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecret,
      final Text serviceName,
      final TokenIssueCallbacks callbacks) {
    this.tokenBinding = tokenBinding;
    this.policy = policy;
    this.encryptionSecret = encryptionSecret;
    this.serviceName = serviceName;
    this.callbacks = callbacks;
  }

  @Override
  public String getCanonicalServiceName() {
    return serviceName.toString();
  }

  /**
   * Issue a a token via a call to
   * {@link DelegationTokenBinding#createDelegationToken(Optional, EncryptionSecrets, Text)}.
   * @param renewer renewer, may be null
   * @return the token
   * @throws IOException failure[
   */
  @Override
  public Token<?> getDelegationToken(@Nullable final String renewer)
      throws IOException {

    try (DurationInfo ignored = new DurationInfo(LOG, DURATION_LOG_AT_INFO,
            "Creating New Delegation Token", tokenBinding.getKind())) {
      Text t = renewer != null ? new Text(renewer) : null;
      Token<AbstractS3ATokenIdentifier> token =
          tokenBinding.getBoundOrNewDT(callbacks,
              policy,
              encryptionSecret,
              t);
      token.setService(serviceName);
      return token;
    }
  }
}
