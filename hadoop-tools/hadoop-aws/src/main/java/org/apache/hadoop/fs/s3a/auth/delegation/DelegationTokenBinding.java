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

import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;

/**
 * Interface for delegation token providers to implement.
 */
public interface DelegationTokenBinding extends DTService {

  /**
   * Get the kind of the tokens managed here.
   * @return the token kind.
   */
  Text getKind();

  /**
   * Return the name of the owner to be used in tokens.
   * This may be that of the UGI owner, or it could be related to
   * the AWS login.
   * @return a text name of the owner.
   */
  Text getOwnerText();

  /**
   * Given the filesystem name, build a canonical name.
   * <p></p>
   * This is only ever called for secondary bindings.
   * <p></p>
   * This MUST NOT return the fsURI unmodified; that is exclusively
   * for the 1ary token.
   * Implementations MAY return a URI which is identical across filesystem
   * instances, e.g the endpoint of the authentication service. This will
   * allow the same token to be used to authenticate arbitrary S3A URLs.
   * If null is returned, the S3ADelegation token class will construct one
   * via {@link S3ADelegationTokens#getTokenServiceForKind}.
   * @param fsURI filesystem URI
   * @return the string to be used to build the canonical name of this token
   * service, or null/empty string.
   */
  default Text buildCanonicalNameForSecondaryBinding(String fsURI) {
    return null;
  }

  /**
   * Predicate: will this binding issue a DT?
   * That is: should the filesystem declare that it is issuing
   * delegation tokens? If true
   * @return a declaration of what will happen when asked for a token.
   */
  S3ADelegationTokens.TokenIssuingPolicy getTokenIssuingPolicy();

  /**
   * Create a delegation token for the user.
   * This will only be called if a new DT is needed, that is: the
   * filesystem has been deployed unbonded.
   * @param policy minimum policy to use, if known.
   * @param encryptionSecrets encryption secrets for the token.
   * @param renewer the principal permitted to renew the token. May be null
   * @return the token or null if the back end does not want to issue one.
   * @throws IOException if one cannot be created
   */
  Token<AbstractS3ATokenIdentifier> createDelegationToken(
      Optional<RoleModel.Policy> policy,
      EncryptionSecrets encryptionSecrets,
      @Nullable Text renewer)
      throws IOException;

  /**
   * Create a token identifier with all the information needed
   * to be included in a delegation token.
   * This is where session credentials need to be extracted, etc.
   * This will only be called if a new DT is needed, that is: the
   * filesystem has been deployed unbonded.
   *
   * If {@link #createDelegationToken(Optional, EncryptionSecrets, Text)}
   * is overridden, this method can be replaced with a stub.
   *
   * @param policy minimum policy to use, if known.
   * @param encryptionSecrets encryption secrets for the token.
   * @param renewer the principal permitted to renew the token.
   * @return the token data to include in the token identifier.
   * @throws IOException failure creating the token data.
   */
  AbstractS3ATokenIdentifier createTokenIdentifier(
      Optional<RoleModel.Policy> policy,
      EncryptionSecrets encryptionSecrets,
      @Nullable Text renewer) throws IOException;

  /**
   * Perform any actions when deploying unbonded, and return a list
   * of credential providers.
   * @return non-empty list of AWS credential providers to use for
   * authenticating this client with AWS services.
   * @throws IOException any failure.
   */
  AWSCredentialProviderList deployUnbonded()
      throws IOException;

  /**
   * Bind to the token identifier, returning the credential providers to use
   * for the owner to talk to S3, DDB and related AWS Services.
   * @param retrievedIdentifier the unmarshalled data
   * @return non-empty list of AWS credential providers to use for
   * authenticating this client with AWS services.
   * @throws IOException any failure.
   */
  AWSCredentialProviderList bindToTokenIdentifier(
      AbstractS3ATokenIdentifier retrievedIdentifier)
      throws IOException;

  /**
   * Create a new subclass of {@link AbstractS3ATokenIdentifier}.
   * This is used in the secret manager.
   * @return an empty identifier.
   */
  AbstractS3ATokenIdentifier createEmptyIdentifier();

  /**
   * Return a description.
   * This is logged during after service start and binding:
   * it should be as informative as possible.
   * @return a description to log.
   */
  String getDescription();

  /**
   * Return a string for use in building up the User-Agent field, so
   * get into the S3 access logs. Useful for diagnostics.
   * @return a string for the S3 logs or "" for "nothing to add"
   */
  String getUserAgentField();

}
