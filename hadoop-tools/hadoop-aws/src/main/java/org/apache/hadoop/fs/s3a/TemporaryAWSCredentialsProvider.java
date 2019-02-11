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

package org.apache.hadoop.fs.s3a;

import javax.annotation.Nullable;
import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;

import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.auth.AbstractSessionCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentialBinding;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.fs.s3a.auth.NoAuthWithAWSException;
import org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException;

/**
 * Support session credentials for authenticating with AWS.
 *
 * Please note that users may reference this class name from configuration
 * property fs.s3a.aws.credentials.provider.  Therefore, changing the class name
 * would be a backward-incompatible change.
 *
 * This credential provider must not fail in creation because that will
 * break a chain of credential providers.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TemporaryAWSCredentialsProvider extends
    AbstractSessionCredentialsProvider {

  public static final String NAME
      = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";

  public static final String COMPONENT
      = "Session credentials in Hadoop configuration";

  /**
   * Construct from just a configuration.
   * @param conf configuration.
   * @throws IOException on any failure to load the credentials.
   */
  public TemporaryAWSCredentialsProvider(final Configuration conf)
      throws IOException {
    this(null, conf);
  }

  /**
   * Constructor: the URI will be null if the provider is inited unbonded
   * to a filesystem.
   * @param uri binding to a filesystem URI.
   * @param conf configuration.
   * @throws IOException on any failure to load the credentials.
   */
  public TemporaryAWSCredentialsProvider(
      @Nullable final URI uri,
      final Configuration conf)
      throws IOException {
    super(uri, conf);
  }

  /**
   * The credentials here must include a session token, else this operation
   * will raise an exception.
   * @param config the configuration
   * @return temporary credentials.
   * @throws IOException on any failure to load the credentials.
   * @throws NoAuthWithAWSException validation failure
   * @throws NoAwsCredentialsException the credentials are actually empty.
   */
  @Override
  protected AWSCredentials createCredentials(Configuration config)
      throws IOException {
    MarshalledCredentials creds = MarshalledCredentialBinding.fromFileSystem(
        getUri(), config);
    MarshalledCredentials.CredentialTypeRequired sessionOnly
        = MarshalledCredentials.CredentialTypeRequired.SessionOnly;
    // treat only having non-session creds as empty.
    if (!creds.isValid(sessionOnly)) {
      throw new NoAwsCredentialsException(COMPONENT);
    }
    return MarshalledCredentialBinding.toAWSCredentials(creds,
        sessionOnly, COMPONENT);
  }

}
