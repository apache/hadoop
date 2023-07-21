/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.adapter;

import java.io.IOException;
import java.net.URI;
import javax.annotation.Nullable;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AnonymousAWSCredentials;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AUtils;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;

/**
 * Adapts a V1 {@link AWSCredentialsProvider} to the V2 {@link AwsCredentialsProvider} interface.
 */
public final class V1ToV2AwsCredentialProviderAdapter implements AwsCredentialsProvider {

  private final AWSCredentialsProvider v1CredentialsProvider;


  private V1ToV2AwsCredentialProviderAdapter(AWSCredentialsProvider v1CredentialsProvider) {
    this.v1CredentialsProvider = requireNonNull(v1CredentialsProvider);
  }

  @Override
  public AwsCredentials resolveCredentials() {
    AWSCredentials toAdapt = v1CredentialsProvider.getCredentials();
    if (toAdapt instanceof AWSSessionCredentials) {
      return AwsSessionCredentials.create(toAdapt.getAWSAccessKeyId(),
          toAdapt.getAWSSecretKey(),
          ((AWSSessionCredentials) toAdapt).getSessionToken());
    } else if (toAdapt instanceof AnonymousAWSCredentials) {
      return AnonymousCredentialsProvider.create().resolveCredentials();
    } else {
      return AwsBasicCredentials.create(toAdapt.getAWSAccessKeyId(), toAdapt.getAWSSecretKey());
    }
  }

  @Override
  public String toString() {
    return "V1ToV2AwsCredentialProviderAdapter{" +
        "v1CredentialsProvider=" + v1CredentialsProvider +
        '}';
  }

  /**
   * @param v1CredentialsProvider V1 credential provider to adapt.
   * @return A new instance of the credentials provider adapter.
   */
  static AwsCredentialsProvider create(AWSCredentialsProvider v1CredentialsProvider) {
    return new V1ToV2AwsCredentialProviderAdapter(v1CredentialsProvider);
  }

  /**
   * Create an AWS credential provider from its class by using reflection.  The
   * class must implement one of the following means of construction, which are
   * attempted in order:
   *
   * <ol>
   * <li>a public constructor accepting java.net.URI and
   *     org.apache.hadoop.conf.Configuration</li>
   * <li>a public constructor accepting
   *    org.apache.hadoop.conf.Configuration</li>
   * <li>a public static method named getInstance that accepts no
   *    arguments and returns an instance of
   *    com.amazonaws.auth.AWSCredentialsProvider, or</li>
   * <li>a public default constructor.</li>
   * </ol>
   * @param conf configuration
   * @param className classname
   * @param uri URI of the FS
   * @return the instantiated class
   * @throws IOException on any instantiation failure.
   */
  static AwsCredentialsProvider create(
      Configuration conf,
      String className,
      @Nullable URI uri) throws IOException {


    final AWSCredentialsProvider instance =
        S3AUtils.getInstanceFromReflection(className, conf, uri, AWSCredentialsProvider.class,
            "getInstance", AWS_CREDENTIALS_PROVIDER);
    return create(instance);

  }

}
