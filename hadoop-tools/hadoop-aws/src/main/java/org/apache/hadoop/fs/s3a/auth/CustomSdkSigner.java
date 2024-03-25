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

package org.apache.hadoop.fs.s3a.auth;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.auth.signer.internal.AbstractAwsS3V4Signer;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.auth.delegation.DelegationTokenProvider;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class is for testing the SDK's signing: it
 * can be declared as the signer class in the configuration
 * and then the full test suite run with it.
 * Derived from the inner class of {@code ITestCustomSigner}.
 * <pre>
 * fs.s3a.custom.signers=CustomSdkSigner:org.apache.hadoop.fs.s3a.auth.CustomSdkSigner
 *
 * fs.s3a.s3.signing-algorithm=CustomSdkSigner
 * </pre>
 */
public class CustomSdkSigner  extends AbstractAwsS3V4Signer implements Signer {

  private static final Logger LOG = LoggerFactory
      .getLogger(CustomSdkSigner.class);

  private static final AtomicInteger INSTANTIATION_COUNT =
      new AtomicInteger(0);
  private static final AtomicInteger INVOCATION_COUNT =
      new AtomicInteger(0);

  /**
   * Signer for all S3 requests.
   */
  private final AwsS3V4Signer s3Signer = AwsS3V4Signer.create();

  /**
   * Signer for other services.
   */
  private final Aws4Signer aws4Signer = Aws4Signer.create();


  public CustomSdkSigner() {
    int c = INSTANTIATION_COUNT.incrementAndGet();
    LOG.info("Creating Signer #{}", c);
  }


  /**
   * Method to sign the incoming request with credentials.
   * <p>
   * NOTE: In case of Client-side encryption, we do a "Generate Key" POST
   * request to AWSKMS service rather than S3, this was causing the test to
   * break. When this request happens, we have the endpoint in form of
   * "kms.[REGION].amazonaws.com", and bucket-name becomes "kms". We can't
   * use AWSS3V4Signer for AWSKMS service as it contains a header
   * "x-amz-content-sha256:UNSIGNED-PAYLOAD", which returns a 400 bad
   * request because the signature calculated by the service doesn't match
   * what we sent.
   * @param request the request to sign.
   * @param executionAttributes request executionAttributes which contain the credentials.
   */
  @Override
  public SdkHttpFullRequest sign(SdkHttpFullRequest request,
      ExecutionAttributes executionAttributes) {
    int c = INVOCATION_COUNT.incrementAndGet();

    String host = request.host();
    LOG.debug("Signing request #{} against {}: class {}",
        c, host, request.getClass());
    String bucketName = parseBucketFromHost(host);
    if (bucketName.equals("kms")) {
      return aws4Signer.sign(request, executionAttributes);
    } else {
      return s3Signer.sign(request, executionAttributes);
    }
  }

  /**
   * Parse the bucket name from the host.
   * @param host hostname
   * @return the parsed bucket name; if "kms" is KMS signing.
   */
  static String parseBucketFromHost(String host) {
    String[] hostBits = host.split("\\.");
    String bucketName = hostBits[0];
    String service = hostBits[1];

    if (bucketName.equals("kms")) {
      return bucketName;
    }

    if (service.contains("s3-accesspoint") || service.contains("s3-outposts")
        || service.contains("s3-object-lambda")) {
      // If AccessPoint then bucketName is of format `accessPoint-accountId`;
      String[] accessPointBits = bucketName.split("-");
      String accountId = accessPointBits[accessPointBits.length - 1];
      // Extract the access point name from bucket name. eg: if bucket name is
      // test-custom-signer-<accountId>, get the access point name test-custom-signer by removing
      // -<accountId> from the bucket name.
      String accessPointName =
          bucketName.substring(0, bucketName.length() - (accountId.length() + 1));
      Arn arn = Arn.builder()
          .accountId(accountId)
          .partition("aws")
          .region(hostBits[2])
          .resource("accesspoint" + "/" + accessPointName)
          .service("s3").build();

      bucketName = arn.toString();
    }

    return bucketName;
  }

  public static int getInstantiationCount() {
    return INSTANTIATION_COUNT.get();
  }

  public static int getInvocationCount() {
    return INVOCATION_COUNT.get();
  }

  public static String description() {
    return "CustomSigner{"
        + "invocations=" + INVOCATION_COUNT.get()
        + ", instantiations=" + INSTANTIATION_COUNT.get()
        + "}";
  }

  public static class Initializer implements AwsSignerInitializer {

    @Override
    public void registerStore(
        final String bucketName,
        final Configuration storeConf,
        final DelegationTokenProvider dtProvider,
        final UserGroupInformation storeUgi) {

      LOG.debug("Registering store for bucket {}", bucketName);
    }

    @Override
    public void unregisterStore(final String bucketName,
        final Configuration storeConf,
        final DelegationTokenProvider dtProvider,
        final UserGroupInformation storeUgi) {
      LOG.debug("Unregistering store for bucket {}", bucketName);
    }
  }

}
