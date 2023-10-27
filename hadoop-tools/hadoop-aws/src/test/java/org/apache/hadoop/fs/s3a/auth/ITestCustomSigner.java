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

package org.apache.hadoop.fs.s3a.auth;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.auth.ITestCustomSigner.CustomSignerInitializer.StoreValue;
import org.apache.hadoop.fs.s3a.auth.delegation.DelegationTokenProvider;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.s3a.Constants.CUSTOM_SIGNERS;
import static org.apache.hadoop.fs.s3a.Constants.SIGNING_ALGORITHM_S3;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;

/**
 * Tests for custom Signers and SignerInitializers.
 */
public class ITestCustomSigner extends AbstractS3ATestBase {

  private static final Logger LOG = LoggerFactory
      .getLogger(ITestCustomSigner.class);

  private static final String TEST_ID_KEY = "TEST_ID_KEY";
  private static final String TEST_REGION_KEY = "TEST_REGION_KEY";

  private String regionName;

  private String endpoint;

  @Override
  public void setup() throws Exception {
    super.setup();
    final S3AFileSystem fs = getFileSystem();
    final Configuration conf = fs.getConf();
    endpoint = conf.getTrimmed(Constants.ENDPOINT, Constants.CENTRAL_ENDPOINT);
    LOG.info("Test endpoint is {}", endpoint);
    regionName = conf.getTrimmed(Constants.AWS_REGION, "");
    if (regionName.isEmpty()) {
      regionName = determineRegion(fs.getBucket());
    }
    LOG.info("Determined region name to be [{}] for bucket [{}]", regionName,
        fs.getBucket());
  }

  @Test
  public void testCustomSignerAndInitializer()
      throws IOException, InterruptedException {

    final Path basePath = path(getMethodName());
    UserGroupInformation ugi1 = UserGroupInformation.createRemoteUser("user1");
    FileSystem fs1 = runMkDirAndVerify(ugi1,
        new Path(basePath, "customsignerpath1"), "id1");

    UserGroupInformation ugi2 = UserGroupInformation.createRemoteUser("user2");
    FileSystem fs2 = runMkDirAndVerify(ugi2,
        new Path(basePath, "customsignerpath2"), "id2");

    Assertions.assertThat(CustomSignerInitializer.knownStores.size())
        .as("Num registered stores mismatch").isEqualTo(2);
    fs1.close();
    Assertions.assertThat(CustomSignerInitializer.knownStores.size())
        .as("Num registered stores mismatch").isEqualTo(1);
    fs2.close();
    Assertions.assertThat(CustomSignerInitializer.knownStores.size())
        .as("Num registered stores mismatch").isEqualTo(0);
  }

  private FileSystem runMkDirAndVerify(UserGroupInformation ugi,
      Path finalPath, String identifier)
      throws IOException, InterruptedException {
    Configuration conf = createTestConfig(identifier);
    return ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> {
      int instantiationCount = CustomSigner.getInstantiationCount();
      int invocationCount = CustomSigner.getInvocationCount();
      FileSystem fs = finalPath.getFileSystem(conf);
      fs.mkdirs(finalPath);
      Assertions.assertThat(CustomSigner.getInstantiationCount())
          .as("CustomSigner Instantiation count lower than expected")
          .isGreaterThan(instantiationCount);
      Assertions.assertThat(CustomSigner.getInvocationCount())
          .as("CustomSigner Invocation count lower than expected")
          .isGreaterThan(invocationCount);

      Assertions.assertThat(CustomSigner.lastStoreValue)
          .as("Store value should not be null in %s", CustomSigner.description())
          .isNotNull();
      Assertions.assertThat(CustomSigner.lastStoreValue.conf)
          .as("Configuration should not be null  in %s", CustomSigner.description())
          .isNotNull();
      Assertions.assertThat(CustomSigner.lastStoreValue.conf.get(TEST_ID_KEY))
          .as("Configuration TEST_KEY mismatch in %s", CustomSigner.description())
          .isEqualTo(identifier);

      return fs;
    });
  }

  /**
   * Create a test conf with the custom signer; fixes up
   * endpoint to be that of the test FS.
   * @param identifier test key.
   * @return a configuration for a filesystem.
   */
  private Configuration createTestConfig(String identifier) {
    Configuration conf = createConfiguration();

    conf.set(CUSTOM_SIGNERS,
        "CustomS3Signer:" + CustomSigner.class.getName() + ":"
            + CustomSignerInitializer.class.getName());
    conf.set(SIGNING_ALGORITHM_S3, "CustomS3Signer");

    conf.set(TEST_ID_KEY, identifier);
    conf.set(TEST_REGION_KEY, regionName);

    // make absolutely sure there is no caching.
    disableFilesystemCaching(conf);

    return conf;
  }

  private String determineRegion(String bucketName) throws IOException {
    return getS3AInternals().getBucketLocation(bucketName);
  }

  @Private
  public static final class CustomSigner implements Signer {


    private static final AtomicInteger INSTANTIATION_COUNT =
        new AtomicInteger(0);
    private static final AtomicInteger INVOCATION_COUNT =
        new AtomicInteger(0);

    private static StoreValue lastStoreValue;

    public CustomSigner() {
      int c = INSTANTIATION_COUNT.incrementAndGet();
      LOG.info("Creating Signer #{}", c);
    }

    /**
     * Method to sign the incoming request with credentials.
     *
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
      LOG.info("Signing request #{}", c);

      String host = request.host();
      String bucketName = parseBucketFromHost(host);
      try {
        lastStoreValue = CustomSignerInitializer
            .getStoreValue(bucketName, UserGroupInformation.getCurrentUser());
        LOG.info("Store value for bucket {} is {}", bucketName, lastStoreValue);
      } catch (IOException e) {
        throw new RuntimeException("Failed to get current Ugi " + e, e);
      }
      if (bucketName.equals("kms")) {
        Aws4Signer realKMSSigner = Aws4Signer.create();
        return realKMSSigner.sign(request, executionAttributes);
      } else {
        AwsS3V4Signer realSigner = AwsS3V4Signer.create();
        return realSigner.sign(request, executionAttributes);
      }
    }

    private String parseBucketFromHost(String host) {
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
          + ", lastStoreValue=" + lastStoreValue
          + "}";
    }
  }

  @Private
  public static final class CustomSignerInitializer
      implements AwsSignerInitializer {

    private static final Map<StoreKey, StoreValue> knownStores = new HashMap<>();

    @Override
    public void registerStore(String bucketName, Configuration storeConf,
        DelegationTokenProvider dtProvider, UserGroupInformation storeUgi) {
      StoreKey storeKey = new StoreKey(bucketName, storeUgi);
      StoreValue storeValue = new StoreValue(storeConf, dtProvider);
      LOG.info("Registering store {} with value {}", storeKey, storeValue);
      knownStores.put(storeKey, storeValue);
    }

    @Override
    public void unregisterStore(String bucketName, Configuration storeConf,
        DelegationTokenProvider dtProvider, UserGroupInformation storeUgi) {
      StoreKey storeKey = new StoreKey(bucketName, storeUgi);
      LOG.info("Unregistering store {}", storeKey);
      knownStores.remove(storeKey);
    }

    public static StoreValue getStoreValue(String bucketName,
        UserGroupInformation ugi) {
      StoreKey storeKey = new StoreKey(bucketName, ugi);
      return knownStores.get(storeKey);
    }

    private static class StoreKey {
      private final String bucketName;
      private final UserGroupInformation ugi;

      public StoreKey(String bucketName, UserGroupInformation ugi) {
        this.bucketName = bucketName;
        this.ugi = ugi;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        StoreKey storeKey = (StoreKey) o;
        return Objects.equals(bucketName, storeKey.bucketName) && Objects
            .equals(ugi, storeKey.ugi);
      }

      @Override
      public int hashCode() {
        return Objects.hash(bucketName, ugi);
      }

      @Override
      public String toString() {
        return "StoreKey{" +
            "bucketName='" + bucketName + '\'' +
            ", ugi=" + ugi +
            '}';
      }
    }

    static class StoreValue {
      private final Configuration conf;
      private final DelegationTokenProvider dtProvider;

      public StoreValue(Configuration conf,
          DelegationTokenProvider dtProvider) {
        this.conf = conf;
        this.dtProvider = dtProvider;
      }

      @Override
      public String toString() {
        return "StoreValue{" +
            "conf=" + conf +
            ", dtProvider=" + dtProvider +
            '}';
      }
    }
  }
}
