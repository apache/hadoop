/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs.s3a.auth;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.auth.TestSignerManager.SignerInitializerForTest.StoreValue;
import org.apache.hadoop.fs.s3a.auth.delegation.DelegationTokenProvider;
import org.apache.hadoop.fs.s3a.impl.InstantiationIOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.CUSTOM_SIGNERS;
import static org.apache.hadoop.fs.s3a.auth.SignerFactory.S3_V2_SIGNER;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests for the SignerManager.
 */
public class TestSignerManager extends AbstractHadoopTestBase {

  private static final Text TEST_TOKEN_KIND = new Text("TestTokenKind");
  private static final Text TEST_TOKEN_SERVICE = new Text("TestTokenService");
  private static final String TEST_KEY_IDENTIFIER = "TEST_KEY_IDENTIFIER";
  private static final String BUCKET1 = "bucket1";
  private static final String BUCKET2 = "bucket2";
  private static final String TESTUSER1 = "testuser1";
  private static final String TESTUSER2 = "testuser2";

  @Before
  public void beforeTest() {
    SignerForTest1.reset();
    SignerForTest2.reset();
    SignerInitializerForTest.reset();
    SignerForInitializerTest.reset();
    SignerInitializer2ForTest.reset();
  }

  @Test
  public void testPredefinedSignerInitialization() throws IOException {
    // Try initializing a pre-defined Signer type.
    // Should run through without an exception.
    Configuration config = new Configuration();
    // Pre-defined signer types as of AWS-SDK 1.11.563
    // AWS4SignerType, QueryStringSignerType, AWSS3V4SignerType
    config.set(CUSTOM_SIGNERS, "AWS4SignerType");
    SignerManager signerManager = new SignerManager("dontcare", null, config,
        UserGroupInformation.getCurrentUser());
    signerManager.initCustomSigners();
  }

  @Test
  public void testCustomSignerFailureIfNotRegistered() throws Exception {
    Configuration config = new Configuration();
    config.set(CUSTOM_SIGNERS, "testsignerUnregistered");
    SignerManager signerManager = new SignerManager("dontcare", null, config,
        UserGroupInformation.getCurrentUser());
    // Make sure the config is respected.
    signerManager.initCustomSigners();
    // Simulate a call from the AWS SDK to create the signer.
    intercept(InstantiationIOException.class,
        () -> SignerFactory.createSigner("testsignerUnregistered", null));
  }

  @Test
  public void testCustomSignerInitialization() throws IOException {
    Configuration config = new Configuration();
    config.set(CUSTOM_SIGNERS, "testsigner1:" + SignerForTest1.class.getName());
    SignerManager signerManager = new SignerManager("dontcare", null, config,
        UserGroupInformation.getCurrentUser());
    signerManager.initCustomSigners();
    Signer s1 = SignerFactory.createSigner("testsigner1", null);
    s1.sign(null, null);
    Assertions.assertThat(SignerForTest1.initialized)
        .as(SignerForTest1.class.getName() + " not initialized")
        .isEqualTo(true);
  }

  @Test
  public void testMultipleCustomSignerInitialization() throws IOException {
    Configuration config = new Configuration();
    config.set(CUSTOM_SIGNERS,
        "testsigner1:" + SignerForTest1.class.getName() + "," + "testsigner2:"
            + SignerForTest2.class.getName());
    SignerManager signerManager = new SignerManager("dontcare", null, config,
        UserGroupInformation.getCurrentUser());
    signerManager.initCustomSigners();
    Signer s1 = SignerFactory.createSigner("testsigner1", null);
    s1.sign(null, null);
    Assertions.assertThat(SignerForTest1.initialized)
        .as(SignerForTest1.class.getName() + " not initialized")
        .isEqualTo(true);

    Signer s2 = SignerFactory.createSigner("testsigner2", null);
    s2.sign(null, null);
    Assertions.assertThat(SignerForTest2.initialized)
        .as(SignerForTest2.class.getName() + " not initialized")
        .isEqualTo(true);
  }

  @Test
  public void testSimpleSignerInitializer() throws IOException {
    Configuration config = new Configuration();
    config.set(CUSTOM_SIGNERS,
        "testsigner1:" + SignerForTest1.class.getName() + ":"
            + SignerInitializerForTest.class.getName());

    Token<? extends TokenIdentifier> token = createTokenForTest("identifier");
    DelegationTokenProvider dtProvider = new DelegationTokenProviderForTest(
        token);

    UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser("testuser");

    SignerManager signerManager = new SignerManager("bucket1", dtProvider,
        config, ugi);
    signerManager.initCustomSigners();
    Assertions.assertThat(SignerInitializerForTest.instanceCount)
        .as(SignerInitializerForTest.class.getName()
            + " creation count mismatch").isEqualTo(1);
    Assertions.assertThat(SignerInitializerForTest.registerCount)
        .as(SignerInitializerForTest.class.getName()
            + " registration count mismatch").isEqualTo(1);
    Assertions.assertThat(SignerInitializerForTest.unregisterCount)
        .as(SignerInitializerForTest.class.getName()
            + " registration count mismatch").isEqualTo(0);

    signerManager.close();
    Assertions.assertThat(SignerInitializerForTest.unregisterCount)
        .as(SignerInitializerForTest.class.getName()
            + " registration count mismatch").isEqualTo(1);
  }

  @Test
  public void testMultipleSignerInitializers() throws IOException {
    Configuration config = new Configuration();
    config.set(CUSTOM_SIGNERS,
        "testsigner1:" + SignerForTest1.class.getName() + ":"
            + SignerInitializerForTest.class.getName() + "," // 2nd signer
            + "testsigner2:" + SignerForTest2.class.getName() + ","
            // 3rd signer
            + "testsigner3:" + SignerForTest2.class.getName() + ":"
            + SignerInitializer2ForTest.class.getName());

    Token<? extends TokenIdentifier> token = createTokenForTest("identifier");
    DelegationTokenProvider dtProvider = new DelegationTokenProviderForTest(
        token);

    UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser("testuser");

    SignerManager signerManager = new SignerManager("bucket1", dtProvider,
        config, ugi);
    signerManager.initCustomSigners();

    Assertions.assertThat(SignerInitializerForTest.instanceCount)
        .as(SignerInitializerForTest.class.getName()
            + " creation count mismatch").isEqualTo(1);
    Assertions.assertThat(SignerInitializerForTest.registerCount)
        .as(SignerInitializerForTest.class.getName()
            + " registration count mismatch").isEqualTo(1);
    Assertions.assertThat(SignerInitializerForTest.unregisterCount)
        .as(SignerInitializerForTest.class.getName()
            + " registration count mismatch").isEqualTo(0);

    Assertions.assertThat(SignerInitializer2ForTest.instanceCount)
        .as(SignerInitializer2ForTest.class.getName()
            + " creation count mismatch").isEqualTo(1);
    Assertions.assertThat(SignerInitializer2ForTest.registerCount)
        .as(SignerInitializer2ForTest.class.getName()
            + " registration count mismatch").isEqualTo(1);
    Assertions.assertThat(SignerInitializer2ForTest.unregisterCount)
        .as(SignerInitializer2ForTest.class.getName()
            + " registration count mismatch").isEqualTo(0);

    signerManager.close();
    Assertions.assertThat(SignerInitializerForTest.unregisterCount)
        .as(SignerInitializerForTest.class.getName()
            + " registration count mismatch").isEqualTo(1);
    Assertions.assertThat(SignerInitializer2ForTest.unregisterCount)
        .as(SignerInitializer2ForTest.class.getName()
            + " registration count mismatch").isEqualTo(1);
  }

  @Test
  public void testSignerInitializerMultipleInstances()
      throws IOException, InterruptedException {

    String id1 = "id1";
    String id2 = "id2";
    String id3 = "id3";
    UserGroupInformation ugiU1 = UserGroupInformation
        .createRemoteUser(TESTUSER1);
    UserGroupInformation ugiU2 = UserGroupInformation
        .createRemoteUser(TESTUSER2);

    SignerManager signerManagerU1B1 = fakeS3AInstanceCreation(id1,
        SignerForInitializerTest.class, SignerInitializerForTest.class, BUCKET1,
        ugiU1);
    SignerManager signerManagerU2B1 = fakeS3AInstanceCreation(id2,
        SignerForInitializerTest.class, SignerInitializerForTest.class, BUCKET1,
        ugiU2);
    SignerManager signerManagerU2B2 = fakeS3AInstanceCreation(id3,
        SignerForInitializerTest.class, SignerInitializerForTest.class, BUCKET2,
        ugiU2);

    Assertions.assertThat(SignerInitializerForTest.instanceCount)
        .as(SignerInitializerForTest.class.getName()
            + " creation count mismatch").isEqualTo(3);
    Assertions.assertThat(SignerInitializerForTest.registerCount)
        .as(SignerInitializerForTest.class.getName()
            + " registration count mismatch").isEqualTo(3);
    Assertions.assertThat(SignerInitializerForTest.unregisterCount)
        .as(SignerInitializerForTest.class.getName()
            + " registration count mismatch").isEqualTo(0);

    // Simulate U1B1 making a request
    attemptSignAndVerify(id1, BUCKET1, ugiU1, false);

    // Simulate U2B1 making a request
    attemptSignAndVerify(id2, BUCKET1, ugiU2, false);

    // Simulate U2B2 making a request
    attemptSignAndVerify(id3, BUCKET2, ugiU2, false);

    // Simulate U1B2 (not defined - so Signer should get a null)
    attemptSignAndVerify("dontcare", BUCKET2, ugiU1, true);

    closeAndVerifyNull(signerManagerU1B1, BUCKET1, ugiU1, 2);
    closeAndVerifyNull(signerManagerU2B2, BUCKET2, ugiU2, 1);
    closeAndVerifyNull(signerManagerU2B1, BUCKET1, ugiU2, 0);

    Assertions.assertThat(SignerInitializerForTest.unregisterCount)
        .as(SignerInitializerForTest.class.getName()
            + " registration count mismatch").isEqualTo(3);
  }

  private void attemptSignAndVerify(String identifier, String bucket,
      UserGroupInformation ugi, boolean expectNullStoreInfo)
      throws IOException, InterruptedException {
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      Signer signer = new SignerForInitializerTest();
      SdkHttpFullRequest signableRequest = constructSignableRequest(bucket);
      signer.sign(signableRequest, null);
      verifyStoreValueInSigner(expectNullStoreInfo, bucket, identifier);
      return null;
    });
  }

  private void verifyStoreValueInSigner(boolean expectNull, String bucketName,
      String identifier) throws IOException {
    if (expectNull) {
      Assertions.assertThat(SignerForInitializerTest.retrievedStoreValue)
          .as("Retrieved store value expected to be null").isNull();
    } else {
      StoreValue storeValue = SignerForInitializerTest.retrievedStoreValue;
      Assertions.assertThat(storeValue).as("StoreValue should not be null")
          .isNotNull();
      Assertions.assertThat(storeValue.getBucketName())
          .as("Bucket Name mismatch").isEqualTo(bucketName);
      Configuration conf = storeValue.getStoreConf();
      Assertions.assertThat(conf).as("Configuration should not be null")
          .isNotNull();
      Assertions.assertThat(conf.get(TEST_KEY_IDENTIFIER))
          .as("Identifier mistmatch").isEqualTo(identifier);
      Token<? extends TokenIdentifier> token = storeValue.getDtProvider()
          .getFsDelegationToken();
      String tokenId = new String(token.getIdentifier(),
          StandardCharsets.UTF_8);
      Assertions.assertThat(tokenId)
          .as("Mismatch in delegation token identifier").isEqualTo(
          createTokenIdentifierString(identifier, bucketName,
              UserGroupInformation.getCurrentUser().getShortUserName()));
    }
  }

  private void closeAndVerifyNull(Closeable closeable, String bucketName,
      UserGroupInformation ugi, int expectedCount)
      throws IOException, InterruptedException {
    closeable.close();
    attemptSignAndVerify("dontcare", bucketName, ugi, true);
    Assertions.assertThat(SignerInitializerForTest.storeCache.size())
        .as("StoreCache size mismatch").isEqualTo(expectedCount);
  }

  /**
   * SignerForTest1.
   */
  @Private
  public static class SignerForTest1 implements Signer {

    private static boolean initialized = false;

    @Override
    public SdkHttpFullRequest sign(SdkHttpFullRequest sdkHttpFullRequest,
        ExecutionAttributes executionAttributes) {
      initialized = true;
      return sdkHttpFullRequest;
    }

    public static void reset() {
      initialized = false;
    }
  }

  /**
   * SignerForTest2.
   */
  @Private
  public static class SignerForTest2 implements Signer {

    private static boolean initialized = false;

    @Override
    public SdkHttpFullRequest sign(SdkHttpFullRequest sdkHttpFullRequest,
        ExecutionAttributes executionAttributes) {
      initialized = true;
      return sdkHttpFullRequest;
    }

    public static void reset() {
      initialized = false;
    }
  }

  /**
   * SignerInitializerForTest.
   */
  @Private
  public static class SignerInitializerForTest implements AwsSignerInitializer {

    private static int registerCount = 0;
    private static int unregisterCount = 0;
    private static int instanceCount = 0;

    private static final Map<StoreKey, StoreValue> storeCache = new HashMap<>();

    public SignerInitializerForTest() {
      instanceCount++;
    }

    @Override
    public void registerStore(String bucketName, Configuration storeConf,
        DelegationTokenProvider dtProvider, UserGroupInformation storeUgi) {
      registerCount++;
      StoreKey storeKey = new StoreKey(bucketName, storeUgi);
      StoreValue storeValue = new StoreValue(bucketName, storeConf, dtProvider);
      storeCache.put(storeKey, storeValue);
    }

    @Override
    public void unregisterStore(String bucketName, Configuration storeConf,
        DelegationTokenProvider dtProvider, UserGroupInformation storeUgi) {
      unregisterCount++;
      StoreKey storeKey = new StoreKey(bucketName, storeUgi);
      storeCache.remove(storeKey);
    }

    public static void reset() {
      registerCount = 0;
      unregisterCount = 0;
      instanceCount = 0;
      storeCache.clear();
    }

    public static StoreValue getStoreInfo(String bucketName,
        UserGroupInformation storeUgi) {
      StoreKey storeKey = new StoreKey(bucketName, storeUgi);
      return storeCache.get(storeKey);
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
    }

    static class StoreValue {
      private final String bucketName;
      private final Configuration storeConf;
      private final DelegationTokenProvider dtProvider;

      public StoreValue(String bucketName, Configuration storeConf,
          DelegationTokenProvider dtProvider) {
        this.bucketName = bucketName;
        this.storeConf = storeConf;
        this.dtProvider = dtProvider;
      }

      String getBucketName() {
        return bucketName;
      }

      Configuration getStoreConf() {
        return storeConf;
      }

      DelegationTokenProvider getDtProvider() {
        return dtProvider;
      }
    }
  }

  /**
   * To be used in conjunction with {@link SignerInitializerForTest}.
   */
  @Private
  public static class SignerForInitializerTest implements Signer {

    private static StoreValue retrievedStoreValue;

    @Override
    public SdkHttpFullRequest sign(SdkHttpFullRequest sdkHttpFullRequest,
        ExecutionAttributes executionAttributes) {
      String bucket = sdkHttpFullRequest.host().split("//")[1];
      // remove trailing slash
      String bucketName = bucket.substring(0, bucket.length() - 1);
      try {
        retrievedStoreValue = SignerInitializerForTest
            .getStoreInfo(bucketName, UserGroupInformation.getCurrentUser());
        return sdkHttpFullRequest;
      } catch (IOException e) {
        throw new RuntimeException("Failed to get current ugi", e);
      }
    }

    public static void reset() {
      retrievedStoreValue = null;
    }
  }

  /**
   * DelegationTokenProviderForTest.
   */
  @Private
  private static class DelegationTokenProviderForTest
      implements DelegationTokenProvider {

    private final Token<? extends TokenIdentifier> token;

    private DelegationTokenProviderForTest(
        Token<? extends TokenIdentifier> token) {
      this.token = token;
    }

    @Override
    public Token<? extends TokenIdentifier> getFsDelegationToken()
        throws IOException {
      return this.token;
    }
  }

  /**
   * SignerInitializer2ForTest.
   */
  @Private
  public static class SignerInitializer2ForTest
      implements AwsSignerInitializer {

    private static int registerCount = 0;
    private static int unregisterCount = 0;
    private static int instanceCount = 0;

    public SignerInitializer2ForTest() {
      instanceCount++;
    }

    @Override
    public void registerStore(String bucketName, Configuration storeConf,
        DelegationTokenProvider dtProvider, UserGroupInformation storeUgi) {
      registerCount++;
    }

    @Override
    public void unregisterStore(String bucketName, Configuration storeConf,
        DelegationTokenProvider dtProvider, UserGroupInformation storeUgi) {
      unregisterCount++;
    }

    public static void reset() {
      registerCount = 0;
      unregisterCount = 0;
      instanceCount = 0;
    }
  }

  private Token<? extends TokenIdentifier> createTokenForTest(String idString) {
    byte[] identifier = idString.getBytes(StandardCharsets.UTF_8);
    byte[] password = "notapassword".getBytes(StandardCharsets.UTF_8);
    Token<? extends TokenIdentifier> token = new Token<>(identifier, password,
        TEST_TOKEN_KIND, TEST_TOKEN_SERVICE);
    return token;
  }

  private SignerManager fakeS3AInstanceCreation(String identifier,
      Class<? extends Signer> signerClazz,
      Class<? extends AwsSignerInitializer> signerInitializerClazz,
      String bucketName, UserGroupInformation ugi) {
    // Simulate new S3A instance interactions.
    Objects.requireNonNull(signerClazz, "SignerClazz missing");
    Objects.requireNonNull(signerInitializerClazz,
        "SignerInitializerClazzMissing");
    Configuration config = new Configuration();
    config.set(TEST_KEY_IDENTIFIER, identifier);
    config.set(CUSTOM_SIGNERS,
        signerClazz.getCanonicalName() + ":" + signerClazz.getName() + ":"
            + signerInitializerClazz.getName());
    Token<? extends TokenIdentifier> token1 = createTokenForTest(
        createTokenIdentifierString(identifier, bucketName,
            ugi.getShortUserName()));
    DelegationTokenProvider dtProvider1 = new DelegationTokenProviderForTest(
        token1);
    SignerManager signerManager = new SignerManager(bucketName, dtProvider1,
        config, ugi);
    signerManager.initCustomSigners();
    return signerManager;
  }

  private String createTokenIdentifierString(String identifier,
      String bucketName, String user) {
    return identifier + "_" + bucketName + "_" + user;
  }

  private SdkHttpFullRequest constructSignableRequest(String bucketName) {
    String host = "s3://" + bucketName + "/";
    return SdkHttpFullRequest.builder().host(host).protocol("https").method(SdkHttpMethod.GET)
        .build();
  }

  @Test
  public void testV2SignerRejected() throws Throwable {
    intercept(InstantiationIOException.class, "no longer supported",
        () -> SignerFactory.createSigner(S3_V2_SIGNER, "key"));
  }

  @Test
  public void testUnknownSignerRejected() throws Throwable {
    intercept(InstantiationIOException.class, "unknownSigner",
        () -> SignerFactory.createSigner("unknownSigner", "key"));
  }

}
