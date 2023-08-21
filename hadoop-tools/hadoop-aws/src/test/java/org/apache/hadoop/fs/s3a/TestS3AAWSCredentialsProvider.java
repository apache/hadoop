/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.auth.AbstractSessionCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider;
import org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.NoAuthWithAWSException;
import org.apache.hadoop.fs.s3a.impl.InstantiationIOException;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.util.Sets;

import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.DEFAULT_CSVTEST_FILE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.authenticationContains;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.buildClassListString;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getCSVTestPath;
import static org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory.STANDARD_AWS_PROVIDERS;
import static org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory.buildAWSProviderList;
import static org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory.createAWSCredentialProviderList;
import static org.apache.hadoop.fs.s3a.impl.InstantiationIOException.DOES_NOT_IMPLEMENT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link Constants#AWS_CREDENTIALS_PROVIDER} logic.
 */
public class TestS3AAWSCredentialsProvider {

  /**
   * URI of the landsat images.
   */
  private static final URI TESTFILE_URI = new Path(
      DEFAULT_CSVTEST_FILE).toUri();

  private static final Logger LOG = LoggerFactory.getLogger(TestS3AAWSCredentialsProvider.class);

  @Test
  public void testProviderWrongClass() throws Exception {
    expectProviderInstantiationFailure(this.getClass(),
        DOES_NOT_IMPLEMENT + " software.amazon.awssdk.auth.credentials.AwsCredentialsProvider");
  }

  @Test
  public void testProviderAbstractClass() throws Exception {
    expectProviderInstantiationFailure(AbstractProvider.class,
        InstantiationIOException.ABSTRACT_PROVIDER);
  }

  @Test
  public void testProviderNotAClass() throws Exception {
    expectProviderInstantiationFailure("NoSuchClass",
        "ClassNotFoundException");
  }

  @Test
  public void testProviderConstructorError() throws Exception {
    expectProviderInstantiationFailure(
        ConstructorSignatureErrorProvider.class,
        InstantiationIOException.CONSTRUCTOR_EXCEPTION);
  }

  @Test
  public void testProviderFailureError() throws Exception {
    expectProviderInstantiationFailure(
        ConstructorFailureProvider.class,
        InstantiationIOException.INSTANTIATION_EXCEPTION);
  }

  @Test
  public void testInstantiationChain() throws Throwable {
    Configuration conf = new Configuration(false);
    conf.set(AWS_CREDENTIALS_PROVIDER,
        TemporaryAWSCredentialsProvider.NAME
            + ", \t" + SimpleAWSCredentialsProvider.NAME
            + " ,\n " + AnonymousAWSCredentialsProvider.NAME);
    Path testFile = getCSVTestPath(conf);

    AWSCredentialProviderList list = createAWSCredentialProviderList(
        testFile.toUri(), conf);
    List<Class<?>> expectedClasses =
        Arrays.asList(
            TemporaryAWSCredentialsProvider.class,
            SimpleAWSCredentialsProvider.class,
            AnonymousAWSCredentialsProvider.class);
    assertCredentialProviders(expectedClasses, list);
  }

  @Test
  public void testDefaultChain() throws Exception {
    URI uri1 = new URI("s3a://bucket1"), uri2 = new URI("s3a://bucket2");
    Configuration conf = new Configuration(false);
    // use the default credential provider chain
    conf.unset(AWS_CREDENTIALS_PROVIDER);
    AWSCredentialProviderList list1 = createAWSCredentialProviderList(
        uri1, conf);
    AWSCredentialProviderList list2 = createAWSCredentialProviderList(
        uri2, conf);
    List<Class<?>> expectedClasses = STANDARD_AWS_PROVIDERS;
    assertCredentialProviders(expectedClasses, list1);
    assertCredentialProviders(expectedClasses, list2);
  }

  @Test
  public void testDefaultChainNoURI() throws Exception {
    Configuration conf = new Configuration(false);
    // use the default credential provider chain
    conf.unset(AWS_CREDENTIALS_PROVIDER);
    assertCredentialProviders(STANDARD_AWS_PROVIDERS,
        createAWSCredentialProviderList(null, conf));
  }

  @Test
  public void testConfiguredChain() throws Exception {
    URI uri1 = new URI("s3a://bucket1"), uri2 = new URI("s3a://bucket2");
    List<Class<?>> expectedClasses =
        Arrays.asList(
            IAMInstanceCredentialsProvider.class,
            AnonymousAWSCredentialsProvider.class,
            EnvironmentVariableCredentialsProvider.class
        );
    Configuration conf =
        createProviderConfiguration(buildClassListString(expectedClasses));
    AWSCredentialProviderList list1 = createAWSCredentialProviderList(
        uri1, conf);
    AWSCredentialProviderList list2 = createAWSCredentialProviderList(
        uri2, conf);
    assertCredentialProviders(expectedClasses, list1);
    assertCredentialProviders(expectedClasses, list2);
  }

  @Test
  public void testConfiguredChainUsesSharedInstanceProfile() throws Exception {
    URI uri1 = new URI("s3a://bucket1"), uri2 = new URI("s3a://bucket2");
    Configuration conf = new Configuration(false);
    List<Class<?>> expectedClasses =
        Arrays.asList(
            InstanceProfileCredentialsProvider.class);
    conf.set(AWS_CREDENTIALS_PROVIDER, buildClassListString(expectedClasses));
    AWSCredentialProviderList list1 = createAWSCredentialProviderList(
        uri1, conf);
    AWSCredentialProviderList list2 = createAWSCredentialProviderList(
        uri2, conf);
    assertCredentialProviders(expectedClasses, list1);
    assertCredentialProviders(expectedClasses, list2);
  }

  @Test
  public void testFallbackToDefaults() throws Throwable {
    // build up the base provider
    final AWSCredentialProviderList credentials = buildAWSProviderList(
        new URI("s3a://bucket1"),
        createProviderConfiguration("  "),
        ASSUMED_ROLE_CREDENTIALS_PROVIDER,
        Arrays.asList(
            EnvironmentVariableCredentialsProvider.class),
        Sets.newHashSet());
    assertTrue("empty credentials", credentials.size() > 0);
  }

  @Test
  public void testProviderConstructor() throws Throwable {
    final AWSCredentialProviderList list = new AWSCredentialProviderList("name",
        new AnonymousAWSCredentialsProvider(),
        new ErrorProvider(TESTFILE_URI, new Configuration()));
    Assertions.assertThat(list.getProviders())
        .describedAs("provider list in %s", list)
        .hasSize(2);
    final AwsCredentials credentials = list.resolveCredentials();
    Assertions.assertThat(credentials)
        .isInstanceOf(AwsBasicCredentials.class);
    assertCredentialResolution(credentials, null, null);
  }

  public static void assertCredentialResolution(AwsCredentials creds, String key, String secret) {
    Assertions.assertThat(creds.accessKeyId())
        .describedAs("access key of %s", creds)
        .isEqualTo(key);
    Assertions.assertThat(creds.secretAccessKey())
        .describedAs("secret key of %s", creds)
        .isEqualTo(secret);
  }

  private String buildClassList(Class... classes) {
    return Arrays.stream(classes)
        .map(Class::getCanonicalName)
        .collect(Collectors.joining(","));
  }

  private String buildClassList(String... classes) {
    return Arrays.stream(classes)
        .collect(Collectors.joining(","));
  }

  /**
   * A credential provider declared as abstract, so it cannot be instantiated.
   */
  static abstract class AbstractProvider implements AwsCredentialsProvider {

    @Override
    public AwsCredentials resolveCredentials() {
      return null;
    }
  }

  /**
   * A credential provider whose constructor signature doesn't match.
   */
  protected static class ConstructorSignatureErrorProvider
      extends AbstractProvider {

    @SuppressWarnings("unused")
    public ConstructorSignatureErrorProvider(String str) {
    }
  }

  /**
   * A credential provider whose constructor raises an NPE.
   */
  protected static class ConstructorFailureProvider
      extends AbstractProvider {

    @SuppressWarnings("unused")
    public ConstructorFailureProvider() {
      throw new NullPointerException("oops");
    }

  }

  @Test
  public void testAWSExceptionTranslation() throws Throwable {
    IOException ex = expectProviderInstantiationFailure(
        AWSExceptionRaisingFactory.class,
        AWSExceptionRaisingFactory.NO_AUTH);
    if (!(ex instanceof AccessDeniedException)) {
      throw ex;
    }
  }

  protected static class AWSExceptionRaisingFactory extends AbstractProvider {

    public static final String NO_AUTH = "No auth";

    public static AwsCredentialsProvider create() {
      throw new NoAuthWithAWSException(NO_AUTH);
    }
  }

  @Test
  public void testFactoryWrongType() throws Throwable {
    expectProviderInstantiationFailure(
        FactoryOfWrongType.class,
        InstantiationIOException.CONSTRUCTOR_EXCEPTION);
  }

  static class FactoryOfWrongType extends AbstractProvider {

    public static final String NO_AUTH = "No auth";

    public static String getInstance() {
      return "oops";
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return null;
    }

  }

  /**
   * Expect a provider to raise an exception on failure.
   * @param option aws provider option string.
   * @param expectedErrorText error text to expect
   * @return the exception raised
   * @throws Exception any unexpected exception thrown.
   */
  private IOException expectProviderInstantiationFailure(String option,
      String expectedErrorText) throws Exception {
    return intercept(IOException.class, expectedErrorText,
        () -> createAWSCredentialProviderList(
            TESTFILE_URI,
            createProviderConfiguration(option)));
  }

  /**
   * Expect a provider to raise an exception on failure.
   * @param aClass class to use
   * @param expectedErrorText error text to expect
   * @return the exception raised
   * @throws Exception any unexpected exception thrown.
   */
  private IOException expectProviderInstantiationFailure(Class aClass,
      String expectedErrorText) throws Exception {
    return expectProviderInstantiationFailure(
        buildClassListString(Collections.singletonList(aClass)),
        expectedErrorText);
  }

  /**
   * Create a configuration with a specific provider.
   * @param providerOption option for the aws credential provider option.
   * @return a configuration to use in test cases
   */
  private Configuration createProviderConfiguration(
      final String providerOption) {
    Configuration conf = new Configuration(false);
    conf.set(AWS_CREDENTIALS_PROVIDER, providerOption);
    return conf;
  }

  /**
   * Create a configuration with a specific class.
   * @param aClass class to use
   * @return a configuration to use in test cases
   */
  public Configuration createProviderConfiguration(final Class<?> aClass) {
    return createProviderConfiguration(buildClassListString(
        Collections.singletonList(aClass)));
  }

  /**
   * Asserts expected provider classes in list.
   * @param expectedClasses expected provider classes
   * @param list providers to check
   */
  private static void assertCredentialProviders(
      List<Class<?>> expectedClasses,
      AWSCredentialProviderList list) {
    assertNotNull(list);
    List<AwsCredentialsProvider> providers = list.getProviders();
    Assertions.assertThat(providers)
        .describedAs("providers")
        .hasSize(expectedClasses.size());
    for (int i = 0; i < expectedClasses.size(); ++i) {
      Class<?> expectedClass =
          expectedClasses.get(i);
      AwsCredentialsProvider provider = providers.get(i);
      assertNotNull(
          String.format("At position %d, expected class is %s, but found null.",
              i, expectedClass), provider);
      assertTrue(
          String.format("At position %d, expected class is %s, but found %s.",
              i, expectedClass, provider.getClass()),
          expectedClass.isAssignableFrom(provider.getClass()));
    }
  }

  /**
   * This is here to check up on the S3ATestUtils probes themselves.
   * @see S3ATestUtils#authenticationContains(Configuration, String).
   */
  @Test
  public void testAuthenticationContainsProbes() {
    Configuration conf = new Configuration(false);
    assertFalse("found AssumedRoleCredentialProvider",
        authenticationContains(conf, AssumedRoleCredentialProvider.NAME));

    conf.set(AWS_CREDENTIALS_PROVIDER, AssumedRoleCredentialProvider.NAME);
    assertTrue("didn't find AssumedRoleCredentialProvider",
        authenticationContains(conf, AssumedRoleCredentialProvider.NAME));
  }

  @Test
  public void testExceptionLogic() throws Throwable {
    AWSCredentialProviderList providers
        = new AWSCredentialProviderList();
    // verify you can't get credentials from it
    NoAuthWithAWSException noAuth = intercept(NoAuthWithAWSException.class,
        AWSCredentialProviderList.NO_AWS_CREDENTIAL_PROVIDERS,
        () -> providers.resolveCredentials());
    // but that it closes safely
    providers.close();

    S3ARetryPolicy retryPolicy = new S3ARetryPolicy(new Configuration(false));
    assertEquals("Expected no retry on auth failure",
        RetryPolicy.RetryAction.FAIL.action,
        retryPolicy.shouldRetry(noAuth, 0, 0, true).action);

    try {
      throw S3AUtils.translateException("login", "", noAuth);
    } catch (AccessDeniedException expected) {
      // this is what we want; other exceptions will be passed up
      assertEquals("Expected no retry on AccessDeniedException",
          RetryPolicy.RetryAction.FAIL.action,
          retryPolicy.shouldRetry(expected, 0, 0, true).action);
    }

  }

  @Test
  public void testRefCounting() throws Throwable {
    AWSCredentialProviderList providers
        = new AWSCredentialProviderList();
    assertEquals("Ref count for " + providers,
        1, providers.getRefCount());
    AWSCredentialProviderList replicate = providers.share();
    assertEquals(providers, replicate);
    assertEquals("Ref count after replication for " + providers,
        2, providers.getRefCount());
    assertFalse("Was closed " + providers, providers.isClosed());
    providers.close();
    assertFalse("Was closed " + providers, providers.isClosed());
    assertEquals("Ref count after close() for " + providers,
        1, providers.getRefCount());

    // this should now close it
    providers.close();
    assertTrue("Was not closed " + providers, providers.isClosed());
    assertEquals("Ref count after close() for " + providers,
        0, providers.getRefCount());
    assertEquals("Ref count after second close() for " + providers,
        0, providers.getRefCount());
    intercept(IllegalStateException.class, "closed",
        () -> providers.share());
    // final call harmless
    providers.close();
    assertEquals("Ref count after close() for " + providers,
        0, providers.getRefCount());

    intercept(NoAuthWithAWSException.class,
        AWSCredentialProviderList.CREDENTIALS_REQUESTED_WHEN_CLOSED,
        () -> providers.resolveCredentials());
  }

  /**
   * Verify that IOEs are passed up without being wrapped.
   */
  @Test
  public void testIOEInConstructorPropagation() throws Throwable {
    IOException expected = expectProviderInstantiationFailure(
        IOERaisingProvider.class.getName(),
        "expected");
    if (!(expected instanceof InterruptedIOException)) {
      throw expected;
    }
  }

  /**
   * Credential provider which raises an IOE when constructed.
   */
  protected static class IOERaisingProvider extends AbstractProvider {

    public IOERaisingProvider(URI uri, Configuration conf)
        throws IOException {
      throw new InterruptedIOException("expected");
    }

  }

  private static final AwsCredentials EXPECTED_CREDENTIALS =
      AwsBasicCredentials.create("expectedAccessKey", "expectedSecret");

  /**
   * Credential provider that takes a long time.
   */
  protected static class SlowProvider extends AbstractSessionCredentialsProvider {

    public SlowProvider(@Nullable URI uri, Configuration conf) {
      super(uri, conf);
    }

    @Override
    protected AwsCredentials createCredentials(Configuration config) throws IOException {
      // yield to other callers to induce race condition
      Thread.yield();
      return EXPECTED_CREDENTIALS;
    }
  }

  private static final int CONCURRENT_THREADS = 10;

  @Test
  public void testConcurrentAuthentication() throws Throwable {
    Configuration conf = createProviderConfiguration(SlowProvider.class.getName());
    Path testFile = getCSVTestPath(conf);

    AWSCredentialProviderList list = createAWSCredentialProviderList(testFile.toUri(), conf);

    SlowProvider provider = (SlowProvider) list.getProviders().get(0);

    ExecutorService pool = Executors.newFixedThreadPool(CONCURRENT_THREADS);

    List<Future<AwsCredentials>> results = new ArrayList<>();

    try {
      assertFalse(
          "Provider not initialized. isInitialized should be false",
          provider.isInitialized());
      assertFalse(
          "Provider not initialized. hasCredentials should be false",
          provider.hasCredentials());
      if (provider.getInitializationException() != null) {
        throw new AssertionError(
            "Provider not initialized. getInitializationException should return null",
            provider.getInitializationException());
      }

      for (int i = 0; i < CONCURRENT_THREADS; i++) {
        results.add(pool.submit(() -> list.resolveCredentials()));
      }

      for (Future<AwsCredentials> result : results) {
        AwsCredentials credentials = result.get();
        assertEquals("Access key from credential provider",
            "expectedAccessKey", credentials.accessKeyId());
        assertEquals("Secret key from credential provider",
            "expectedSecret", credentials.secretAccessKey());
      }
    } finally {
      pool.awaitTermination(10, TimeUnit.SECONDS);
      pool.shutdown();
    }

    assertTrue(
        "Provider initialized without errors. isInitialized should be true",
        provider.isInitialized());
    assertTrue(
        "Provider initialized without errors. hasCredentials should be true",
        provider.hasCredentials());
    if (provider.getInitializationException() != null) {
      throw new AssertionError(
          "Provider initialized without errors. getInitializationException should return null",
          provider.getInitializationException());
    }
  }

  /**
   * Credential provider with error.
   */
  protected static class ErrorProvider extends AbstractSessionCredentialsProvider {

    public ErrorProvider(@Nullable URI uri, Configuration conf) {
      super(uri, conf);
    }

    @Override
    protected AwsCredentials createCredentials(Configuration config) throws IOException {
      throw new IOException("expected error");
    }
  }

  @Test
  public void testConcurrentAuthenticationError() throws Throwable {
    Configuration conf = createProviderConfiguration(ErrorProvider.class.getName());
    Path testFile = getCSVTestPath(conf);

    AWSCredentialProviderList list = createAWSCredentialProviderList(testFile.toUri(), conf);
    ErrorProvider provider = (ErrorProvider) list.getProviders().get(0);

    ExecutorService pool = Executors.newFixedThreadPool(CONCURRENT_THREADS);

    List<Future<AwsCredentials>> results = new ArrayList<>();

    try {
      assertFalse("Provider not initialized. isInitialized should be false",
          provider.isInitialized());
      assertFalse("Provider not initialized. hasCredentials should be false",
          provider.hasCredentials());
      if (provider.getInitializationException() != null) {
        throw new AssertionError(
            "Provider not initialized. getInitializationException should return null",
            provider.getInitializationException());
      }

      for (int i = 0; i < CONCURRENT_THREADS; i++) {
        results.add(pool.submit(() -> list.resolveCredentials()));
      }

      for (Future<AwsCredentials> result : results) {
        interceptFuture(CredentialInitializationException.class,
            "expected error",
            result
        );
      }
    } finally {
      pool.awaitTermination(10, TimeUnit.SECONDS);
      pool.shutdown();
    }

    assertTrue(
        "Provider initialization failed. isInitialized should be true",
        provider.isInitialized());
    assertFalse(
        "Provider initialization failed. hasCredentials should be false",
        provider.hasCredentials());
    assertTrue(
        "Provider initialization failed. getInitializationException should contain the error",
        provider.getInitializationException().getMessage().contains("expected error"));
  }


  /**
   * V2 Credentials whose factory method raises ClassNotFoundException.
   * This will fall back to an attempted v1 load which will fail because it
   * is the wrong type.
   * The exception raised will be from the v2 instantiation attempt,
   * not the v1 attempt.
   */
  @Test
  public void testV2ClassNotFound() throws Throwable {
    InstantiationIOException expected = intercept(InstantiationIOException.class,
        "simulated v2 CNFE",
        () -> createAWSCredentialProviderList(
            TESTFILE_URI,
            createProviderConfiguration(V2CredentialProviderDoesNotInstantiate.class.getName())));
    // print for the curious
    LOG.info("{}", expected.toString());
  }

  /**
   * V2 credentials which raises an instantiation exception in
   * the factory method.
   */
  public static final class V2CredentialProviderDoesNotInstantiate
      extends AbstractProvider {

    private V2CredentialProviderDoesNotInstantiate() {
    }

    public static AwsCredentialsProvider create() throws ClassNotFoundException {
      throw new ClassNotFoundException("simulated v2 CNFE");
    }
  }

}
