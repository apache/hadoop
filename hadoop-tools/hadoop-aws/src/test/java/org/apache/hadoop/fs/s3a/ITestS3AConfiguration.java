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

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;

import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.StsException;
import software.amazon.encryption.s3.S3EncryptionClient;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.auth.STSClientFactory;
import org.apache.hadoop.fs.s3a.impl.AWSClientConfig;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.http.HttpStatus;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.EU_WEST_1;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.TEST_FS_S3A_NAME;
import static org.junit.Assert.*;

/**
 * S3A tests for configuration, especially credentials.
 */
public class ITestS3AConfiguration extends AbstractHadoopTestBase {
  private static final String EXAMPLE_ID = "AKASOMEACCESSKEY";
  private static final String EXAMPLE_KEY =
      "RGV0cm9pdCBSZ/WQgY2xl/YW5lZCB1cAEXAMPLE";
  private static final String AP_ILLEGAL_ACCESS =
      "ARN of type accesspoint cannot be passed as a bucket";

  private Configuration conf;
  private S3AFileSystem fs;

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AConfiguration.class);

  @Rule
  public Timeout testTimeout = new Timeout(
      S3ATestConstants.S3A_TEST_TIMEOUT
  );

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  /**
   * Get the S3 client of the active filesystem.
   * @param reason why?
   * @return the client
   */
  private S3Client getS3Client(String reason) {
    return requireNonNull(getS3AInternals().getAmazonS3Client(reason));
  }

  /**
   * Get the internals of the active filesystem.
   * @return the internals
   */
  private S3AInternals getS3AInternals() {
    return fs.getS3AInternals();
  }

  /**
   * Test if custom endpoint is picked up.
   * <p>
   * The test expects {@link S3ATestConstants#CONFIGURATION_TEST_ENDPOINT}
   * to be defined in the Configuration
   * describing the endpoint of the bucket to which TEST_FS_S3A_NAME points
   * (i.e. "s3-eu-west-1.amazonaws.com" if the bucket is located in Ireland).
   * Evidently, the bucket has to be hosted in the region denoted by the
   * endpoint for the test to succeed.
   * <p>
   * More info and the list of endpoint identifiers:
   * @see <a href="http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">endpoint list</a>.
   *
   * @throws Exception
   */
  @Test
  public void testEndpoint() throws Exception {
    conf = new Configuration();
    String endpoint = conf.getTrimmed(
        S3ATestConstants.CONFIGURATION_TEST_ENDPOINT, "");
    if (endpoint.isEmpty()) {
      LOG.warn("Custom endpoint test skipped as " +
          S3ATestConstants.CONFIGURATION_TEST_ENDPOINT + " config " +
          "setting was not detected");
    } else {
      conf.set(Constants.ENDPOINT, endpoint);
      fs = S3ATestUtils.createTestFileSystem(conf);
      String endPointRegion = "";
      // Differentiate handling of "s3-" and "s3." based endpoint identifiers
      String[] endpointParts = StringUtils.split(endpoint, '.');
      if (endpointParts.length == 3) {
        endPointRegion = endpointParts[0].substring(3);
      } else if (endpointParts.length == 4) {
        endPointRegion = endpointParts[1];
      } else {
        fail("Unexpected endpoint");
      }
      String region = getS3AInternals().getBucketLocation();
      assertEquals("Endpoint config setting and bucket location differ: ",
          endPointRegion, region);
    }
  }

  @Test
  public void testProxyConnection() throws Exception {
    useFailFastConfiguration();
    conf.set(Constants.PROXY_HOST, "127.0.0.1");
    conf.setInt(Constants.PROXY_PORT, 1);
    String proxy =
        conf.get(Constants.PROXY_HOST) + ":" + conf.get(Constants.PROXY_PORT);
    expectFSCreateFailure(ConnectException.class,
        conf, "when using proxy " + proxy);
  }

  /**
   * Create a configuration designed to fail fast on network problems.
   */
  protected void useFailFastConfiguration() {
    conf = new Configuration();
    conf.setInt(Constants.MAX_ERROR_RETRIES, 2);
    conf.setInt(Constants.RETRY_LIMIT, 2);
    conf.set(RETRY_INTERVAL, "100ms");
  }

  /**
   * Expect a filesystem to not be created from a configuration.
   * @return the exception intercepted
   * @throws Exception any other exception
   */
  private <E extends Throwable> E expectFSCreateFailure(
      Class<E> clazz,
      Configuration conf,
      String text)
      throws Exception {

    return intercept(clazz,
        () -> {
          fs = S3ATestUtils.createTestFileSystem(conf);
          fs.listFiles(new Path("/"), false);
          return "expected failure creating FS " + text + " got " + fs;
        });
  }

  @Test
  public void testProxyPortWithoutHost() throws Exception {
    useFailFastConfiguration();
    conf.unset(Constants.PROXY_HOST);
    conf.setInt(Constants.PROXY_PORT, 1);
    IllegalArgumentException e = expectFSCreateFailure(
        IllegalArgumentException.class,
        conf, "Expected a connection error for proxy server");
    String msg = e.toString();
    if (!msg.contains(Constants.PROXY_HOST) &&
        !msg.contains(Constants.PROXY_PORT)) {
      throw e;
    }
  }

  @Test
  public void testAutomaticProxyPortSelection() throws Exception {
    useFailFastConfiguration();
    conf.unset(Constants.PROXY_PORT);
    conf.set(Constants.PROXY_HOST, "127.0.0.1");
    conf.set(Constants.SECURE_CONNECTIONS, "true");
    expectFSCreateFailure(ConnectException.class,
        conf, "Expected a connection error for proxy server");
    conf.set(Constants.SECURE_CONNECTIONS, "false");
    expectFSCreateFailure(ConnectException.class,
        conf, "Expected a connection error for proxy server");
  }

  @Test
  public void testUsernameInconsistentWithPassword() throws Exception {
    useFailFastConfiguration();
    conf.set(Constants.PROXY_HOST, "127.0.0.1");
    conf.setInt(Constants.PROXY_PORT, 1);
    conf.set(Constants.PROXY_USERNAME, "user");
    IllegalArgumentException e = expectFSCreateFailure(
        IllegalArgumentException.class,
        conf, "Expected a connection error for proxy server");
    assertIsProxyUsernameError(e);
  }

  private void assertIsProxyUsernameError(final IllegalArgumentException e) {
    String msg = e.toString();
    if (!msg.contains(Constants.PROXY_USERNAME) &&
        !msg.contains(Constants.PROXY_PASSWORD)) {
      throw e;
    }
  }

  @Test
  public void testUsernameInconsistentWithPassword2() throws Exception {
    useFailFastConfiguration();
    conf.set(Constants.PROXY_HOST, "127.0.0.1");
    conf.setInt(Constants.PROXY_PORT, 1);
    conf.set(Constants.PROXY_PASSWORD, "password");
    IllegalArgumentException e = expectFSCreateFailure(
        IllegalArgumentException.class,
        conf, "Expected a connection error for proxy server");
    assertIsProxyUsernameError(e);
  }

  @Test
  public void testCredsFromCredentialProvider() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionAccessKeys(conf);

    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    S3xLoginHelper.Login creds =
        S3AUtils.getAWSAccessKeys(new URI("s3a://foobar"), conf);
    assertEquals("AccessKey incorrect.", EXAMPLE_ID, creds.getUser());
    assertEquals("SecretKey incorrect.", EXAMPLE_KEY, creds.getPassword());
  }

  void provisionAccessKeys(final Configuration conf) throws Exception {
    // add our creds to the provider
    final CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry(Constants.ACCESS_KEY,
        EXAMPLE_ID.toCharArray());
    provider.createCredentialEntry(Constants.SECRET_KEY,
        EXAMPLE_KEY.toCharArray());
    provider.flush();
  }

  @Test
  public void testSecretFromCredentialProviderIDFromConfig() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    // add our creds to the provider
    final CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry(Constants.SECRET_KEY,
        EXAMPLE_KEY.toCharArray());
    provider.flush();

    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID);
    S3xLoginHelper.Login creds =
        S3AUtils.getAWSAccessKeys(new URI("s3a://foobar"), conf);
    assertEquals("AccessKey incorrect.", EXAMPLE_ID, creds.getUser());
    assertEquals("SecretKey incorrect.", EXAMPLE_KEY, creds.getPassword());
  }

  @Test
  public void testIDFromCredentialProviderSecretFromConfig() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    // add our creds to the provider
    final CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry(Constants.ACCESS_KEY,
        EXAMPLE_ID.toCharArray());
    provider.flush();

    conf.set(Constants.SECRET_KEY, EXAMPLE_KEY);
    S3xLoginHelper.Login creds =
        S3AUtils.getAWSAccessKeys(new URI("s3a://foobar"), conf);
    assertEquals("AccessKey incorrect.", EXAMPLE_ID, creds.getUser());
    assertEquals("SecretKey incorrect.", EXAMPLE_KEY, creds.getPassword());
  }

  @Test
  public void testExcludingS3ACredentialProvider() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        "jceks://s3a/foobar," + jks.toString());

    // first make sure that the s3a based provider is removed
    Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(
        conf, S3AFileSystem.class);
    String newPath = conf.get(
        CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH);
    assertFalse("Provider Path incorrect", newPath.contains("s3a://"));

    // now let's make sure the new path is created by the S3AFileSystem
    // and the integration still works. Let's provision the keys through
    // the altered configuration instance and then try and access them
    // using the original config with the s3a provider in the path.
    provisionAccessKeys(c);

    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    URI uri2 = new URI("s3a://foobar");
    S3xLoginHelper.Login creds =
        S3AUtils.getAWSAccessKeys(uri2, conf);
    assertEquals("AccessKey incorrect.", EXAMPLE_ID, creds.getUser());
    assertEquals("SecretKey incorrect.", EXAMPLE_KEY, creds.getPassword());

  }

  @Test
  public void shouldBeAbleToSwitchOnS3PathStyleAccessViaConfigProperty()
      throws Exception {

    conf = new Configuration();
    skipIfCrossRegionClient(conf);
    unsetEncryption(conf);
    conf.set(Constants.PATH_STYLE_ACCESS, Boolean.toString(true));
    assertTrue(conf.getBoolean(Constants.PATH_STYLE_ACCESS, false));

    try {
      fs = S3ATestUtils.createTestFileSystem(conf);
      assertNotNull(fs);
      S3Client s3 = getS3Client("configuration");

      SdkClientConfiguration clientConfiguration = getField(s3, SdkClientConfiguration.class,
          "clientConfiguration");
      S3Configuration s3Configuration =
          (S3Configuration)clientConfiguration.option(SdkClientOption.SERVICE_CONFIGURATION);
      assertTrue("Expected to find path style access to be switched on!",
          s3Configuration.pathStyleAccessEnabled());
      byte[] file = ContractTestUtils.toAsciiByteArray("test file");
      ContractTestUtils.writeAndRead(fs,
          createTestPath(new Path("/path/style/access/testFile")),
          file, file.length,
          (int) conf.getLongBytes(Constants.FS_S3A_BLOCK_SIZE, file.length),
          false, true);
    } catch (final AWSRedirectException e) {
      LOG.error("Caught exception: ", e);
      // Catch/pass standard path style access behaviour when live bucket
      // isn't in the same region as the s3 client default. See
      // http://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html
      assertEquals(HttpStatus.SC_MOVED_PERMANENTLY, e.statusCode());
    } catch (final IllegalArgumentException e) {
      // Path style addressing does not work with AP ARNs
      if (!fs.getBucket().contains("arn:")) {
        LOG.error("Caught unexpected exception: ", e);
        throw e;
      }

      GenericTestUtils.assertExceptionContains(AP_ILLEGAL_ACCESS, e);
    }
  }

  @Test
  public void testDefaultUserAgent() throws Exception {
    conf = new Configuration();
    skipIfCrossRegionClient(conf);
    unsetEncryption(conf);
    fs = S3ATestUtils.createTestFileSystem(conf);
    assertNotNull(fs);
    S3Client s3 = getS3Client("User Agent");
    SdkClientConfiguration clientConfiguration = getField(s3, SdkClientConfiguration.class,
        "clientConfiguration");
    Assertions.assertThat(clientConfiguration.option(SdkClientOption.CLIENT_USER_AGENT))
        .describedAs("User Agent prefix")
        .startsWith("Hadoop " + VersionInfo.getVersion());
  }

  @Test
  public void testCustomUserAgent() throws Exception {
    conf = new Configuration();
    skipIfCrossRegionClient(conf);
    unsetEncryption(conf);
    conf.set(Constants.USER_AGENT_PREFIX, "MyApp");
    fs = S3ATestUtils.createTestFileSystem(conf);
    assertNotNull(fs);
    S3Client s3 = getS3Client("User agent");
    SdkClientConfiguration clientConfiguration = getField(s3, SdkClientConfiguration.class,
        "clientConfiguration");
    Assertions.assertThat(clientConfiguration.option(SdkClientOption.CLIENT_USER_AGENT))
        .describedAs("User Agent prefix")
        .startsWith("MyApp, Hadoop " + VersionInfo.getVersion());
  }

  @Test
  public void testRequestTimeout() throws Exception {
    conf = new Configuration();
    skipIfCrossRegionClient(conf);
    // remove the safety check on minimum durations.
    AWSClientConfig.setMinimumOperationDuration(Duration.ZERO);
    try {
      Duration timeout = Duration.ofSeconds(120);
      conf.set(REQUEST_TIMEOUT, timeout.getSeconds() + "s");
      fs = S3ATestUtils.createTestFileSystem(conf);
      SdkClient s3 = getS3Client("Request timeout (ms)");
      if (s3 instanceof S3EncryptionClient) {
        s3 = ((S3EncryptionClient) s3).delegate();
      }
      SdkClientConfiguration clientConfiguration = getField(s3, SdkClientConfiguration.class,
          "clientConfiguration");
      Assertions.assertThat(clientConfiguration.option(SdkClientOption.API_CALL_ATTEMPT_TIMEOUT))
          .describedAs("Configured " + REQUEST_TIMEOUT +
              " is different than what AWS sdk configuration uses internally")
          .isEqualTo(timeout);
    } finally {
      AWSClientConfig.resetMinimumOperationDuration();
    }
  }

  @Test
  public void testCloseIdempotent() throws Throwable {
    conf = new Configuration();
    fs = S3ATestUtils.createTestFileSystem(conf);
    AWSCredentialProviderList credentials =
        getS3AInternals().shareCredentials("testCloseIdempotent");
    credentials.close();
    fs.close();
    assertTrue("Closing FS didn't close credentials " + credentials,
        credentials.isClosed());
    assertEquals("refcount not zero in " + credentials, 0, credentials.getRefCount());
    fs.close();
    // and the numbers should not change
    assertEquals("refcount not zero in " + credentials, 0, credentials.getRefCount());
  }

  @Test
  public void testDirectoryAllocatorDefval() throws Throwable {
    conf = new Configuration();
    conf.unset(Constants.BUFFER_DIR);
    fs = S3ATestUtils.createTestFileSystem(conf);
    File tmp = createTemporaryFileForWriting();
    assertTrue("not found: " + tmp, tmp.exists());
    tmp.delete();
  }

  /**
   * Create a temporary file for writing; requires the FS to have been created/initialized.
   * @return a temporary file
   * @throws IOException creation issues.
   */
  private File createTemporaryFileForWriting() throws IOException {
    return fs.getS3AInternals().getStore().createTemporaryFileForWriting("out-", 1024, conf);
  }

  @Test
  public void testDirectoryAllocatorRR() throws Throwable {
    File dir1 = GenericTestUtils.getRandomizedTestDir();
    File dir2 = GenericTestUtils.getRandomizedTestDir();
    dir1.mkdirs();
    dir2.mkdirs();
    conf = new Configuration();
    conf.set(Constants.BUFFER_DIR, dir1 + ", " + dir2);
    fs = S3ATestUtils.createTestFileSystem(conf);
    File tmp1 = createTemporaryFileForWriting();
    tmp1.delete();
    File tmp2 = createTemporaryFileForWriting();
    tmp2.delete();
    assertNotEquals("round robin not working",
        tmp1.getParent(), tmp2.getParent());
  }

  @Test
  public void testUsernameFromUGI() throws Throwable {
    final String alice = "alice";
    UserGroupInformation fakeUser =
        UserGroupInformation.createUserForTesting(alice,
            new String[]{"users", "administrators"});
    conf = new Configuration();
    fs = fakeUser.doAs(new PrivilegedExceptionAction<S3AFileSystem>() {
      @Override
      public S3AFileSystem run() throws Exception{
        return S3ATestUtils.createTestFileSystem(conf);
      }
    });
    assertEquals("username", alice, fs.getUsername());
    FileStatus status = fs.getFileStatus(new Path("/"));
    assertEquals("owner in " + status, alice, status.getOwner());
    assertEquals("group in " + status, alice, status.getGroup());
  }

  /**
   * Reads and returns a field from an object using reflection.  If the field
   * cannot be found, is null, or is not the expected type, then this method
   * fails the test.
   *
   * @param target object to read
   * @param fieldType type of field to read, which will also be the return type
   * @param fieldName name of field to read
   * @return field that was read
   * @throws IllegalAccessException if access not allowed
   */
  private static <T> T getField(Object target, Class<T> fieldType,
      String fieldName) throws IllegalAccessException {
    Object obj = FieldUtils.readField(target, fieldName, true);
    assertNotNull(String.format(
        "Could not read field named %s in object with class %s.", fieldName,
        target.getClass().getName()), obj);
    assertTrue(String.format(
        "Unexpected type found for field named %s, expected %s, actual %s.",
        fieldName, fieldType.getName(), obj.getClass().getName()),
        fieldType.isAssignableFrom(obj.getClass()));
    return fieldType.cast(obj);
  }

  @Test
  public void testConfOptionPropagationToFS() throws Exception {
    Configuration config = new Configuration();
    String testFSName = config.getTrimmed(TEST_FS_S3A_NAME, "");
    String bucket = new URI(testFSName).getHost();
    setBucketOption(config, bucket, "propagation", "propagated");
    fs = S3ATestUtils.createTestFileSystem(config);
    Configuration updated = fs.getConf();
    assertOptionEquals(updated, "fs.s3a.propagation", "propagated");
  }

  @Test(timeout = 10_000L)
  public void testS3SpecificSignerOverride() throws Exception {
    Configuration config = new Configuration();
    removeBaseAndBucketOverrides(config,
        CUSTOM_SIGNERS, SIGNING_ALGORITHM_S3, SIGNING_ALGORITHM_STS, AWS_REGION);

    config.set(CUSTOM_SIGNERS,
        "CustomS3Signer:" + CustomS3Signer.class.getName()
            + ",CustomSTSSigner:" + CustomSTSSigner.class.getName());

    config.set(SIGNING_ALGORITHM_S3, "CustomS3Signer");
    config.set(SIGNING_ALGORITHM_STS, "CustomSTSSigner");

    config.set(AWS_REGION, EU_WEST_1);
    disableFilesystemCaching(config);
    fs = S3ATestUtils.createTestFileSystem(config);

    S3Client s3Client = getS3Client("testS3SpecificSignerOverride");

    final String bucket = fs.getBucket();
    StsClient stsClient =
        STSClientFactory.builder(config, bucket, new AnonymousAWSCredentialsProvider(), "",
            "").build();

    intercept(StsException.class, "", () ->
        stsClient.getSessionToken());

    final IOException ioe = intercept(IOException.class, "", () ->
        Invoker.once("head", bucket, () ->
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build())));

    Assertions.assertThat(CustomS3Signer.isS3SignerCalled())
        .describedAs("Custom S3 signer not called").isTrue();

    Assertions.assertThat(CustomSTSSigner.isSTSSignerCalled())
        .describedAs("Custom STS signer not called").isTrue();
  }

  public static final class CustomS3Signer implements Signer {

    private static boolean s3SignerCalled = false;

    @Override
    public SdkHttpFullRequest sign(SdkHttpFullRequest request,
        ExecutionAttributes executionAttributes) {
      LOG.debug("Custom S3 signer called");
      s3SignerCalled = true;
      return request;
    }

    public static boolean isS3SignerCalled() {
      return s3SignerCalled;
    }
  }

  public static final class CustomSTSSigner implements Signer {

    private static boolean stsSignerCalled = false;

    @Override
    public SdkHttpFullRequest sign(SdkHttpFullRequest request,
        ExecutionAttributes executionAttributes) {
      LOG.debug("Custom STS signer called");
      stsSignerCalled = true;
      return request;
    }

    public static boolean isSTSSignerCalled() {
      return stsSignerCalled;
    }
  }

  /**
   * Skip a test if client created is cross region.
   * @param configuration configuration to probe
   */
  private static void skipIfCrossRegionClient(
      Configuration configuration) {
    if (configuration.getBoolean(AWS_S3_CROSS_REGION_ACCESS_ENABLED,
        AWS_S3_CROSS_REGION_ACCESS_ENABLED_DEFAULT)) {
      skip("Skipping test as cross region client is in use ");
    }
  }
}
