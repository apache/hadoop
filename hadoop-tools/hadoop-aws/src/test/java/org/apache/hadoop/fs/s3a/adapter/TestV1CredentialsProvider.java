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

package org.apache.hadoop.fs.s3a.adapter;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider;
import org.apache.hadoop.fs.s3a.impl.InstantiationIOException;
import org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils;

import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory.ANONYMOUS_CREDENTIALS_V1;
import static org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory.EC2_CONTAINER_CREDENTIALS_V1;
import static org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory.ENVIRONMENT_CREDENTIALS_V1;
import static org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory.createAWSCredentialProviderList;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for v1 to v2 credential provider logic.
 */
public class TestV1CredentialsProvider {

  /**
   * URI of the test file.
   */
  private static final URI TESTFILE_URI = new Path(
      PublicDatasetTestUtils.DEFAULT_EXTERNAL_FILE).toUri();

  private static final Logger LOG = LoggerFactory.getLogger(TestV1CredentialsProvider.class);


  @Test
  public void testV1V2Mapping() throws Exception {
    URI uri1 = new URI("s3a://bucket1");

    List<Class<?>> expectedClasses =
        Arrays.asList(
            IAMInstanceCredentialsProvider.class,
            AnonymousAWSCredentialsProvider.class,
            EnvironmentVariableCredentialsProvider.class);
    Configuration conf =
        createProviderConfiguration(buildClassList(
            EC2_CONTAINER_CREDENTIALS_V1,
            ANONYMOUS_CREDENTIALS_V1,
            ENVIRONMENT_CREDENTIALS_V1));
    AWSCredentialProviderList list1 = createAWSCredentialProviderList(
        uri1, conf);
    assertCredentialProviders(expectedClasses, list1);
  }

  @Test
  public void testV1Wrapping() throws Exception {
    URI uri1 = new URI("s3a://bucket1");

    List<Class<?>> expectedClasses =
        Arrays.asList(
            V1ToV2AwsCredentialProviderAdapter.class,
            V1ToV2AwsCredentialProviderAdapter.class);
    Configuration conf =
        createProviderConfiguration(buildClassList(
            LegacyV1CredentialProvider.class.getName(),
            LegacyV1CredentialProviderWithConf.class.getName()));
    AWSCredentialProviderList list1 = createAWSCredentialProviderList(
        uri1, conf);
    assertCredentialProviders(expectedClasses, list1);
  }

  private String buildClassList(String... classes) {
    return Arrays.stream(classes)
        .collect(Collectors.joining(","));
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


  public static class LegacyV1CredentialProvider implements AWSCredentialsProvider {

    public LegacyV1CredentialProvider() {
    }

    @Override
    public AWSCredentials getCredentials() {
      return null;
    }

    @Override
    public void refresh() {

    }
  }

  /**
   * V1 credentials with a configuration constructor.
   */
  public static final class LegacyV1CredentialProviderWithConf
      extends LegacyV1CredentialProvider {

    public LegacyV1CredentialProviderWithConf(Configuration conf) {
    }
  }

  /**
   * V1 Credentials whose factory method raises ClassNotFoundException.
   * Expect this to fail rather than trigger recursive recovery;
   * exception will be wrapped with something intended to be informative.
   */
  @Test
  public void testV1InstantiationFailurePropagation() throws Throwable {
    InstantiationIOException expected = intercept(InstantiationIOException.class,
        "simulated CNFE",
        () -> createAWSCredentialProviderList(
            TESTFILE_URI,
            createProviderConfiguration(V1CredentialProviderDoesNotInstantiate.class.getName())));
    // print for the curious
    LOG.info("{}", expected.toString());
  }


  /**
   * V1 credentials which raises an instantiation exception.
   */
  public static final class V1CredentialProviderDoesNotInstantiate
      extends LegacyV1CredentialProvider {

    private V1CredentialProviderDoesNotInstantiate() {
    }

    public static AWSCredentialsProvider getInstance() throws ClassNotFoundException {
      throw new ClassNotFoundException("simulated CNFE");
    }
  }


}
