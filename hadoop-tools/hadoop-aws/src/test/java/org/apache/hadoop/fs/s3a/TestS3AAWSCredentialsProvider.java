/**
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

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link Constants#AWS_CREDENTIALS_PROVIDER} logic.
 */
public class TestS3AAWSCredentialsProvider {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testProviderWrongClass() throws Exception {
    expectProviderInstantiationFailure(this.getClass().getName(),
        NOT_AWS_PROVIDER);
  }

  @Test
  public void testProviderAbstractClass() throws Exception {
    expectProviderInstantiationFailure(AbstractProvider.class.getName(),
        ABSTRACT_PROVIDER);
  }

  @Test
  public void testProviderNotAClass() throws Exception {
    expectProviderInstantiationFailure("NoSuchClass",
        "ClassNotFoundException");
  }

  @Test
  public void testProviderConstructorError() throws Exception {
    expectProviderInstantiationFailure(
        ConstructorSignatureErrorProvider.class.getName(),
        CONSTRUCTOR_EXCEPTION);
  }

  @Test
  public void testProviderFailureError() throws Exception {
    expectProviderInstantiationFailure(
        ConstructorFailureProvider.class.getName(),
        INSTANTIATION_EXCEPTION);
  }

  @Test
  public void testInstantiationChain() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(AWS_CREDENTIALS_PROVIDER,
        TemporaryAWSCredentialsProvider.NAME
            + ", \t" + SimpleAWSCredentialsProvider.NAME
            + " ,\n " + AnonymousAWSCredentialsProvider.NAME);
    Path testFile = new Path(
        conf.getTrimmed(KEY_CSVTEST_FILE, DEFAULT_CSVTEST_FILE));

    URI uri = testFile.toUri();
    AWSCredentialProviderList list = S3AUtils.createAWSCredentialProviderSet(
        uri, conf);
    List<Class<? extends AWSCredentialsProvider>> expectedClasses =
        Arrays.asList(
            TemporaryAWSCredentialsProvider.class,
            SimpleAWSCredentialsProvider.class,
            AnonymousAWSCredentialsProvider.class);
    assertCredentialProviders(expectedClasses, list);
  }

  @Test
  public void testDefaultChain() throws Exception {
    URI uri1 = new URI("s3a://bucket1"), uri2 = new URI("s3a://bucket2");
    Configuration conf = new Configuration();
    AWSCredentialProviderList list1 = S3AUtils.createAWSCredentialProviderSet(
        uri1, conf);
    AWSCredentialProviderList list2 = S3AUtils.createAWSCredentialProviderSet(
        uri2, conf);
    List<Class<? extends AWSCredentialsProvider>> expectedClasses =
        Arrays.asList(
            BasicAWSCredentialsProvider.class,
            EnvironmentVariableCredentialsProvider.class,
            SharedInstanceProfileCredentialsProvider.class);
    assertCredentialProviders(expectedClasses, list1);
    assertCredentialProviders(expectedClasses, list2);
    assertSameInstanceProfileCredentialsProvider(list1.getProviders().get(2),
        list2.getProviders().get(2));
  }

  @Test
  public void testConfiguredChain() throws Exception {
    URI uri1 = new URI("s3a://bucket1"), uri2 = new URI("s3a://bucket2");
    Configuration conf = new Configuration();
    List<Class<? extends AWSCredentialsProvider>> expectedClasses =
        Arrays.asList(
            EnvironmentVariableCredentialsProvider.class,
            SharedInstanceProfileCredentialsProvider.class,
            AnonymousAWSCredentialsProvider.class);
    conf.set(AWS_CREDENTIALS_PROVIDER, buildClassListString(expectedClasses));
    AWSCredentialProviderList list1 = S3AUtils.createAWSCredentialProviderSet(
        uri1, conf);
    AWSCredentialProviderList list2 = S3AUtils.createAWSCredentialProviderSet(
        uri2, conf);
    assertCredentialProviders(expectedClasses, list1);
    assertCredentialProviders(expectedClasses, list2);
    assertSameInstanceProfileCredentialsProvider(list1.getProviders().get(1),
        list2.getProviders().get(1));
  }

  @Test
  public void testConfiguredChainUsesSharedInstanceProfile() throws Exception {
    URI uri1 = new URI("s3a://bucket1"), uri2 = new URI("s3a://bucket2");
    Configuration conf = new Configuration();
    List<Class<? extends AWSCredentialsProvider>> expectedClasses =
        Arrays.<Class<? extends AWSCredentialsProvider>>asList(
            InstanceProfileCredentialsProvider.class);
    conf.set(AWS_CREDENTIALS_PROVIDER, buildClassListString(expectedClasses));
    AWSCredentialProviderList list1 = S3AUtils.createAWSCredentialProviderSet(
        uri1, conf);
    AWSCredentialProviderList list2 = S3AUtils.createAWSCredentialProviderSet(
        uri2, conf);
    assertCredentialProviders(expectedClasses, list1);
    assertCredentialProviders(expectedClasses, list2);
    assertSameInstanceProfileCredentialsProvider(list1.getProviders().get(0),
        list2.getProviders().get(0));
  }

  /**
   * A credential provider declared as abstract, so it cannot be instantiated.
   */
  static abstract class AbstractProvider implements AWSCredentialsProvider {
  }

  /**
   * A credential provider whose constructor signature doesn't match.
   */
  static class ConstructorSignatureErrorProvider
      implements AWSCredentialsProvider {

    @SuppressWarnings("unused")
    public ConstructorSignatureErrorProvider(String str) {
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
   * A credential provider whose constructor raises an NPE.
   */
  static class ConstructorFailureProvider
      implements AWSCredentialsProvider {

    @SuppressWarnings("unused")
    public ConstructorFailureProvider() {
      throw new NullPointerException("oops");
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
   * Declare what exception to raise, and the text which must be found
   * in it.
   * @param exceptionClass class of exception
   * @param text text in exception
   */
  private void expectException(Class<? extends Throwable> exceptionClass,
      String text) {
    exception.expect(exceptionClass);
    exception.expectMessage(text);
  }

  private void expectProviderInstantiationFailure(String option,
      String expectedErrorText) throws IOException {
    Configuration conf = new Configuration();
    conf.set(AWS_CREDENTIALS_PROVIDER, option);
    Path testFile = new Path(
        conf.getTrimmed(KEY_CSVTEST_FILE, DEFAULT_CSVTEST_FILE));
    expectException(IOException.class, expectedErrorText);
    URI uri = testFile.toUri();
    S3AUtils.createAWSCredentialProviderSet(uri, conf);
  }

  /**
   * Asserts expected provider classes in list.
   * @param expectedClasses expected provider classes
   * @param list providers to check
   */
  private static void assertCredentialProviders(
      List<Class<? extends AWSCredentialsProvider>> expectedClasses,
      AWSCredentialProviderList list) {
    assertNotNull(list);
    List<AWSCredentialsProvider> providers = list.getProviders();
    assertEquals(expectedClasses.size(), providers.size());
    for (int i = 0; i < expectedClasses.size(); ++i) {
      Class<? extends AWSCredentialsProvider> expectedClass =
          expectedClasses.get(i);
      AWSCredentialsProvider provider = providers.get(i);
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
   * Asserts that two different references point to the same shared instance of
   * InstanceProfileCredentialsProvider using a descriptive assertion message.
   * @param provider1 provider to check
   * @param provider2 provider to check
   */
  private static void assertSameInstanceProfileCredentialsProvider(
      AWSCredentialsProvider provider1, AWSCredentialsProvider provider2) {
    assertNotNull(provider1);
    assertInstanceOf(InstanceProfileCredentialsProvider.class, provider1);
    assertNotNull(provider2);
    assertInstanceOf(InstanceProfileCredentialsProvider.class, provider2);
    assertSame("Expected all usage of InstanceProfileCredentialsProvider to "
        + "share a singleton instance, but found unique instances.",
        provider1, provider2);
  }
}
