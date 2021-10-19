/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn;

import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestCosCredentials {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCosCredentials.class);

  private final URI fsUri;

  private final String testCosNSecretId = "secretId";
  private final String testCosNSecretKey = "secretKey";
  private final String testCosNEnvSecretId = "env_secretId";
  private final String testCosNEnvSecretKey = "env_secretKey";

  public TestCosCredentials() throws URISyntaxException {
    // A fake uri for tests.
    this.fsUri = new URI("cosn://test-bucket-1250000000");
  }

  @Test
  public void testSimpleCredentialsProvider() throws Throwable {
    Configuration configuration = new Configuration();
    configuration.set(CosNConfigKeys.COSN_SECRET_ID_KEY,
        testCosNSecretId);
    configuration.set(CosNConfigKeys.COSN_SECRET_KEY_KEY,
        testCosNSecretKey);
    validateCredentials(this.fsUri, configuration);
  }

  @Test
  public void testEnvironmentCredentialsProvider() throws Throwable {
    Configuration configuration = new Configuration();
    // Set EnvironmentVariableCredentialsProvider as the CosCredentials
    // Provider.
    configuration.set(CosNConfigKeys.COSN_CREDENTIALS_PROVIDER,
        "org.apache.hadoop.fs.cosn.EnvironmentVariableCredentialsProvider");
    // Set the environment variables storing the secret id and secret key.
    System.setProperty(Constants.COSN_SECRET_ID_ENV, testCosNEnvSecretId);
    System.setProperty(Constants.COSN_SECRET_KEY_ENV, testCosNEnvSecretKey);
    validateCredentials(this.fsUri, configuration);
  }

  private void validateCredentials(URI uri, Configuration configuration)
      throws IOException {
    if (null != configuration) {
      COSCredentialsProvider credentialsProvider =
          CosNUtils.createCosCredentialsProviderSet(uri, configuration);
      COSCredentials cosCredentials = credentialsProvider.getCredentials();
      assertNotNull("The cos credentials obtained is null.", cosCredentials);
      if (configuration.get(
          CosNConfigKeys.COSN_CREDENTIALS_PROVIDER).compareToIgnoreCase(
          "org.apache.hadoop.fs.cosn.EnvironmentVariableCredentialsProvider")
          == 0) {
        if (null == cosCredentials.getCOSAccessKeyId()
            || cosCredentials.getCOSAccessKeyId().isEmpty()
            || null == cosCredentials.getCOSSecretKey()
            || cosCredentials.getCOSSecretKey().isEmpty()) {
          String failMessage = String.format(
              "Test EnvironmentVariableCredentialsProvider failed. The " +
                  "expected is [secretId: %s, secretKey: %s], but got null or" +
                  " empty.", testCosNEnvSecretId, testCosNEnvSecretKey);
          fail(failMessage);
        }

        if (cosCredentials.getCOSAccessKeyId()
            .compareTo(testCosNEnvSecretId) != 0
            || cosCredentials.getCOSSecretKey()
            .compareTo(testCosNEnvSecretKey) != 0) {
          String failMessage = String.format("Test " +
                  "EnvironmentVariableCredentialsProvider failed. " +
                  "The expected is [secretId: %s, secretKey: %s], but got is " +
                  "[secretId:%s, secretKey:%s].", testCosNEnvSecretId,
              testCosNEnvSecretKey, cosCredentials.getCOSAccessKeyId(),
              cosCredentials.getCOSSecretKey());
        }
        // expected
      } else {
        if (null == cosCredentials.getCOSAccessKeyId()
            || cosCredentials.getCOSAccessKeyId().isEmpty()
            || null == cosCredentials.getCOSSecretKey()
            || cosCredentials.getCOSSecretKey().isEmpty()) {
          String failMessage = String.format(
              "Test COSCredentials failed. The " +
                  "expected is [secretId: %s, secretKey: %s], but got null or" +
                  " empty.", testCosNSecretId, testCosNSecretKey);
          fail(failMessage);
        }
        if (cosCredentials.getCOSAccessKeyId()
            .compareTo(testCosNSecretId) != 0
            || cosCredentials.getCOSSecretKey()
            .compareTo(testCosNSecretKey) != 0) {
          String failMessage = String.format("Test " +
                  "EnvironmentVariableCredentialsProvider failed. " +
                  "The expected is [secretId: %s, secretKey: %s], but got is " +
                  "[secretId:%s, secretKey:%s].", testCosNSecretId,
              testCosNSecretKey, cosCredentials.getCOSAccessKeyId(),
              cosCredentials.getCOSSecretKey());
          fail(failMessage);
        }
        // expected
      }
    }
  }
}
