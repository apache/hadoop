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

import java.util.concurrent.TimeUnit;

import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.Signer;
import com.amazonaws.auth.SignerFactory;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.s3a.Constants.CUSTOM_SIGNERS;

/**
 * Tests for the SignerManager.
 */
public class TestSignerManager {

  @Rule
  public Timeout testTimeout = new Timeout(
      10_000L, TimeUnit.MILLISECONDS
  );

  @Test
  public void testCustomSignerFailureIfNotRegistered() throws Exception {
    LambdaTestUtils.intercept(Exception.class,
        () -> SignerFactory.createSigner("testsignerUnregistered", null));
    // Expecting generic Exception.class to handle future implementation
    // changes.
    // For now, this is an NPE
  }

  @Test
  public void testCustomSignerInitialization() {
    Configuration config = new Configuration();
    SignerForTest1.reset();
    SignerForTest2.reset();
    config.set(CUSTOM_SIGNERS, "testsigner1:" + SignerForTest1.class.getName());
    SignerManager signerManager = new SignerManager();
    signerManager.initCustomSigners(config);
    Signer s1 = SignerFactory.createSigner("testsigner1", null);
    s1.sign(null, null);
    Assertions.assertThat(SignerForTest1.initialized)
        .as(SignerForTest1.class.getName() + " not initialized")
        .isEqualTo(true);
  }

  @Test
  public void testMultipleCustomSignerInitialization() {
    Configuration config = new Configuration();
    SignerForTest1.reset();
    SignerForTest2.reset();
    config.set(CUSTOM_SIGNERS,
        "testsigner1:" + SignerForTest1.class.getName() + "," + "testsigner2:"
            + SignerForTest2.class.getName());
    SignerManager signerManager = new SignerManager();
    signerManager.initCustomSigners(config);
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

  /**
   * SignerForTest1.
   */
  @Private
  public static class SignerForTest1 implements Signer {

    private static boolean initialized = false;

    @Override
    public void sign(SignableRequest<?> request, AWSCredentials credentials) {
      initialized = true;
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
    public void sign(SignableRequest<?> request, AWSCredentials credentials) {
      initialized = true;
    }

    public static void reset() {
      initialized = false;
    }
  }
}
