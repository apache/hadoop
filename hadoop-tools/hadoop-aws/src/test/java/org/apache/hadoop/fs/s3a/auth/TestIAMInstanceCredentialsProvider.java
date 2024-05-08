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

package org.apache.hadoop.fs.s3a.auth;

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

import org.apache.hadoop.test.AbstractHadoopTestBase;

/**
 * Unit tests for IAMInstanceCredentials provider.
 * This is a bit tricky as we don't want to require running in EC2,
 * but nor do we want a test which doesn't work in EC2.
 */
public class TestIAMInstanceCredentialsProvider extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestIAMInstanceCredentialsProvider.class);

  /**
   * Error string from
   * software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider,
   * if IAM resolution has been disabled: {@value}.
   */
  public static final String DISABLED =
      "IMDS credentials have been disabled by environment variable or system property";

  /**
   * Test an immediate create/close.
   */
  @Test
  public void testIAMInstanceCredentialsProviderClose() throws Throwable {
    new IAMInstanceCredentialsProvider().close();
  }

  /**
   * Test instantiation.
   * Multiple outcomes depending on host setup.
   * <ol>
   *   <li> In EC2: credentials resolved.
   *        Assert the credentials comes with a key.</li>
   *   <li> Not in EC2: NoAwsCredentialsException wraps network error trying
   *        to talk to the service.
   *        Assert wrapped exception is an IOE.</li>
   *   <li> IMDS resolution disabled by env var/sysprop.
   *        NoAwsCredentialsException raised doesn't contain an IOE.
   *        Require the message to contain the {@link #DISABLED} text.</li>j
   * </ol>
   */
  @Test
  public void testIAMInstanceCredentialsInstantiate() throws Throwable {
    try (IAMInstanceCredentialsProvider provider = new IAMInstanceCredentialsProvider()) {
      try {
        final AwsCredentials credentials = provider.resolveCredentials();
        // if we get here this test suite is running in a container/EC2
        LOG.info("Credentials: retrieved from {}: key={}",
            provider.isContainerCredentialsProvider() ? "container" : "EC2",
            credentials.accessKeyId());
        Assertions.assertThat(credentials.accessKeyId())
            .describedAs("Access key from IMDS")
            .isNotBlank();

        // and if we get here, so does a second call
        provider.resolveCredentials();
      } catch (NoAwsCredentialsException expected) {
        // this is expected if the test is not running in a container/EC2
        LOG.info("Not running in a container/EC2");
        LOG.info("Exception raised", expected);
        // and we expect to have fallen back to InstanceProfileCredentialsProvider
        Assertions.assertThat(provider.isContainerCredentialsProvider())
            .describedAs("%s: shoud be using InstanceProfileCredentialsProvider")
            .isFalse();
        final Throwable cause = expected.getCause();
        if (cause == null) {
          throw expected;
        }
        if (!(cause instanceof IOException)
            && !cause.toString().contains(DISABLED)) {
          throw new AssertionError("Cause not a IOException", cause);
        }
      }
    }
  }


}
