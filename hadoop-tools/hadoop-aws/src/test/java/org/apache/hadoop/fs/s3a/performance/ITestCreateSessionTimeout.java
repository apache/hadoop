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

package org.apache.hadoop.fs.s3a.performance;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AWSApiCallTimeoutException;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.auth.CustomHttpSigner;
import org.apache.hadoop.fs.s3a.impl.AWSClientConfig;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.s3a.Constants.CUSTOM_SIGNERS;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_CLASS_NAME;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.RETRY_LIMIT;
import static org.apache.hadoop.fs.s3a.Constants.S3A_BUCKET_PROBE;
import static org.apache.hadoop.fs.s3a.Constants.S3EXPRESS_CREATE_SESSION;
import static org.apache.hadoop.fs.s3a.Constants.SIGNING_ALGORITHM_S3;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfNotS3ExpressBucket;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test timeout of S3 Client CreateSession call, which was originally
 * hard coded to 10 seconds.
 * Only executed against an S3Express store.
 */
public class ITestCreateSessionTimeout extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestCreateSessionTimeout.class);

  /**
   * What is the duration for the operation after which the test is considered
   * to have failed because timeouts didn't get passed down?
   */
  private static final long TIMEOUT_EXCEPTION_THRESHOLD = Duration.ofSeconds(5).toMillis();

  /**
   * How long to sleep in requests?
   */
  private static final AtomicLong SLEEP_DURATION = new AtomicLong(
      Duration.ofSeconds(20).toMillis());

  /**
   * Flag set if the sleep was interrupted during signing.
   */
  private static final AtomicBoolean SLEEP_INTERRUPTED = new AtomicBoolean(false);

  /**
   * Create a configuration with a 10 millisecond timeout on API calls
   * and a custom signer which sleeps much longer than that.
   * @return the configuration.
   */
  @Override
  public Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();
    skipIfNotS3ExpressBucket(conf);
    disableFilesystemCaching(conf);
    removeBaseAndBucketOverrides(conf,
        CUSTOM_SIGNERS,
        HTTP_SIGNER_ENABLED,
        REQUEST_TIMEOUT,
        RETRY_LIMIT,
        S3A_BUCKET_PROBE,
        S3EXPRESS_CREATE_SESSION,
        SIGNING_ALGORITHM_S3
    );

    conf.setBoolean(HTTP_SIGNER_ENABLED, true);
    conf.setClass(HTTP_SIGNER_CLASS_NAME, SlowSigner.class, HttpSigner.class);
    Duration duration = Duration.ofMillis(10);

    conf.setLong(REQUEST_TIMEOUT, duration.toMillis());
    conf.setInt(RETRY_LIMIT, 1);

    return conf;
  }

  @Override
  public void setup() throws Exception {
    // remove the safety check on minimum durations.
    AWSClientConfig.setMinimumOperationDuration(Duration.ZERO);
    try {
      super.setup();
    } finally {
      // restore the safety check on minimum durations.
      AWSClientConfig.resetMinimumOperationDuration();
    }
  }

  @Override
  protected void deleteTestDirInTeardown() {
    // no-op
  }

  /**
   * Make this a no-op to avoid IO.
   * @param path path path
   */
  @Override
  protected void mkdirs(Path path) {

  }

  @Test
  public void testSlowSigningTriggersTimeout() throws Throwable {

    final S3AFileSystem fs = getFileSystem();
    DurationInfo call = new DurationInfo(LOG, true, "Create session");
    final AWSApiCallTimeoutException thrown = intercept(AWSApiCallTimeoutException.class,
        () -> fs.getFileStatus(path("testShortTimeout")));
    call.finished();
    LOG.info("Exception raised after {}", call, thrown);
    // if the timeout took too long, fail with details and include the original
    // exception
    if (call.value() > TIMEOUT_EXCEPTION_THRESHOLD) {
      throw new AssertionError("Duration of create session " + call.getDurationString()
          + " exceeds threshold " + TIMEOUT_EXCEPTION_THRESHOLD + " ms: " + thrown, thrown);
    }
    Assertions.assertThat(SLEEP_INTERRUPTED.get())
        .describedAs("Sleep interrupted during signing")
        .isTrue();

    // now scan the inner exception stack for "createSession"
    Arrays.stream(thrown.getCause().getStackTrace())
        .filter(e -> e.getMethodName().equals("createSession"))
        .findFirst()
        .orElseThrow(() ->
            new AssertionError("No createSession() in inner stack trace of", thrown));
  }

  /**
   * Sleep for as long as {@link #SLEEP_DURATION} requires.
   */
  private static void sleep() {
    long sleep = SLEEP_DURATION.get();
    if (sleep > 0) {
      LOG.info("Sleeping for {} ms", sleep, new Exception());
      try (DurationInfo d = new DurationInfo(LOG, true, "Sleep for %d ms", sleep)) {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        LOG.info("Interrupted", e);
        SLEEP_INTERRUPTED.set(true);
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * A signer which calls {@link #sleep()} before signing.
   * As this signing takes place within the CreateSession Pipeline,
   */
  public static class SlowSigner extends CustomHttpSigner {

    @Override
    public SignedRequest sign(
        final SignRequest<? extends AwsCredentialsIdentity> request) {

      final SdkHttpRequest httpRequest = request.request();
      LOG.info("Signing request {}", httpRequest);
      sleep();
      return super.sign(request);
    }

    @Override
    public CompletableFuture<AsyncSignedRequest> signAsync(
        final AsyncSignRequest<? extends AwsCredentialsIdentity> request) {
      sleep();
      return super.signAsync(request);
    }

  }
}
