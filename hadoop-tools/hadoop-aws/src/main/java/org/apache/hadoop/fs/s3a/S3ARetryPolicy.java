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

package org.apache.hadoop.fs.s3a;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.file.AccessDeniedException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.ConnectTimeoutException;

import static org.apache.hadoop.io.retry.RetryPolicies.*;

import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * The S3A request retry policy.
 *
 * This uses the retry options in the configuration file to determine retry
 * count and delays for "normal" retries and separately, for throttling;
 * the latter is best handled for longer with an exponential back-off.
 *
 * <ol>
 * <li> Those exceptions considered unrecoverable (networking) are
 *    failed fast.</li>
 * <li>All non-IOEs are failed immediately. Assumed: bugs in code,
 *    unrecoverable errors, etc</li>
 * </ol>
 *
 * For non-idempotent operations, only failures due to throttling or
 * from failures which are known to only arise prior to talking to S3
 * are retried.
 *
 * The retry policy is all built around that of the normal IO exceptions,
 * particularly those extracted from
 * {@link S3AUtils#translateException(String, Path, AmazonClientException)}.
 * Because the {@link #shouldRetry(Exception, int, int, boolean)} method
 * does this translation if an {@code AmazonClientException} is processed,
 * the policy defined for the IOEs also applies to the original exceptions.
 *
 * Put differently: this retry policy aims to work for handlers of the
 * untranslated exceptions, as well as the translated ones.
 * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">S3 Error responses</a>
 * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/ErrorBestPractices.html">Amazon S3 Error Best Practices</a>
 * @see <a href="http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html">Dynamo DB Commmon errors</a>
 */
public class S3ARetryPolicy implements RetryPolicy {

  private final RetryPolicy retryPolicy;

  /**
   * Instantiate.
   * @param conf configuration to read.
   */
  public S3ARetryPolicy(Configuration conf) {
    Preconditions.checkArgument(conf != null, "Null configuration");

    // base policy from configuration
    RetryPolicy fixedRetries = retryUpToMaximumCountWithFixedSleep(
        conf.getInt(RETRY_LIMIT, RETRY_LIMIT_DEFAULT),
        conf.getTimeDuration(RETRY_INTERVAL,
            RETRY_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS),
        TimeUnit.MILLISECONDS);

    // which is wrapped by a rejection of all non-idempotent calls except
    // for specific failures.
    RetryPolicy retryIdempotentCalls = new FailNonIOEs(
        new IdempotencyRetryFilter(fixedRetries));

    // and a separate policy for throttle requests, which are considered
    // repeatable, even for non-idempotent calls, as the service
    // rejected the call entirely
    RetryPolicy throttlePolicy = exponentialBackoffRetry(
        conf.getInt(RETRY_THROTTLE_LIMIT, RETRY_THROTTLE_LIMIT_DEFAULT),
        conf.getTimeDuration(RETRY_THROTTLE_INTERVAL,
            RETRY_THROTTLE_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS),
        TimeUnit.MILLISECONDS);

    // no retry on network and tangible API issues
    RetryPolicy fail = RetryPolicies.TRY_ONCE_THEN_FAIL;

    // client connectivity: fixed retries without care for idempotency
    RetryPolicy connectivityFailure = fixedRetries;

    // the policy map maps the exact classname; subclasses do not
    // inherit policies.
    Map<Class<? extends Exception>, RetryPolicy> policyMap = new HashMap<>();

    // failfast exceptions which we consider unrecoverable
    policyMap.put(UnknownHostException.class, fail);
    policyMap.put(NoRouteToHostException.class, fail);
    policyMap.put(InterruptedException.class, fail);
    // note this does not pick up subclasses (like socket timeout)
    policyMap.put(InterruptedIOException.class, fail);
    policyMap.put(AWSRedirectException.class, fail);
    // interesting question: should this be retried ever?
    policyMap.put(AccessDeniedException.class, fail);
    policyMap.put(FileNotFoundException.class, fail);
    policyMap.put(InvalidRequestException.class, fail);

    // should really be handled by resubmitting to new location;
    // that's beyond the scope of this retry policy
    policyMap.put(AWSRedirectException.class, fail);

    // throttled requests are can be retried, always
    policyMap.put(AWSServiceThrottledException.class, throttlePolicy);

    // connectivity problems are retried without worrying about idempotency
    policyMap.put(ConnectTimeoutException.class, connectivityFailure);

    // this can be a sign of an HTTP connection breaking early.
    // which can be reacted to by another attempt if the request was idempotent.
    // But: could also be a sign of trying to read past the EOF on a GET,
    // which isn't going to be recovered from
    policyMap.put(EOFException.class, retryIdempotentCalls);

    // policy on a 400/bad request still ambiguous. Given it
    // comes and goes on test runs: try again
    policyMap.put(AWSBadRequestException.class, connectivityFailure);

    // Status 500 error code is also treated as a connectivity problem
    policyMap.put(AWSStatus500Exception.class, connectivityFailure);

    // server didn't respond.
    policyMap.put(AWSNoResponseException.class, retryIdempotentCalls);

    // other operations
    policyMap.put(AWSClientIOException.class, retryIdempotentCalls);
    policyMap.put(AWSServiceIOException.class, retryIdempotentCalls);
    policyMap.put(AWSS3IOException.class, retryIdempotentCalls);
    policyMap.put(SocketTimeoutException.class, retryIdempotentCalls);

    // Dynamo DB exceptions
    // asking for more than you should get. It's a retry but should be logged
    // trigger sleep
    policyMap.put(ProvisionedThroughputExceededException.class, throttlePolicy);

    retryPolicy = retryByException(retryIdempotentCalls, policyMap);
  }

  @Override
  public RetryAction shouldRetry(Exception exception,
      int retries,
      int failovers,
      boolean idempotent) throws Exception {
    Exception ex = exception;
    if (exception instanceof AmazonClientException) {
      // uprate the amazon client exception for the purpose of exception
      // processing.
      ex = S3AUtils.translateException("", "",
          (AmazonClientException) exception);
    }
    return retryPolicy.shouldRetry(ex, retries, failovers, idempotent);
  }

  /**
   * Policy which fails fast any non-idempotent call; hands off
   * all idempotent calls to the next retry policy.
   */
  private static final class IdempotencyRetryFilter implements RetryPolicy {

    private final RetryPolicy next;

    IdempotencyRetryFilter(RetryPolicy next) {
      this.next = next;
    }

    @Override
    public RetryAction shouldRetry(Exception e,
        int retries,
        int failovers,
        boolean idempotent) throws Exception {
      return
          idempotent ?
              next.shouldRetry(e, retries, failovers, true)
              : RetryAction.FAIL;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "IdempotencyRetryFilter{");
      sb.append("next=").append(next);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * All non-IOE exceptions are failed.
   */
  private static final class FailNonIOEs implements RetryPolicy {

    private final RetryPolicy next;

    private FailNonIOEs(RetryPolicy next) {
      this.next = next;
    }

    @Override
    public RetryAction shouldRetry(Exception e,
        int retries,
        int failovers,
        boolean isIdempotentOrAtMostOnce) throws Exception {
      return
          e instanceof IOException ?
              next.shouldRetry(e, retries, failovers, true)
              : RetryAction.FAIL;
    }
  }

}
