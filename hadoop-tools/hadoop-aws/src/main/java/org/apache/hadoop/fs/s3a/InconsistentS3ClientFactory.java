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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * S3 Client factory used for testing with eventual consistency fault injection.
 * This client is for testing <i>only</i>; it is in the production
 * {@code hadoop-aws} module to enable integration tests to use this
 * just by editing the Hadoop configuration used to bring up the client.
 *
 * The factory injects an {@link ExecutionInterceptor} to inject failures.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InconsistentS3ClientFactory extends DefaultS3ClientFactory {

  @Override
  protected ClientOverrideConfiguration createClientOverrideConfiguration(
      S3ClientCreationParameters parameters, Configuration conf) throws IOException {
    LOG.warn("** FAILURE INJECTION ENABLED.  Do not run in production! **");
    LOG.warn("List inconsistency is no longer emulated; only throttling and read errors");
    return super.createClientOverrideConfiguration(parameters, conf)
        .toBuilder()
        .addExecutionInterceptor(new FailureInjectionInterceptor(
            new FailureInjectionPolicy(conf)))
        .build();
  }

  private static class FailureInjectionInterceptor implements ExecutionInterceptor {

    private final FailureInjectionPolicy policy;

    /**
     * Counter of failures since last reset.
     */
    private final AtomicLong failureCounter = new AtomicLong(0);

   FailureInjectionInterceptor(FailureInjectionPolicy policy) {
      this.policy = policy;
    }

    @Override
    public void beforeExecution(Context.BeforeExecution context,
        ExecutionAttributes executionAttributes) {
      maybeFail();
    }

    private void maybeFail() {
      maybeFail("throttled", 503);
    }

    /**
     * Conditionally fail the operation.
     * @param errorMsg description of failure
     * @param statusCode http status code for error
     * @throws SdkException if the client chooses to fail
     * the request.
     */
    private void maybeFail(String errorMsg, int statusCode)
        throws SdkException {
      // code structure here is to line up for more failures later
      AwsServiceException ex = null;
      if (FailureInjectionPolicy.trueWithProbability(policy.getThrottleProbability())) {
        // throttle the request
        ex = AwsServiceException.builder()
            .message(errorMsg + " count = " + (failureCounter.get() + 1))
            .statusCode(statusCode)
            .build();
      }

      int failureLimit = policy.getFailureLimit();
      if (ex != null) {
        long count = failureCounter.incrementAndGet();
        if (failureLimit == 0
            || (failureLimit > 0 && count < failureLimit)) {
          throw ex;
        }
      }
    }
  }
}
