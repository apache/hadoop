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
package org.apache.hadoop.io.retry;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry;
import org.apache.hadoop.ipc.RemoteException;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.ipc.RetriableException;

public class RetryUtils {
  public static final Log LOG = LogFactory.getLog(RetryUtils.class);
  
  /**
   * Return the default retry policy set in conf.
   * 
   * If the value retryPolicyEnabledKey is set to false in conf,
   * use TRY_ONCE_THEN_FAIL.
   * 
   * Otherwise, get the MultipleLinearRandomRetry policy specified in the conf
   * and then
   * (1) use multipleLinearRandomRetry for
   *     - remoteExceptionToRetry, or
   *     - IOException other than RemoteException, or
   *     - ServiceException; and
   * (2) use TRY_ONCE_THEN_FAIL for
   *     - non-remoteExceptionToRetry RemoteException, or
   *     - non-IOException.
   *     
   *
   * @param conf
   * @param retryPolicyEnabledKey     conf property key for enabling retry
   * @param defaultRetryPolicyEnabled default retryPolicyEnabledKey conf value 
   * @param retryPolicySpecKey        conf property key for retry policy spec
   * @param defaultRetryPolicySpec    default retryPolicySpecKey conf value
   * @param remoteExceptionToRetry    The particular RemoteException to retry
   * @return the default retry policy.
   */
  public static RetryPolicy getDefaultRetryPolicy(
      Configuration conf,
      String retryPolicyEnabledKey,
      boolean defaultRetryPolicyEnabled,
      String retryPolicySpecKey,
      String defaultRetryPolicySpec,
      final String remoteExceptionToRetry
      ) {
    
    final RetryPolicy multipleLinearRandomRetry = 
        getMultipleLinearRandomRetry(
            conf, 
            retryPolicyEnabledKey, defaultRetryPolicyEnabled, 
            retryPolicySpecKey, defaultRetryPolicySpec
            );
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("multipleLinearRandomRetry = " + multipleLinearRandomRetry);
    }

    if (multipleLinearRandomRetry == null) {
      //no retry
      return RetryPolicies.TRY_ONCE_THEN_FAIL;
    } else {
      return new WrapperRetryPolicy(
          (MultipleLinearRandomRetry) multipleLinearRandomRetry,
          remoteExceptionToRetry);
    }
  }

  private static final class WrapperRetryPolicy implements RetryPolicy {
    private MultipleLinearRandomRetry multipleLinearRandomRetry;
    private String remoteExceptionToRetry;

    private WrapperRetryPolicy(
        final MultipleLinearRandomRetry multipleLinearRandomRetry,
        final String remoteExceptionToRetry) {
      this.multipleLinearRandomRetry = multipleLinearRandomRetry;
      this.remoteExceptionToRetry = remoteExceptionToRetry;
    }

    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isMethodIdempotent) throws Exception {
      if (e instanceof ServiceException) {
        //unwrap ServiceException
        final Throwable cause = e.getCause();
        if (cause != null && cause instanceof Exception) {
          e = (Exception)cause;
        }
      }

      //see (1) and (2) in the javadoc of this method.
      final RetryPolicy p;
      if (e instanceof RetriableException
          || RetryPolicies.getWrappedRetriableException(e) != null) {
        // RetriableException or RetriableException wrapped
        p = multipleLinearRandomRetry;
      } else if (e instanceof RemoteException) {
        final RemoteException re = (RemoteException)e;
        p = re.getClassName().equals(remoteExceptionToRetry)
            ? multipleLinearRandomRetry : RetryPolicies.TRY_ONCE_THEN_FAIL;
      } else if (e instanceof IOException || e instanceof ServiceException) {
        p = multipleLinearRandomRetry;
      } else { //non-IOException
        p = RetryPolicies.TRY_ONCE_THEN_FAIL;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("RETRY " + retries + ") policy="
            + p.getClass().getSimpleName() + ", exception=" + e);
      }
      return p.shouldRetry(e, retries, failovers, isMethodIdempotent);
    }

    /**
     * remoteExceptionToRetry is ignored as part of equals since it does not
     * affect connection failure handling.
     */
    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      } else {
        return (obj instanceof WrapperRetryPolicy)
            && this.multipleLinearRandomRetry
                .equals(((WrapperRetryPolicy) obj).multipleLinearRandomRetry);
      }
    }

    /**
     * Similarly, remoteExceptionToRetry is ignored as part of hashCode since it
     * does not affect connection failure handling.
     */
    @Override
    public int hashCode() {
      return multipleLinearRandomRetry.hashCode();
    }

    @Override
    public String toString() {
      return "RetryPolicy[" + multipleLinearRandomRetry + ", "
          + RetryPolicies.TRY_ONCE_THEN_FAIL.getClass().getSimpleName() + "]";
    }
  }

  /**
   * Return the MultipleLinearRandomRetry policy specified in the conf,
   * or null if the feature is disabled.
   * If the policy is specified in the conf but the policy cannot be parsed,
   * the default policy is returned.
   * 
   * Retry policy spec:
   *   N pairs of sleep-time and number-of-retries "s1,n1,s2,n2,..."
   * 
   * @param conf
   * @param retryPolicyEnabledKey     conf property key for enabling retry
   * @param defaultRetryPolicyEnabled default retryPolicyEnabledKey conf value 
   * @param retryPolicySpecKey        conf property key for retry policy spec
   * @param defaultRetryPolicySpec    default retryPolicySpecKey conf value
   * @return the MultipleLinearRandomRetry policy specified in the conf,
   *         or null if the feature is disabled.
   */
  public static RetryPolicy getMultipleLinearRandomRetry(
      Configuration conf,
      String retryPolicyEnabledKey,
      boolean defaultRetryPolicyEnabled,
      String retryPolicySpecKey,
      String defaultRetryPolicySpec
      ) {
    final boolean enabled = 
        conf.getBoolean(retryPolicyEnabledKey, defaultRetryPolicyEnabled);
    if (!enabled) {
      return null;
    }

    final String policy = conf.get(retryPolicySpecKey, defaultRetryPolicySpec);

    final RetryPolicy r = 
        RetryPolicies.MultipleLinearRandomRetry.parseCommaSeparatedString(
            policy);
    return (r != null) ? 
        r : 
        RetryPolicies.MultipleLinearRandomRetry.parseCommaSeparatedString(
        defaultRetryPolicySpec);
  }
}
