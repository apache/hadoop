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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

import java.util.Set;

/**
 * {@code ContainerRetryContext} indicates how container retry after it fails
 * to run.
 * <p>
 * It provides details such as:
 * <ul>
 *   <li>
 *     {@link ContainerRetryPolicy} :
 *     - NEVER_RETRY(DEFAULT value): no matter what error code is when container
 *       fails to run, just do not retry.
 *     - RETRY_ON_ALL_ERRORS: no matter what error code is, when container fails
 *       to run, just retry.
 *     - RETRY_ON_SPECIFIC_ERROR_CODES: when container fails to run, do retry if
 *       the error code is one of <em>errorCodes</em>, otherwise do not retry.
 *
 *     Note: if error code is 137(SIGKILL) or 143(SIGTERM), it will not retry
 *     because it is usually killed on purpose.
 *   </li>
 *   <li>
 *     <em>maxRetries</em> specifies how many times to retry if need to retry.
 *     If the value is -1, it means retry forever.
 *   </li>
 *   <li><em>retryInterval</em> specifies delaying some time before relaunch
 *   container, the unit is millisecond.</li>
 *   <li>
 *     <em>failuresValidityInterval</em>: default value is -1.
 *     When failuresValidityInterval in milliseconds is set to {@literal >} 0,
 *     the failure number will not take failures which happen out of the
 *     failuresValidityInterval into failure count. If failure count
 *     reaches to <em>maxRetries</em>, the container will be failed.
 *   </li>
 * </ul>
 */
@Public
@Unstable
public abstract class ContainerRetryContext {
  public static final int RETRY_FOREVER = -1;
  public static final int RETRY_INVALID = -1000;
  public static final ContainerRetryContext NEVER_RETRY_CONTEXT =
      newInstance(ContainerRetryPolicy.NEVER_RETRY, null, 0, 0);

  @Private
  @Unstable
  public static ContainerRetryContext newInstance(
      ContainerRetryPolicy retryPolicy, Set<Integer> errorCodes,
      int maxRetries, int retryInterval, long failuresValidityInterval) {
    ContainerRetryContext containerRetryContext =
        Records.newRecord(ContainerRetryContext.class);
    containerRetryContext.setRetryPolicy(retryPolicy);
    containerRetryContext.setErrorCodes(errorCodes);
    containerRetryContext.setMaxRetries(maxRetries);
    containerRetryContext.setRetryInterval(retryInterval);
    containerRetryContext.setFailuresValidityInterval(failuresValidityInterval);
    return containerRetryContext;
  }

  @Private
  @Unstable
  public static ContainerRetryContext newInstance(
      ContainerRetryPolicy retryPolicy, Set<Integer> errorCodes,
      int maxRetries, int retryInterval) {
    return newInstance(retryPolicy, errorCodes, maxRetries, retryInterval, -1);
  }

  public abstract ContainerRetryPolicy getRetryPolicy();
  public abstract void setRetryPolicy(ContainerRetryPolicy retryPolicy);
  public abstract Set<Integer> getErrorCodes();
  public abstract void setErrorCodes(Set<Integer> errorCodes);
  public abstract int getMaxRetries();
  public abstract void setMaxRetries(int maxRetries);
  public abstract int getRetryInterval();
  public abstract void setRetryInterval(int retryInterval);
  public abstract long getFailuresValidityInterval();
  public abstract void setFailuresValidityInterval(
      long failuresValidityInterval);
}
