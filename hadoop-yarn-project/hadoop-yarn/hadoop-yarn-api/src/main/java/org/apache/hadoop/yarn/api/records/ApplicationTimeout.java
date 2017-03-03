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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code ApplicationTimeout} is a report for configured application timeouts.
 * It includes details such as:
 * <ul>
 * <li>{@link ApplicationTimeoutType} of the timeout type.</li>
 * <li>Expiry time in ISO8601 standard with format
 * <b>yyyy-MM-dd'T'HH:mm:ss.SSSZ</b> or "UNLIMITED".</li>
 * <li>Remaining time in seconds.</li>
 * </ul>
 * The possible values for {ExpiryTime, RemainingTimeInSeconds} are
 * <ul>
 * <li>{UNLIMITED,-1} : Timeout is not configured for given timeout type
 * (LIFETIME).</li>
 * <li>{ISO8601 date string, 0} : Timeout is configured and application has
 * completed.</li>
 * <li>{ISO8601 date string, greater than zero} : Timeout is configured and
 * application is RUNNING. Application will be timed out after configured
 * value.</li>
 * </ul>
 */
@Public
@Unstable
public abstract class ApplicationTimeout {

  @Public
  @Unstable
  public static ApplicationTimeout newInstance(ApplicationTimeoutType type,
      String expiryTime, long remainingTime) {
    ApplicationTimeout timeouts = Records.newRecord(ApplicationTimeout.class);
    timeouts.setTimeoutType(type);
    timeouts.setExpiryTime(expiryTime);
    timeouts.setRemainingTime(remainingTime);
    return timeouts;
  }

  /**
   * Get the application timeout type.
   * @return timeoutType of an application timeout.
   */
  @Public
  @Unstable
  public abstract ApplicationTimeoutType getTimeoutType();

  /**
   * Set the application timeout type.
   * @param timeoutType of an application timeout.
   */
  @Public
  @Unstable
  public abstract void setTimeoutType(ApplicationTimeoutType timeoutType);

  /**
   * Get <code>expiryTime</code> for given timeout type.
   * @return expiryTime in ISO8601 standard with format
   *         <b>yyyy-MM-dd'T'HH:mm:ss.SSSZ</b>.
   */
  @Public
  @Unstable
  public abstract String getExpiryTime();

  /**
   * Set <code>expiryTime</code> for given timeout type.
   * @param expiryTime in ISO8601 standard with format
   *          <b>yyyy-MM-dd'T'HH:mm:ss.SSSZ</b>.
   */
  @Public
  @Unstable
  public abstract void setExpiryTime(String expiryTime);

  /**
   * Get <code>Remaining Time</code> of an application for given timeout type.
   * @return Remaining Time in seconds.
   */
  @Public
  @Unstable
  public abstract long getRemainingTime();

  /**
   * Set <code>Remaining Time</code> of an application for given timeout type.
   * @param remainingTime in seconds.
   */
  @Public
  @Unstable
  public abstract void setRemainingTime(long remainingTime);
}
