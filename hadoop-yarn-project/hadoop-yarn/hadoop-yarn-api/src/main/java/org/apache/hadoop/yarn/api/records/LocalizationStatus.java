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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * Represents the localization status of a resource.
 * The status of the localization includes:
 * <ul>
 *   <li>resource key</li>
 *   <li>{@link LocalizationState} of the resource</li>
 * </ul>
 */
@Public
@Unstable
public abstract class LocalizationStatus {

  public static LocalizationStatus newInstance(String resourceKey,
      LocalizationState localizationState) {
    return newInstance(resourceKey, localizationState, null);
  }

  public static LocalizationStatus newInstance(String resourceKey,
      LocalizationState localizationState,
      String diagnostics) {
    LocalizationStatus status = Records.newRecord(LocalizationStatus.class);
    status.setResourceKey(resourceKey);
    status.setLocalizationState(localizationState);
    status.setDiagnostics(diagnostics);
    return status;
  }

  /**
   * Get the resource key.
   *
   * @return resource key.
   */
  public abstract String getResourceKey();

  /**
   * Sets the resource key.
   * @param resourceKey
   */
  @InterfaceAudience.Private
  public abstract void setResourceKey(String resourceKey);

  /**
   * Get the localization sate.
   *
   * @return localization state.
   */
  public abstract LocalizationState getLocalizationState();

  /**
   * Sets the localization state.
   * @param state localization state
   */
  @InterfaceAudience.Private
  public abstract void setLocalizationState(LocalizationState state);

  /**
   * Get the diagnostics.
   *
   * @return diagnostics.
   */
  public abstract String getDiagnostics();

  /**
   * Sets the diagnostics.
   * @param diagnostics diagnostics.
   */
  @InterfaceAudience.Private
  public abstract void setDiagnostics(String diagnostics);

}
