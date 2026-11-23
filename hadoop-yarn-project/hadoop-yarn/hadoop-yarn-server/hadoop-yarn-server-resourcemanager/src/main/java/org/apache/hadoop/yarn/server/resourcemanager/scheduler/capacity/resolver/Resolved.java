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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A resolved queue configuration value and where it came from.
 * <p>
 * A value that could not be parsed does not fail the resolution: the
 * exception is kept and rethrown by {@link #getValue()}, so a caller that
 * needs the value fails with the same exception type and message as the
 * corresponding {@code CapacitySchedulerConfiguration} getter, while a
 * caller that only inspects the configuration can use {@link #isFailed()}.
 * @param <T> type of the value
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class Resolved<T> {
  private final T value;
  private final ValueSource source;
  private final String sourceDetail;
  private final RuntimeException failure;

  private Resolved(T value, ValueSource source, String sourceDetail,
      RuntimeException failure) {
    this.value = value;
    this.source = source;
    this.sourceDetail = sourceDetail;
    this.failure = failure;
  }

  static <T> Resolved<T> of(T value, ValueSource source, String sourceDetail) {
    return new Resolved<>(value, source, sourceDetail, null);
  }

  static <T> Resolved<T> failed(RuntimeException failure, ValueSource source,
      String sourceDetail) {
    return new Resolved<>(null, source, sourceDetail, failure);
  }

  /**
   * Returns the value.
   * @return the resolved value, may be {@code null} where the getter returns
   *         {@code null}
   * @throws RuntimeException the exception the value failed to parse with
   */
  public T getValue() {
    if (failure != null) {
      throw failure;
    }
    return value;
  }

  public ValueSource getSource() {
    return source;
  }

  /**
   * Returns the full key, parent queue path or formula the value came from.
   * @return the source detail, {@code null} for built-in defaults
   */
  public String getSourceDetail() {
    return sourceDetail;
  }

  public boolean isFailed() {
    return failure != null;
  }

  /**
   * Returns the message of the parse failure.
   * @return the failure message, or {@code null} when the value parsed
   */
  public String getFailureMessage() {
    return failure == null ? null : failure.getMessage();
  }

  @Override
  public String toString() {
    String shown = failure != null ? "failed: " + failure : String.valueOf(value);
    return shown + " (" + source
        + (sourceDetail == null ? "" : " " + sourceDetail) + ")";
  }
}
