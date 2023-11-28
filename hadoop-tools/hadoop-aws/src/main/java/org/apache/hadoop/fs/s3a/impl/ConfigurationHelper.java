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

package org.apache.hadoop.fs.s3a.impl;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.store.LogExactlyOnce;

/**
 * Helper class for configuration; where methods related to extracting
 * configuration should go instead of {@code S3AUtils}.
 */
@InterfaceAudience.Private
public final class ConfigurationHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationHelper.class);

  /** Log to warn of range issues on a timeout. */
  private static final LogExactlyOnce DURATION_WARN_LOG = new LogExactlyOnce(LOG);

  private ConfigurationHelper() {
  }

  /**
   * Get a duration. This may be negative; callers must check or set the minimum to zero.
   * If the config option is greater than {@code Integer.MAX_VALUE} milliseconds,
   * it is set to that max.
   * If {@code minimumDuration} is set, and the value is less than that, then
   * the minimum is used.
   * Logs the value for diagnostics.
   * @param conf config
   * @param name option name
   * @param defaultDuration default duration
   * @param defaultUnit default unit on the config option if not declared.
   * @param minimumDuration optional minimum duration;
   * @return duration. may be negative.
   */
  public static Duration getDuration(
      final Configuration conf,
      final String name,
      final Duration defaultDuration,
      final TimeUnit defaultUnit,
      @Nullable final Duration minimumDuration) {
    long timeMillis = conf.getTimeDuration(name,
        defaultDuration.toMillis(), defaultUnit, TimeUnit.MILLISECONDS);

    if (timeMillis > Integer.MAX_VALUE) {
      DURATION_WARN_LOG.warn("Option {} is too high({} ms). Setting to {} ms instead",
          name, timeMillis, Integer.MAX_VALUE);
      LOG.debug("Option {} is too high({} ms). Setting to {} ms instead",
          name, timeMillis, Integer.MAX_VALUE);
      timeMillis = Integer.MAX_VALUE;
    }

    Duration duration = enforceMinimumDuration(name, Duration.ofMillis(timeMillis),
        minimumDuration);
    LOG.debug("Duration of {} = {}", name, duration);
    return duration;
  }

  /**
   * Set a duration as a time in seconds, with the suffix "s" added.
   * @param conf configuration to update.
   * @param name option name
   * @param duration duration
   */
  public static void setDurationAsSeconds(
      final Configuration conf,
      final String name,
      final Duration duration) {
    conf.set(name, duration.getSeconds() + "s");
  }

  /**
   * Set a duration as a time in milliseconds, with the suffix "ms" added.
   * @param conf configuration to update.
   * @param name option name
   * @param duration duration
   */
  public static void setDurationAsMillis(
      final Configuration conf,
      final String name,
      final Duration duration) {
    conf.set(name, duration.toMillis()+ "ms");
  }

  /**
   * Enforce a minimum duration of a configuration option, if the supplied
   * value is non-null.
   * @param name option name
   * @param duration duration to check
   * @param minimumDuration minimum duration; may be null
   * @return a duration which, if the minimum duration is set, is at least that value.
   */
  public static Duration enforceMinimumDuration(
      final String name,
      final Duration duration,
      @Nullable final Duration minimumDuration) {

    if (minimumDuration != null && duration.compareTo(minimumDuration) < 0) {
      String message = String.format("Option %s is too low (%,d ms). Setting to %,d ms instead",
          name, duration.toMillis(), minimumDuration.toMillis());
      DURATION_WARN_LOG.warn(message);
      LOG.debug(message);
      return minimumDuration;
    }
    return duration;
  }
}
