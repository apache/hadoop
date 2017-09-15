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

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Declaration of retry policy.
 * This is purely for visibility in source and is currently package-scoped.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Retries {
  /**
   * No retry, exceptions are translated.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface Once_translated {
    String value() default "";
  };

  /**
   * No retry, exceptions are not translated.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface Once_raw {
    String value() default "";
  };

  /**
   * No retry, expect a bit of both.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface Once_mixed {
    String value() default "";
  };

  /**
   * Retried, exceptions are translated.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface Retry_translated {
    String value() default "";
  };

  /**
   * Retried, no translation.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface Retry_raw {
    String value() default "";
  };

  /**
   * Retried, mixed translation.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface Retry_mixed {
    String value() default "";
  };

}
