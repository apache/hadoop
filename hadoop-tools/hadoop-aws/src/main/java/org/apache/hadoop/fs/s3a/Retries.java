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
 * Declaration of retry policy for documentation only.
 * This is purely for visibility in source and is currently package-scoped.
 * Compare with {@link org.apache.hadoop.io.retry.AtMostOnce}
 * and {@link org.apache.hadoop.io.retry.Idempotent}; these are real
 * markers used by Hadoop RPC.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Retries {
  /**
   * No retry, exceptions are translated.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface OnceTranslated {
    String value() default "";
  }

  /**
   * No retry, exceptions are not translated.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface OnceRaw {
    String value() default "";
  }

  /**
   * No retry, expect a bit of both.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface OnceMixed {
    String value() default "";
  }

  /**
   * Retried, exceptions are translated.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface RetryTranslated {
    String value() default "";
  }

  /**
   * Retried, no translation.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface RetryRaw {
    String value() default "";
  }

  /**
   * Retried, mixed translation.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface RetryMixed {
    String value() default "";
  }


  /**
   * Retried, Exceptions are swallowed.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface RetryExceptionsSwallowed {

    String value() default "";
  }

  /**
   * One attempt, Exceptions are swallowed.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface OnceExceptionsSwallowed {

    String value() default "";
  }

}
