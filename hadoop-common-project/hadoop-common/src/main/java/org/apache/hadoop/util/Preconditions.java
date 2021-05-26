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

package org.apache.hadoop.util;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * <p>This class replaces {@code guava.Preconditions} which provides helpers
 * to validate the following conditions:
 * <ul>
 *   <li>An invalid {@code null} obj causes a {@link NullPointerException}.</li>
 *   <li>An invalid argument causes an {@link IllegalArgumentException}.</li>
 *   <li>An invalid state causes an {@link IllegalStateException}.</li>
 *   <li>An invalid index causes an {@link IndexOutOfBoundsException}.</li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class Preconditions {
  private static final Logger LOG =
      LoggerFactory.getLogger(Preconditions.class);

  private static final String VALIDATE_IS_NOT_NULL_EX_MESSAGE =
      "The argument object is NULL";

  private Preconditions() {
  }

  /**
   * <p>Preconditions that the specified argument is not {@code null},
   * throwing a NPE exception otherwise.
   *
   * <p>The message of the exception is
   * &quot;The validated object is null&quot;.</p>
   *
   * @param <T> the object type
   * @param obj  the object to check
   * @return the validated object
   * @throws NullPointerException if the object is {@code null}
   * @see #checkNotNull(Object, Object)
   */
  public static <T> T checkNotNull(final T obj) {
    return checkNotNull(obj, VALIDATE_IS_NOT_NULL_EX_MESSAGE);
  }

  /**
   * <p>Preconditions that the specified argument is not {@code null},
   * throwing a NPE exception otherwise.
   *
   * <p>The message of the exception is {@code errorMessage}.</p>
   *
   * @param <T> the object type
   * @param obj  the object to check
   * @param errorMessage  the message associated with the NPE
   * @return the validated object
   * @throws NullPointerException if the object is {@code null}
   * @see #checkNotNull(Object, String, Object...)
   */
  public static <T> T checkNotNull(final T obj,
      final Object errorMessage) {
    if (obj == null) {
      throw new NullPointerException(String.valueOf(errorMessage));
    }
    return obj;
  }

  /**
   * <p>Preconditions that the specified argument is not {@code null},
   * throwing a NPE exception otherwise.
   *
   * <p>The message of the exception is {@code String.format(f, m)}.</p>
   *
   * @param <T> the object type
   * @param obj  the object to check
   * @param message  the {@link String#format(String, Object...)}
   *                 exception message if valid. Otherwise,
   *                 the message is {@link #VALIDATE_IS_NOT_NULL_EX_MESSAGE}
   * @param values  the optional values for the formatted exception message
   * @return the validated object
   * @throws NullPointerException if the object is {@code null}
   * @see #checkNotNull(Object, Supplier)
   */
  public static <T> T checkNotNull(final T obj, final String message,
      final Object... values) {
    // Deferring the evaluation of the message is a tradeoff between the cost
    // of constructing lambda Vs constructing a string object.
    // Using lambda would allocate an object on every call:
    //       return checkNotNull(obj, () -> String.format(message, values));
    if (obj == null) {
      String msg;
      try {
        msg = String.format(message, values);
      } catch (Exception e) {
        LOG.debug("Error formatting message", e);
        msg = VALIDATE_IS_NOT_NULL_EX_MESSAGE;
      }
      throw new NullPointerException(msg);
    }
    return obj;
  }

  /**
   * <p>Preconditions that the specified argument is not {@code null},
   * throwing a NPE exception otherwise.
   *
   * <p>The message of the exception is {@code msgSupplier.get()}.</p>
   *
   * @param <T> the object type
   * @param obj  the object to check
   * @param msgSupplier  the {@link Supplier#get()} set the
   *                 exception message if valid. Otherwise,
   *                 the message is {@link #VALIDATE_IS_NOT_NULL_EX_MESSAGE}
   * @return the validated object (never {@code null} for method chaining)
   * @throws NullPointerException if the object is {@code null}
   */
  public static <T> T checkNotNull(final T obj,
      final Supplier<String> msgSupplier) {
    if (obj == null) {
      String msg;
      try {
        // note that we can get NPE evaluating the message itself;
        // but we do not want this to override the actual NPE.
        msg = msgSupplier.get();
      } catch (Exception e) {
        // ideally we want to log the error to capture. This may cause log files
        // to bloat. On the other hand, swallowing the exception may hide a bug
        // in the caller. Debug level is a good compromise between the two
        // concerns.
        LOG.debug("Error formatting message", e);
        msg = VALIDATE_IS_NOT_NULL_EX_MESSAGE;
      }
      throw new NullPointerException(msg);
    }
    return obj;
  }

  @VisibleForTesting
  public static String getDefaultNullMSG() {
    return VALIDATE_IS_NOT_NULL_EX_MESSAGE;
  }
}
