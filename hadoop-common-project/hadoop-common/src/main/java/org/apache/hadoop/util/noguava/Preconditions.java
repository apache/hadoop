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
package org.apache.hadoop.util.noguava;

import java.util.Collection;
import java.util.Objects;
import javax.annotation.Nullable;

public final class Preconditions {
  private static final String VALIDATE_STATE_EX_MESSAGE =
      "The validated state is false";
  private static final String VALIDATE_IS_NOT_NULL_EX_MESSAGE =
      "The validated object is null";
  private static final String VALIDATE_RANGE_INDEX_ARRAY_EX_MESSAGE =
      "The validated array size: %d, index is out of range: %d";
  private static final String VALIDATE_INDEX_ARRAY_EX_MESSAGE =
      "The validated array index is invalid: %d";
  private static final String VALIDATE_SIZE_ARRAY_EX_MESSAGE =
      "The validated array size is invalid: %d";
  private static final String VALIDATE_IS_TRUE_EX_MESSAGE =
      "The validated expression is false";

  private Preconditions() {
    super();
  }

  public static void checkIsTrue(final boolean expression, final String message,
      final long value) {
    if (!expression) {
      throw new IllegalArgumentException(
          String.format(message, Long.valueOf(value)));
    }
  }

  public static void checkIsTrue(final boolean expression, final String message,
      final double value) {
    if (!expression) {
      throw new IllegalArgumentException(
          String.format(message, Double.valueOf(value)));
    }
  }

  public static void checkIsTrue(final boolean expression, final String message,
      final Object... values) {
    if (!expression) {
      throw new IllegalArgumentException(String.format(message, values));
    }
  }

  public static void checkIsTrue(final boolean expression) {
    if (!expression) {
      throw new IllegalArgumentException(VALIDATE_IS_TRUE_EX_MESSAGE);
    }
  }

  public static <T> T checkNotNull(final T object) {
    return checkNotNull(object, VALIDATE_IS_NOT_NULL_EX_MESSAGE);
  }

  public static <T> T checkNotNull(final T reference,
      @Nullable final Object errorMessage) {
    return Objects.requireNonNull(reference, String.valueOf(errorMessage));
  }

  public static <T> T checkNotNull(final T object,
      @Nullable final String message, final Object... values) {
    return Objects.requireNonNull(object, () -> String.format(message, values));
  }

  public static void checkState(final boolean expression) {
    if (!expression) {
      throw new IllegalStateException(VALIDATE_STATE_EX_MESSAGE);
    }
  }

  public static void checkState(final boolean expression, final String message,
      final Object... values) {
    if (!expression) {
      throw new IllegalStateException(String.format(message, values));
    }
  }

  public static <T> T[] checkIndex(final T[] array, final int index) {
    return checkIndex(array, index, VALIDATE_INDEX_ARRAY_EX_MESSAGE,
        Integer.valueOf(index));
  }

  public static <T> T[] checkIndex(final T[] array, final int index,
      final String message, final Object... values) {
    checkNotNull(array);
    if (index < 0 || index >= array.length) {
      throw new IndexOutOfBoundsException(String.format(message, values));
    }
    return array;
  }

  public static <T extends Collection<?>> T checkIndex(final T collection,
      final int index, final String message, final Object... values) {
    checkNotNull(collection);
    if (index < 0 || index >= collection.size()) {
      throw new IndexOutOfBoundsException(String.format(message, values));
    }
    return collection;
  }

  public static int checkIndex(final int index, final int size) {
    if (index < 0 || index > size) {
      throw new IndexOutOfBoundsException(checkIndexAndSize(index, size));
    }
    return index;
  }

  private static String checkIndexAndSize(final int index, final int size) {
    if (index < 0) {
      return String.format(VALIDATE_INDEX_ARRAY_EX_MESSAGE, index);
    } else if (size < 0) {
      throw new IllegalArgumentException(
          String.format(VALIDATE_SIZE_ARRAY_EX_MESSAGE, size));
    } else { // index > size
      return String.format(VALIDATE_RANGE_INDEX_ARRAY_EX_MESSAGE, size, index);
    }
  }
}
