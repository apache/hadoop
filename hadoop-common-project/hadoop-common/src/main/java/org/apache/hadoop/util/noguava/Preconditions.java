package org.apache.hadoop.util.noguava;

import java.util.Collection;
import java.util.Objects;
import javax.annotation.Nullable;

public final class Preconditions {
  private static final String DEFAULT_VALID_STATE_EX_MESSAGE =
      "The validated state is false";
  private static final String DEFAULT_IS_NULL_EX_MESSAGE =
      "The validated object is null";
  private static final String DEFAULT_RANGE_INDEX_ARRAY_EX_MESSAGE =
      "The validated array size: %d, index is out of range: %d";
  private static final String DEFAULT_VALID_INDEX_ARRAY_EX_MESSAGE =
      "The validated array index is invalid: %d";
  private static final String DEFAULT_VALID_SIZE_ARRAY_EX_MESSAGE =
      "The validated array size is invalid: %d";
  private static final String DEFAULT_IS_TRUE_EX_MESSAGE =
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
      throw new IllegalArgumentException(DEFAULT_IS_TRUE_EX_MESSAGE);
    }
  }

  public static <T> T checkNotNull(final T object) {
    return checkNotNull(object, DEFAULT_IS_NULL_EX_MESSAGE);
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
      throw new IllegalStateException(DEFAULT_VALID_STATE_EX_MESSAGE);
    }
  }

  public static void checkState(final boolean expression, final String message,
      final Object... values) {
    if (!expression) {
      throw new IllegalStateException(String.format(message, values));
    }
  }

  public static <T> T[] validIndex(final T[] array, final int index) {
    return validIndex(array, index, DEFAULT_VALID_INDEX_ARRAY_EX_MESSAGE,
        Integer.valueOf(index));
  }

  public static <T> T[] validIndex(final T[] array, final int index,
      final String message, final Object... values) {
    checkNotNull(array);
    if (index < 0 || index >= array.length) {
      throw new IndexOutOfBoundsException(String.format(message, values));
    }
    return array;
  }

  public static <T extends Collection<?>> T validIndex(final T collection,
      final int index, final String message, final Object... values) {
    checkNotNull(collection);
    if (index < 0 || index >= collection.size()) {
      throw new IndexOutOfBoundsException(String.format(message, values));
    }
    return collection;
  }

  public static int checkPositionIndex(final int index, final int size) {
    return checkPositionIndex(index, size,
        DEFAULT_VALID_INDEX_ARRAY_EX_MESSAGE);
  }

  public static int checkPositionIndex(final int index, final int size,
      @Nullable final String desc) {
    // Carefully optimized for execution by hotspot (explanatory comment above)
    if (index < 0 || index > size) {
      throw new IndexOutOfBoundsException(evaluateIndexAndSize(index, size));
    }
    return index;
  }

  private static String evaluateIndexAndSize(final int index, final int size) {
    if (index < 0) {
      return String.format(DEFAULT_VALID_INDEX_ARRAY_EX_MESSAGE, index);
    } else if (size < 0) {
      throw new IllegalArgumentException(
          String.format(DEFAULT_VALID_SIZE_ARRAY_EX_MESSAGE, size));
    } else { // index > size
      return String.format(DEFAULT_RANGE_INDEX_ARRAY_EX_MESSAGE, size, index);
    }
  }

  public static void checkPositionIndexes(final int start, final int end,
      final int size) {
    // Carefully optimized for execution by hotspot (explanatory comment above)
    if (start < 0 || end < start || end > size) {
      throw new IndexOutOfBoundsException(badPositionIndexes(start, end, size));
    }
  }

  private static String badPositionIndexes(final int start, final int end,
      final int size) {
    if (start < 0 || start > size) {
      return evaluateIndexAndSize(start, size);
    }
    if (end < 0 || end > size) {
      return evaluateIndexAndSize(end, size);
    }
    // end < start
    return String.format(
        "end index (%s) must not be less than start index (%s)", end, start);
  }

  public static int checkElementIndex(final int index, final int size) {
    return checkElementIndex(index, size, "index");
  }

  public static int checkElementIndex(final int index, final int size,
      @Nullable final String desc) {
    // Carefully optimized for execution by hotspot (explanatory comment above)
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException(badElementIndex(index, size, desc));
    }
    return index;
  }

  private static String badElementIndex(final int index, final int size,
      @Nullable final String desc) {
    if (index < 0) {
      return String.format("%s (%s) must not be negative", desc, index);
    } else if (size < 0) {
      throw new IllegalArgumentException("negative size: " + size);
    } else { // index >= size
      return String.format("%s (%s) must be less than size (%s)", desc,
          index, size);
    }
  }
}
