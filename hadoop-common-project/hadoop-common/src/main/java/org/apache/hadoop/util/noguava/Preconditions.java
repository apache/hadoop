package org.apache.hadoop.util.noguava;

import java.util.Collection;
import java.util.Objects;
import javax.annotation.Nullable;

public class Preconditions {
  private static final String DEFAULT_VALID_STATE_EX_MESSAGE = "The validated state is false";
  private static final String DEFAULT_IS_NULL_EX_MESSAGE = "The validated object is null";
  private static final String DEFAULT_RANGE_INDEX_ARRAY_EX_MESSAGE = "The validated array size: %d, index is out of range: %d";
  private static final String DEFAULT_VALID_INDEX_ARRAY_EX_MESSAGE = "The validated array index is invalid: %d";
  private static final String DEFAULT_VALID_SIZE_ARRAY_EX_MESSAGE = "The validated array size is invalid: %d";
  private static final String DEFAULT_IS_TRUE_EX_MESSAGE = "The validated expression is false";
  Preconditions() {
    super();
  }
  // isTrue
  //---------------------------------------------------------------------------------

  /**
   * <p>Validate that the argument condition is {@code true}; otherwise
   * throwing an exception with the specified message. This method is useful when
   * validating according to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>Validate.isTrue(i &gt; 0.0, "The value must be greater than zero: &#37;d", i);</pre>
   *
   * <p>For performance reasons, the long value is passed as a separate parameter and
   * appended to the exception message only in the case of an error.</p>
   *
   * @param expression  the boolean expression to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param value  the value to append to the message when invalid
   * @throws IllegalArgumentException if expression is {@code false}
   * @see #isTrue(boolean)
   * @see #isTrue(boolean, String, double)
   * @see #isTrue(boolean, String, Object...)
   */
  public static void checkExpression(final boolean expression, final String message, final long value) {
    if (!expression) {
      throw new IllegalArgumentException(String.format(message, Long.valueOf(value)));
    }
  }

  /**
   * <p>Validate that the argument condition is {@code true}; otherwise
   * throwing an exception with the specified message. This method is useful when
   * validating according to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>Validate.isTrue(d &gt; 0.0, "The value must be greater than zero: &#37;s", d);</pre>
   *
   * <p>For performance reasons, the double value is passed as a separate parameter and
   * appended to the exception message only in the case of an error.</p>
   *
   * @param expression  the boolean expression to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param value  the value to append to the message when invalid
   * @throws IllegalArgumentException if expression is {@code false}
   * @see #isTrue(boolean)
   * @see #isTrue(boolean, String, long)
   * @see #isTrue(boolean, String, Object...)
   */
  public static void checkExpression(final boolean expression, final String message, final double value) {
    if (!expression) {
      throw new IllegalArgumentException(String.format(message, Double.valueOf(value)));
    }
  }

  /**
   * <p>Validate that the argument condition is {@code true}; otherwise
   * throwing an exception with the specified message. This method is useful when
   * validating according to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>
   * Validate.isTrue(i &gt;= min &amp;&amp; i &lt;= max, "The value must be between &#37;d and &#37;d", min, max);
   * Validate.isTrue(myObject.isOk(), "The object is not okay");</pre>
   *
   * @param expression  the boolean expression to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalArgumentException if expression is {@code false}
   * @see #isTrue(boolean)
   * @see #isTrue(boolean, String, long)
   * @see #isTrue(boolean, String, double)
   */
  public static void checkExpression(final boolean expression, final String message, final Object... values) {
    if (!expression) {
      throw new IllegalArgumentException(String.format(message, values));
    }
  }

  /**
   * <p>Validate that the argument condition is {@code true}; otherwise
   * throwing an exception. This method is useful when validating according
   * to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>
   * Validate.isTrue(i &gt; 0);
   * Validate.isTrue(myObject.isOk());</pre>
   *
   * <p>The message of the exception is &quot;The validated expression is
   * false&quot;.</p>
   *
   * @param expression  the boolean expression to check
   * @throws IllegalArgumentException if expression is {@code false}
   * @see #isTrue(boolean, String, long)
   * @see #isTrue(boolean, String, double)
   * @see #isTrue(boolean, String, Object...)
   */
  public static void checkExpression(final boolean expression) {
    if (!expression) {
      throw new IllegalArgumentException(DEFAULT_IS_TRUE_EX_MESSAGE);
    }
  }

  public static <T> T checkNotNull(final T object) {
    return checkNotNull(object, DEFAULT_IS_NULL_EX_MESSAGE);
  }

  public static <T> T checkNotNull(T reference, @Nullable Object errorMessage) {
    return Objects.requireNonNull(reference, String.valueOf(errorMessage));
  }

  public static <T> T checkNotNull(
      T object, @Nullable String message,
      Object... values) {
    return Objects.requireNonNull(object, () -> String.format(message, values));
  }

  public static void checkState(final boolean expression) {
    if (!expression) {
      throw new IllegalStateException(DEFAULT_VALID_STATE_EX_MESSAGE);
    }
  }

  public static void checkState(final boolean expression, final String message, final Object... values) {
    if (!expression) {
      throw new IllegalStateException(String.format(message, values));
    }
  }

  public static <T> T[] validIndex(final T[] array, final int index) {
    return validIndex(array, index, DEFAULT_VALID_INDEX_ARRAY_EX_MESSAGE, Integer.valueOf(index));
  }

  public static <T> T[] validIndex(final T[] array, final int index,
      final String message, final Object... values) {
    checkNotNull(array);
    if (index < 0 || index >= array.length) {
      throw new IndexOutOfBoundsException(String.format(message, values));
    }
    return array;
  }

  public static <T extends Collection<?>> T validIndex(final T collection, final int index, final String message, final Object... values) {
    checkNotNull(collection);
    if (index < 0 || index >= collection.size()) {
      throw new IndexOutOfBoundsException(String.format(message, values));
    }
    return collection;
  }

  public static int checkPositionIndex(final int index, final int size) {
    return checkPositionIndex(index, size, DEFAULT_VALID_INDEX_ARRAY_EX_MESSAGE);
  }

  public static int checkPositionIndex(final int index, final int size,
      @Nullable String desc) {
    // Carefully optimized for execution by hotspot (explanatory comment above)
    if (index < 0 || index > size) {
      throw new IndexOutOfBoundsException(evaluateIndexAndSize(index, size));
    }
    return index;
  }

  private static String evaluateIndexAndSize(int index, int size) {
    if (index < 0) {
      return String.format(DEFAULT_VALID_INDEX_ARRAY_EX_MESSAGE, index);
    } else if (size < 0) {
      throw new IllegalArgumentException(String.format(DEFAULT_VALID_SIZE_ARRAY_EX_MESSAGE, size));
    } else { // index > size
      return String.format(DEFAULT_RANGE_INDEX_ARRAY_EX_MESSAGE, size, index);
    }
  }

  public static void checkPositionIndexes(int start, int end, int size) {
    // Carefully optimized for execution by hotspot (explanatory comment above)
    if (start < 0 || end < start || end > size) {
      throw new IndexOutOfBoundsException(badPositionIndexes(start, end, size));
    }
  }

  private static String badPositionIndexes(int start, int end, int size) {
    if (start < 0 || start > size) {
      return evaluateIndexAndSize(start, size);
    }
    if (end < 0 || end > size) {
      return evaluateIndexAndSize(end, size);
    }
    // end < start
    return Strings.lenientFormat(
        "end index (%s) must not be less than start index (%s)", end, start);
  }

  public static int checkElementIndex(int index, int size) {
    return checkElementIndex(index, size, "index");
  }

  public static int checkElementIndex(int index, int size,
      @Nullable String desc) {
    // Carefully optimized for execution by hotspot (explanatory comment above)
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException(badElementIndex(index, size, desc));
    }
    return index;
  }

  private static String badElementIndex(int index, int size,
      @Nullable String desc) {
    if (index < 0) {
      return Strings.lenientFormat("%s (%s) must not be negative", desc, index);
    } else if (size < 0) {
      throw new IllegalArgumentException("negative size: " + size);
    } else { // index >= size
      return Strings.lenientFormat("%s (%s) must be less than size (%s)", desc,
          index, size);
    }
  }
}
