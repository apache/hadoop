package org.apache.hadoop.ozone.util;

/**
 * Defines a functional interface having two inputs and returns boolean as
 * output.
 */
@FunctionalInterface
public interface BooleanBiFunction<LEFT, RIGHT> {
  boolean apply(LEFT left, RIGHT right);
}

