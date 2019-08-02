package org.apache.hadoop.ozone.util;

/**
 * Defines a functional interface having two inputs and returns boolean as
 * output.
 */
@FunctionalInterface
public interface BiFunction<LEFT, RIGHT> {
  boolean apply(LEFT left, RIGHT right);
}

